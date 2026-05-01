package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

// TaskContext 对应 Python 端推入的“就绪弹药包”结构
// 在这个安全版 worker 中，它代表本地回测订单的执行上下文。
type TaskContext struct {
	TaskUUID         string            `json:"task_uuid"`
	BusinessZoneCode  string            `json:"business_zone_code"`
	EgressNodeDSN     string            `json:"egress_node_dsn"`
	TargetEndpoint    string            `json:"target_endpoint"`
	HTTPContext       map[string]string `json:"http_context"`
	AuthToken         string            `json:"authorization_token"`
	PayloadTemplate   string            `json:"payload_template"`
}

func main() {
	// 1. 初始化 Redis 连接
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "localhost:6379"
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     redisURL,
		PoolSize: 100,
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("❌ [FATAL] Failed to connect to Redis: %v", err)
	}
	log.Println("✅ [INFO] Connected to Redis Event Bus successfully.")

	// 2. 初始化 SQLite 连接池，开启 WAL，减少锁竞争
	db, err := openSQLiteDB(filepath.Join("..", "..", "shared", "goldspade.sqlite3"))
	if err != nil {
		log.Fatalf("❌ [FATAL] Failed to open SQLite database: %v", err)
	}
	defer db.Close()

	// 3. 配置底层 HTTP Client
	customTransport := http.DefaultTransport.(*http.Transport).Clone()
	customTransport.MaxConnsPerHost = 2000
	customTransport.MaxIdleConns = 2000
	customTransport.MaxIdleConnsPerHost = 500
	customTransport.IdleConnTimeout = 90 * time.Second

	httpClient := &http.Client{
		Timeout:   10 * time.Second,
		Transport: customTransport,
	}

	// 4. 订阅事件总线
	pubsub := rdb.Subscribe(ctx, "channel:task_trigger")
	defer pubsub.Close()

	ch := pubsub.Channel()
	log.Println("📡 [INFO] Awaiting transaction triggers on 'channel:task_trigger'...")

	// 优雅退出守护
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("⚠️ [WARN] Shutdown signal received. Waiting for flying transactions to land...")
		os.Exit(0)
	}()

	// 5. 事件驱动循环
	for msg := range ch {
		log.Printf("🔥 [EVENT] Trigger received. Payload: %s", msg.Payload)
		executeReadyTasks(db, rdb, httpClient)
	}
}

func openSQLiteDB(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dbPath+"?_busy_timeout=5000&_journal_mode=WAL&_synchronous=NORMAL")
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := db.ExecContext(ctx, `PRAGMA journal_mode=WAL;`); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, `PRAGMA synchronous=NORMAL;`); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, `PRAGMA busy_timeout=5000;`); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}

// executeReadyTasks 负责装填并拉起高并发冲击
func executeReadyTasks(db *sql.DB, rdb *redis.Client, client *http.Client) {
	var tasks []TaskContext

	for i := 0; i < 500; i++ {
		val, err := rdb.LPop(ctx, "list:ready_tasks").Result()
		if err == redis.Nil {
			break
		} else if err != nil {
			log.Printf("❌ [ERROR] Redis LPOP failed: %v", err)
			break
		}

		var task TaskContext
		if err := json.Unmarshal([]byte(val), &task); err != nil {
			log.Printf("❌ [ERROR] Task unmarshal failed: %v", err)
			continue
		}
		tasks = append(tasks, task)
	}

	if len(tasks) == 0 {
		log.Println("⏳ [INFO] Trigger received, but 'list:ready_tasks' is empty. Ignoring.")
		return
	}

	log.Printf("🚀 [BLAST] Assembled %d prepared contexts. Initiating concurrent submission...", len(tasks))

	var wg sync.WaitGroup
	for _, task := range tasks {
		wg.Add(1)
		go func(t TaskContext) {
			defer wg.Done()
			blastTarget(db, client, t)
		}(task)
	}

	wg.Wait()
	log.Println("🏁 [INFO] All concurrent transactions completed for this wave.")
}

// blastTarget 执行最终的网络 I/O 动作，并将结果写回 SQLite
func blastTarget(db *sql.DB, client *http.Client, task TaskContext) {
	finalPayload := []byte(task.PayloadTemplate)

	req, err := http.NewRequest("POST", task.TargetEndpoint, bytes.NewBuffer(finalPayload))
	if err != nil {
		log.Printf("❌ [Task %s] Request build failed: %v", task.TaskUUID, err)
		if dbErr := updateTaskStatus(db, task.TaskUUID, "failed"); dbErr != nil {
			log.Printf("❌ [Task %s] SQLite update failed after request build error: %v", task.TaskUUID, dbErr)
		}
		return
	}

	for k, v := range task.HTTPContext {
		req.Header.Set(k, v)
	}

	start := time.Now()
	resp, err := client.Do(req)
	duration := time.Since(start)

	if err != nil {
		log.Printf("💥 [Task %s] Network failure: %v (took %v)", task.TaskUUID, err, duration)
		if dbErr := updateTaskStatus(db, task.TaskUUID, "failed"); dbErr != nil {
			log.Printf("❌ [Task %s] SQLite update failed after network error: %v", task.TaskUUID, dbErr)
		}
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		log.Printf("✅ [Task %s] SUCCESS! Status: %d, Time: %v. Response: %s", task.TaskUUID, resp.StatusCode, duration, string(body)[:min(100, len(body))])
		if dbErr := updateTaskStatus(db, task.TaskUUID, "success"); dbErr != nil {
			log.Printf("❌ [Task %s] SQLite update failed after success: %v", task.TaskUUID, dbErr)
		}
	} else {
		log.Printf("⚠️ [Task %s] FAILED. Status: %d, Time: %v. Response: %s", task.TaskUUID, resp.StatusCode, duration, string(body)[:min(100, len(body))])
		if dbErr := updateTaskStatus(db, task.TaskUUID, "failed"); dbErr != nil {
			log.Printf("❌ [Task %s] SQLite update failed after non-2xx response: %v", task.TaskUUID, dbErr)
		}
	}
}

func updateTaskStatus(db *sql.DB, taskID string, status string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	_, err = tx.ExecContext(ctx,
		`UPDATE tasks SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE task_id = ?`,
		status,
		taskID,
	)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
