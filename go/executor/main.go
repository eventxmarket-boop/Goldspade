package main

import (
	\"context\"
	\"encoding/json\"
	\"log\"
	\"os\"

	\"github.com/redis/go-redis/v9\"
)

// TaskContext 完美对齐 Python 端的 7 个核心字段
type TaskContext struct {
	TaskUUID          string            `json:\"task_uuid\"`
	BusinessZoneCode string            `json:\"business_zone_code\"`
	EgressNodeDSN    string            `json:\"egress_node_dsn\"`
	TargetEndpoint   string            `json:\"target_endpoint\"`
	HTTPContext      map[string]string `json:\"http_context\"` // 包含养熟的 Cookie 和 User-Agent
	AuthorizationToken string          `json:\"authorization_token\"`
	PayloadTemplate  string            `json:\"payload_template\"`
}

func main() {
	log.Println(\"🚀 Goldspade Executor (V2) is starting...\")

	// 1. 初始化 Redis 连接
	redisURL := os.Getenv(\"REDIS_URL\")
	if redisURL == \"\" {
		redisURL = \"redis://localhost:6379/0\"
	}

	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Fatalf(\"❌ Failed to parse Redis URL: %v\", err)
	}
	rdb := redis.NewClient(opt)
	ctx := context.Background()

	// 测试 Redis 连通性
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf(\"❌ Failed to connect to Redis: %v\", err)
	}
	log.Println(\"✅ Connected to Redis successfully. Waiting for payloads...\")

	// 2. 阻塞监听 list:ready_tasks 队列 (BRPOP)
	for {
		// BRPOP 会阻塞直到队列中有数据，0 表示无限等待
		result, err := rdb.BRPop(ctx, 0, \"list:ready_tasks\").Result()
		if err != nil {
			log.Printf(\"⚠️ Error popping from Redis: %v\", err)
			continue
		}

		// result[0] 是 key 名字，result[1] 是真实的 JSON 数据
		rawPayload := result[1]

		var task TaskContext
		if err := json.Unmarshal([]byte(rawPayload), &task); err != nil {
			log.Printf(\"❌ Failed to parse task JSON: %v. Raw: %s\", err, rawPayload)
			continue
		}

		log.Printf(\"🔥 [Signal Received] TaskUUID: %s\", task.TaskUUID)
		log.Printf(\" ├─ Target: %s\", task.TargetEndpoint)
		log.Printf(\" ├─ Egress: %s\", task.EgressNodeDSN)
		log.Printf(\" └─ Cookie Length: %d bytes\", len(task.HTTPContext[\"Cookie\"]))

		// 设定并发数为 500 (实战时可调高至 1000)
		engine := NewEgressEngine(500)
		// 异步开火，绝不阻塞下一条任务的读取
		go engine.Fire(&task)
	}
}