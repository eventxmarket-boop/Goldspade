package main

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// EgressEngine 极限并发引擎
type EgressEngine struct {
	client      *http.Client
	concurrency int
}

// NewEgressEngine 初始化突破限制的 HTTP Client
func NewEgressEngine(concurrency int) *EgressEngine {
	transport := &http.Transport{
		MaxConnsPerHost:     concurrency, // 核心：解除单机连接数上限
		MaxIdleConns:        concurrency,
		MaxIdleConnsPerHost: concurrency,
		IdleConnTimeout:     30 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 15 * time.Second,
		}).DialContext,
		WriteBufferSize:  8192,
		ReadBufferSize:   8192,
		ForceAttemptHTTP2: false, // 降低旧版 F5 握手失败率
	}

	return &EgressEngine{
		client: &http.Client{
			Transport: transport,
			Timeout:   10 * time.Second, // 总体超时时间
		},
		concurrency: concurrency,
	}
}

// Fire 万箭齐发：瞬间拉起千级并发
func (e *EgressEngine) Fire(task *TaskContext) {
	var wg sync.WaitGroup
	var successCount, failCount atomic.Int32

	fmt.Printf("⚡️ [Engine] Firing %d concurrent requests for Task: %s\n", e.concurrency, task.TaskUUID)
	start := time.Now()

	for i := 0; i < e.concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			// 动态注入打码 Token 到 Payload
			payload := strings.Replace(task.PayloadTemplate, "{TOKEN}", task.AuthorizationToken, -1)

			req, err := http.NewRequest("POST", task.TargetEndpoint, bytes.NewBuffer([]byte(payload)))
			if err != nil {
				failCount.Add(1)
				return
			}

			// 挂载由 Python 预热好的真实 Cookie 和 User-Agent
			if cookie, ok := task.HTTPContext["Cookie"]; ok {
				req.Header.Set("Cookie", cookie)
			}
			if ua, ok := task.HTTPContext["User-Agent"]; ok {
				req.Header.Set("User-Agent", ua)
			}
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

			resp, err := e.client.Do(req)
			if err != nil {
				failCount.Add(1)
				return
			}
			defer resp.Body.Close()

			// 判断是否抢占成功 (视具体业务状态码而定，通常 200 或 302 算成功)
			if resp.StatusCode >= 200 && resp.StatusCode < 400 {
				successCount.Add(1)
			} else {
				failCount.Add(1)
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("🏁 [Engine] Task %s Finished in %v | Success: %d | Failed: %d\n",
		task.TaskUUID, elapsed, successCount.Load(), failCount.Load())
}
