package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

type Executor struct {
	rdb *redis.Client
}

func NewExecutor() *Executor {
	return &Executor{
		rdb: redis.NewClient(&redis.Options{
			Addr: "127.0.0.1:6379",
		}),
	}
}

func (e *Executor) Run(ctx context.Context) error {
	pubsub := e.rdb.Subscribe(ctx, "channel:task_trigger")
	defer pubsub.Close()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			msg, err := pubsub.ReceiveMessage(ctx)
			if err != nil {
				time.Sleep(time.Second)
				continue
			}
			e.handleTrigger(ctx, msg.Payload)
		}
	}
}

func (e *Executor) handleTrigger(ctx context.Context, payload string) {
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = e.rdb.LPop(ctx, "list:ready_tasks").Result()
			_ = payload
		}()
	}
	wg.Wait()
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	executor := NewExecutor()
	if err := executor.Run(ctx); err != nil && err != context.Canceled {
		log.Fatal(err)
	}
}
