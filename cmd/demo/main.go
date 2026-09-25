package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/lisama569/async-job-queue-go/queue"
)

func main() {
	logger := log.New(os.Stdout, "", log.Ltime|log.Lmicroseconds)

	var n atomic.Int64
	h := func(ctx context.Context, payload []byte) error {
		i := n.Add(1)
		key := string(payload)
		switch {
		case key == "always-ok":
			return nil
		case key == "always-fail":
			return errors.New("persistent failure")
		default:
			if i%3 != 0 { // 前两次失败，第三次成功，演示退避重试
				return fmt.Errorf("transient failure #%d", i)
			}
			return nil
		}
	}

	q := queue.New(h,
		queue.WithWorkers(2),
		queue.WithCapacity(4),
		queue.WithMaxAttempts(4),
		queue.WithBackoff(10*time.Millisecond, 100*time.Millisecond),
		queue.WithLogger(slogAdapter{logger}),
	)

	for _, key := range []string{"always-ok", "retry-then-ok", "always-fail"} {
		if err := q.Enqueue(queue.Job{Key: key, Payload: []byte(key)}); err != nil {
			logger.Printf("enqueue %s rejected: %v", key, err)
		}
	}
	// 重复提交：同 Key 仍活跃时被明确拒绝。
	if err := q.Enqueue(queue.Job{Key: "always-ok"}); err != nil {
		logger.Printf("duplicate enqueue rejected as expected: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := q.Shutdown(ctx); err != nil {
		logger.Fatalf("shutdown: %v", err)
	}

	logger.Printf("final metrics: %+v", q.Metrics())
}

type slogAdapter struct{ l *log.Logger }

func (s slogAdapter) Printf(format string, args ...any) {
	s.l.Printf("[queue] "+format, args...)
}
