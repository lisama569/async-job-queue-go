package jobqueue_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lisama569/async-job-queue-go/jobqueue"
)

// Example 演示：前两次失败、第三次成功的任务，经退避重试后完成；
// 重复提交被去重拒绝；停机后计数守恒。
func Example() {
	var calls int
	h := jobqueue.HandlerFunc(func(ctx context.Context, t *jobqueue.Task) error {
		calls++
		if calls < 3 {
			return errors.New("transient error")
		}
		return nil
	})

	q, err := jobqueue.New(jobqueue.Config{
		Capacity:          8,
		Workers:           1,
		MaxAttempts:       3,
		InitialBackoff:    time.Millisecond,
		MaxBackoff:        10 * time.Millisecond,
		BackoffMultiplier: 2,
	}, h, nil)
	if err != nil {
		panic(err)
	}

	_ = q.Enqueue("demo-1", "demo", []byte("payload"))

	// 任务尚未完成时重复提交：明确返回 ErrDuplicateTask。
	if err := q.Enqueue("demo-1", "demo", nil); errors.Is(err, jobqueue.ErrDuplicateTask) {
		fmt.Println("duplicate rejected")
	}

	// 等待任务走到终态。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s, _ := q.TaskState("demo-1"); s.Terminal() {
			break
		}
		time.Sleep(time.Millisecond)
	}

	state, _ := q.TaskState("demo-1")
	attempts, _ := q.TaskAttempts("demo-1")
	stats, err := q.Close(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Printf("state=%s attempts=%d succeeded=%d conserved=%t\n",
		state, attempts, stats.Succeeded, stats.Conserved())

	// 关闭后写入被明确拒绝。
	err = q.Enqueue("demo-2", "demo", nil)
	fmt.Println("after close:", errors.Is(err, jobqueue.ErrQueueClosed))

	// Output:
	// duplicate rejected
	// state=SUCCEEDED attempts=3 succeeded=1 conserved=true
	// after close: true
}
