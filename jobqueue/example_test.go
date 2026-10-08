package jobqueue_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lisama569/async-job-queue-go/jobqueue"
)

// Example 演示去重、退避重试、分组有序、死信重投与停机守恒。
func Example() {
	var mu sync.Mutex
	var calls int
	h := jobqueue.HandlerFunc(func(_ context.Context, t *jobqueue.Task) error {
		// 前两次失败、第三次成功的任务，经退避重试后完成。
		if t.ID == "demo-1" {
			mu.Lock()
			calls++
			n := calls
			mu.Unlock()
			if n < 3 {
				return errors.New("transient error")
			}
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

// ExampleEnqueueKey 演示按业务键有序消费：同一账户严格串行，不同账户并发。
func ExampleQueue_enqueueKey() {
	var mu sync.Mutex
	order := []string{}
	h := jobqueue.HandlerFunc(func(_ context.Context, t *jobqueue.Task) error {
		mu.Lock()
		order = append(order, t.ID)
		mu.Unlock()
		return nil
	})

	q, err := jobqueue.New(jobqueue.Config{
		Capacity: 16, Workers: 4, MaxAttempts: 1,
		InitialBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond, BackoffMultiplier: 2,
	}, h, nil)
	if err != nil {
		panic(err)
	}

	// 同一账户的三笔操作严格按提交顺序消费。
	ids := []string{"acct-1:deposit", "acct-1:coupon", "acct-1:receipt"}
	for _, id := range ids {
		if err := q.EnqueueKey("acct-1", id, "op", nil); err != nil {
			panic(err)
		}
	}

	// 等待三个任务都成功后再停机。
	dl := time.Now().Add(2 * time.Second)
	for time.Now().Before(dl) {
		done := 0
		for _, id := range ids {
			if s, _ := q.TaskState(id); s == jobqueue.StateSucceeded {
				done++
			}
		}
		if done == len(ids) {
			break
		}
		time.Sleep(time.Millisecond)
	}

	stats, err := q.Close(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println(order)
	fmt.Println("conserved:", stats.Conserved())

	// Output:
	// [acct-1:deposit acct-1:coupon acct-1:receipt]
	// conserved: true
}

// ExampleRequeue 演示死信任务的可控重投：重投是一次全新生命周期。
func ExampleQueue_requeue() {
	var mu sync.Mutex
	failed := map[string]bool{}
	h := jobqueue.HandlerFunc(func(_ context.Context, t *jobqueue.Task) error {
		mu.Lock()
		// 首个生命周期失败一次（MaxAttempts=1 → 死信）；重投后的 epoch=2 成功。
		shouldFail := t.Epoch() == 1 && !failed[t.ID]
		failed[t.ID] = true
		mu.Unlock()
		if shouldFail {
			return errors.New("downstream temporarily unavailable")
		}
		return nil
	})

	q, err := jobqueue.New(jobqueue.Config{
		Capacity: 8, Workers: 2, MaxAttempts: 1,
		InitialBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond, BackoffMultiplier: 2,
	}, h, nil)
	if err != nil {
		panic(err)
	}

	if err := q.Enqueue("order-9", "charge", nil); err != nil {
		panic(err)
	}
	// 等它进入死信。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s, _ := q.TaskState("order-9"); s == jobqueue.StateDeadLetter {
			break
		}
		time.Sleep(time.Millisecond)
	}
	st1, _ := q.TaskState("order-9")

	// 死信后重投：作为一次新生命周期，尝试次数重新统计。
	if err := q.Requeue("order-9"); err != nil {
		panic(err)
	}
	for time.Now().Before(deadline) {
		if s, _ := q.TaskState("order-9"); s == jobqueue.StateSucceeded {
			break
		}
		time.Sleep(time.Millisecond)
	}
	info, _ := q.TaskInfo("order-9")
	stats, err := q.Close(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Printf("first=%s requeued-origin=%s epoch=%d attempts=%d deadletter=%d succeeded=%d\n",
		st1, info.Origin, info.Epoch, info.Attempts, stats.DeadLetter, stats.Succeeded)

	// Output:
	// first=DEAD_LETTER requeued-origin=REQUEUE epoch=2 attempts=1 deadletter=1 succeeded=1
}
