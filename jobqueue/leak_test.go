package jobqueue

import (
	"context"
	"runtime"
	"testing"
	"time"
)

func TestNoGoroutineLeakAfterBoundedClose(t *testing.T) {
	h := newStuckHandler()
	q := mustNew(t, Config{
		Capacity: 8, Workers: 4, MaxAttempts: 2,
		InitialBackoff: time.Millisecond, MaxBackoff: 3 * time.Millisecond, BackoffMultiplier: 2,
	}, h)
	for i := 0; i < 4; i++ {
		id := "s-" + string(rune('a'+i))
		h.gate(id)
		if err := q.Enqueue(id, "t", nil); err != nil {
			t.Fatal(err)
		}
	}
	before := runtime.NumGoroutine()
	ctx, c := context.WithTimeout(context.Background(), 20*time.Millisecond)
	if _, err := q.Close(ctx); err == nil {
		t.Fatal("want deadline")
	}
	c()
	for i := 0; i < 4; i++ {
		close(h.gate("s-" + string(rune('a'+i))))
	}
	if _, err := q.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 给 worker/调度 goroutine 退出时间。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		runtime.Gosched()
		if runtime.NumGoroutine() <= before+1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("goroutine leak: before=%d after=%d", before, runtime.NumGoroutine())
}
