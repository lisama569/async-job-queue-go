package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testLogger 把队列内部的每次状态迁移打印到单测日志（含任务标识）。
type testLogger struct{ t *testing.T }

func (l testLogger) Printf(format string, args ...any) {
	l.t.Helper()
	l.t.Logf("[queue] "+format, args...)
}

// waitFor 轮询断言，避免测试硬睡眠。
func waitFor(timeout time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return fn()
}

func newTestQueue(t *testing.T, h Handler, opts ...Option) *Queue {
	t.Helper()
	all := append([]Option{WithLogger(testLogger{t})}, opts...)
	return New(h, all...)
}

func TestEnqueueAndSucceed(t *testing.T) {
	q := newTestQueue(t, func(ctx context.Context, payload []byte) error {
		return nil
	}, WithWorkers(2), WithCapacity(4), WithMaxAttempts(1))

	if err := q.Enqueue(Job{Key: "j-ok", Payload: []byte("hi")}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	if !waitFor(2*time.Second, func() bool {
		r, ok := q.Result("j-ok")
		return ok && r.Status == StatusSucceeded
	}) {
		t.Fatal("job did not reach SUCCEEDED")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := q.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	m := q.Metrics()
	if m.Enqueued != 1 || m.Succeeded != 1 || m.Discarded != 0 {
		t.Fatalf("unexpected metrics: %+v", m)
	}
}

func TestRetryThenSuccess(t *testing.T) {
	var attempts atomic.Int32
	q := newTestQueue(t, func(ctx context.Context, payload []byte) error {
		if attempts.Add(1) < 3 {
			return errors.New("transient failure")
		}
		return nil
	}, WithWorkers(1), WithMaxAttempts(5),
		WithBackoff(time.Millisecond, 20*time.Millisecond))

	if err := q.Enqueue(Job{Key: "j-retry"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if !waitFor(2*time.Second, func() bool {
		r, ok := q.Result("j-retry")
		return ok && r.Status == StatusSucceeded
	}) {
		t.Fatal("job never succeeded")
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want 3", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := q.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if m := q.Metrics(); m.Retried != 2 || m.Succeeded != 1 {
		t.Fatalf("metrics = %+v", m)
	}
}

func TestDiscardedAfterMaxAttempts(t *testing.T) {
	q := newTestQueue(t, func(ctx context.Context, payload []byte) error {
		return errors.New("boom")
	}, WithWorkers(1), WithMaxAttempts(3),
		WithBackoff(time.Millisecond, 5*time.Millisecond))

	if err := q.Enqueue(Job{Key: "j-fail"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if !waitFor(2*time.Second, func() bool {
		r, ok := q.Result("j-fail")
		return ok && r.Status == StatusDiscarded
	}) {
		t.Fatal("job was not discarded")
	}
	r, _ := q.Result("j-fail")
	if r.Attempts != 3 {
		t.Fatalf("attempts = %d, want 3", r.Attempts)
	}
	if !strings.Contains(r.LastError, "boom") {
		t.Fatalf("failure reason not preserved: %q", r.LastError)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := q.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	m := q.Metrics()
	if m.Enqueued != 1 || m.Discarded != 1 || m.Succeeded != 0 {
		t.Fatalf("metrics = %+v", m)
	}
}

func TestPanicRecoveredAsFailure(t *testing.T) {
	q := newTestQueue(t, func(ctx context.Context, payload []byte) error {
		panic("kaboom")
	}, WithWorkers(1), WithMaxAttempts(1))

	if err := q.Enqueue(Job{Key: "j-panic"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if !waitFor(2*time.Second, func() bool {
		_, ok := q.Result("j-panic")
		return ok
	}) {
		t.Fatal("job never reached terminal")
	}
	r, _ := q.Result("j-panic")
	if r.Status != StatusDiscarded || !strings.Contains(r.LastError, "panic recovered") {
		t.Fatalf("unexpected result: %+v", r)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = q.Shutdown(ctx)
}

func TestDuplicateRejectedWhileActive(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	q := newTestQueue(t, func(ctx context.Context, payload []byte) error {
		select {
		case <-started:
		default:
			close(started)
		}
		<-release
		return nil
	}, WithWorkers(1), WithCapacity(8))

	if err := q.Enqueue(Job{Key: "dup"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	<-started
	// 同一逻辑任务在 RUNNING 期间重复提交，必须被明确拒绝且不会多执行。
	if err := q.Enqueue(Job{Key: "dup"}); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("want ErrDuplicate, got %v", err)
	}
	close(release)

	if !waitFor(2*time.Second, func() bool {
		r, ok := q.Result("dup")
		return ok && r.Status == StatusSucceeded
	}) {
		t.Fatal("job did not finish")
	}
	// 到达终态后同一 Key 允许作为新任务再次提交。
	if err := q.Enqueue(Job{Key: "dup"}); err != nil {
		t.Fatalf("re-enqueue after terminal: %v", err)
	}
	if !waitFor(2*time.Second, func() bool {
		return q.Metrics().Succeeded == 2
	}) {
		t.Fatal("second job did not run")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = q.Shutdown(ctx)
	if m := q.Metrics(); m.Enqueued != 2 || m.Duplicates != 1 {
		t.Fatalf("metrics = %+v", m)
	}
}

func TestRejectFullAndClosed(t *testing.T) {
	gate := make(chan struct{})
	started := make(chan struct{})
	var startOnce sync.Once
	q := newTestQueue(t, func(ctx context.Context, payload []byte) error {
		startOnce.Do(func() { close(started) })
		<-gate
		return nil
	}, WithWorkers(1), WithCapacity(1), WithMaxAttempts(1))

	if err := q.Enqueue(Job{Key: "c1"}); err != nil {
		t.Fatalf("c1: %v", err)
	}
	// 等 c1 确实被唯一的消费者取走并阻塞，此时通道才有确定的 1 个空闲槽。
	<-started
	if err := q.Enqueue(Job{Key: "c2"}); err != nil {
		t.Fatalf("c2: %v", err)
	}
	// 1 个槽位已被 c2 占满 -> 容量不足。
	if err := q.Enqueue(Job{Key: "c3"}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("want ErrQueueFull, got %v", err)
	}
	close(gate)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := q.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	// 停机后写入必须被明确拒绝。
	if err := q.Enqueue(Job{Key: "c4"}); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("want ErrQueueClosed, got %v", err)
	}
	m := q.Metrics()
	if m.Enqueued != 2 || m.Rejected != 2 {
		t.Fatalf("metrics = %+v", m)
	}
}

func TestBackoffMonotonicAndBounded(t *testing.T) {
	cfg := Config{BaseBackoff: 10 * time.Millisecond, MaxBackoff: 100 * time.Millisecond}
	prev := time.Duration(0)
	for attempt := 1; attempt <= 8; attempt++ {
		d := backoff(cfg, attempt)
		if d < 0 || d > cfg.MaxBackoff {
			t.Fatalf("attempt %d backoff %v out of bounds", attempt, d)
		}
		if d < prev {
			t.Fatalf("backoff not monotonic: %v < %v", d, prev)
		}
		prev = d
	}
	if got := backoff(cfg, 1); got != 10*time.Millisecond {
		t.Fatalf("first backoff = %v", got)
	}
	if got := backoff(cfg, 8); got != cfg.MaxBackoff {
		t.Fatalf("capped backoff = %v, want %v", got, cfg.MaxBackoff)
	}
}

func TestConcurrentProducersConsumersConservation(t *testing.T) {
	const producers = 8
	const perProducer = 50
	const failUntilAttempt = 2 // 每个任务前 2 次失败，第 3 次成功

	var mu sync.Mutex
	attempts := make(map[string]int)

	q := newTestQueue(t, func(ctx context.Context, payload []byte) error {
		key := string(payload)
		mu.Lock()
		attempts[key]++
		n := attempts[key]
		mu.Unlock()
		if n <= failUntilAttempt {
			return fmt.Errorf("simulated failure #%d for %s", n, key)
		}
		return nil
	}, WithWorkers(4), WithCapacity(32), WithMaxAttempts(5),
		WithBackoff(time.Millisecond, 10*time.Millisecond))

	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				key := fmt.Sprintf("p%d-j%03d", p, i)
				// 容量不足是明确的背压信号：生产者短暂让出后重试，
				// 最终每个逻辑任务都应恰好入队一次。
				for {
					err := q.Enqueue(Job{Key: key, Payload: []byte(key)})
					if err == nil {
						break
					}
					if errors.Is(err, ErrQueueFull) {
						time.Sleep(time.Millisecond)
						continue
					}
					t.Errorf("enqueue %s: %v", key, err)
					return
				}
			}
		}(p)
	}
	wg.Wait()

	total := int64(producers * perProducer)
	if !waitFor(5*time.Second, func() bool {
		m := q.Metrics()
		return m.Succeeded+m.Discarded == total
	}) {
		t.Fatalf("not all jobs reached terminal: %+v", q.Metrics())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := q.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	m := q.Metrics()
	// 守恒：入队数 = 成功数 + 丢弃数；活跃任务必须清零。
	if m.Enqueued != total {
		t.Fatalf("enqueued = %d, want %d", m.Enqueued, total)
	}
	if m.Succeeded+m.Discarded != m.Enqueued {
		t.Fatalf("conservation broken: %+v", m)
	}
	if q.ActiveCount() != 0 {
		t.Fatalf("active jobs remain: %d", q.ActiveCount())
	}
	// 每个任务都恰好成功一次：去重保证重复提交/重试不会造成重复终态计数。
	if m.Succeeded != total {
		t.Fatalf("succeeded = %d, want %d (metrics %+v)", m.Succeeded, total, m)
	}
	t.Logf("concurrent metrics: %+v", m)
}

func TestShutdownDrainsRetryingJobs(t *testing.T) {
	// 退避设得很长，任务进入 RETRYING 后靠停机唤醒快速耗尽尝试次数。
	q := newTestQueue(t, func(ctx context.Context, payload []byte) error {
		return errors.New("always fail")
	}, WithWorkers(2), WithCapacity(16), WithMaxAttempts(4),
		WithBackoff(30*time.Second, time.Minute))

	const n = 10
	for i := 0; i < n; i++ {
		if err := q.Enqueue(Job{Key: fmt.Sprintf("s-%02d", i)}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	// 等待所有任务至少失败一次并进入退避。
	if !waitFor(2*time.Second, func() bool {
		return q.Metrics().Retried >= int64(n)
	}) {
		t.Fatalf("jobs did not enter retrying: %+v", q.Metrics())
	}

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := q.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("shutdown took too long: %v", elapsed)
	}

	m := q.Metrics()
	// 停机后已完成 + 待重试 + 已丢弃数量守恒：本用例全部走向丢弃终态。
	if m.Enqueued != n || m.Discarded != n || m.Succeeded != 0 {
		t.Fatalf("metrics after shutdown = %+v", m)
	}
	if q.ActiveCount() != 0 {
		t.Fatalf("active jobs remain: %d", q.ActiveCount())
	}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("s-%02d", i)
		r, ok := q.Result(key)
		if !ok || r.Status != StatusDiscarded || r.Attempts != 4 {
			t.Fatalf("unexpected terminal result for %s: %+v ok=%v", key, r, ok)
		}
	}
}

func TestShutdownMidFlightConservation(t *testing.T) {
	// 一半任务成功、一半始终失败（混合重试），在仍有在途任务时直接停机。
	var counter atomic.Int64
	q := newTestQueue(t, func(ctx context.Context, payload []byte) error {
		if counter.Add(1)%2 == 0 {
			return nil
		}
		return errors.New("odd attempt fails")
	}, WithWorkers(4), WithCapacity(64), WithMaxAttempts(3),
		WithBackoff(time.Millisecond, 10*time.Millisecond))

	const n = 40
	for i := 0; i < n; i++ {
		if err := q.Enqueue(Job{Key: fmt.Sprintf("m-%02d", i)}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := q.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	m := q.Metrics()
	if m.Enqueued != n {
		t.Fatalf("enqueued = %d, want %d", m.Enqueued, n)
	}
	if m.Succeeded+m.Discarded != n {
		t.Fatalf("conservation broken: %+v", m)
	}
	if q.ActiveCount() != 0 {
		t.Fatalf("active jobs remain: %d", q.ActiveCount())
	}
	if err := q.Enqueue(Job{Key: "after-close"}); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("want ErrQueueClosed, got %v", err)
	}
	t.Logf("mid-flight shutdown metrics: %+v", m)
}

func TestConcurrentDuplicateSubmissions(t *testing.T) {
	var runs atomic.Int64
	gate := make(chan struct{})
	q := newTestQueue(t, func(ctx context.Context, payload []byte) error {
		runs.Add(1)
		<-gate // 拉长 RUNNING 窗口，让并发重复提交都能观察到活跃任务
		return nil
	}, WithWorkers(2), WithCapacity(64), WithMaxAttempts(1))

	const n = 32
	var wg sync.WaitGroup
	var dupRejected, fullRejected, enqueued atomic.Int64
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			switch err := q.Enqueue(Job{Key: "hot-key"}); {
			case err == nil:
				enqueued.Add(1)
			case errors.Is(err, ErrDuplicate):
				dupRejected.Add(1)
			case errors.Is(err, ErrQueueFull):
				fullRejected.Add(1)
			}
		}()
	}
	wg.Wait()
	close(gate)

	if enqueued.Load() != 1 {
		t.Fatalf("enqueued = %d, want exactly 1", enqueued.Load())
	}
	if dupRejected.Load()+fullRejected.Load() != n-1 {
		t.Fatalf("rejected dup=%d full=%d, want total %d",
			dupRejected.Load(), fullRejected.Load(), n-1)
	}

	if !waitFor(2*time.Second, func() bool {
		r, ok := q.Result("hot-key")
		return ok && r.Status == StatusSucceeded
	}) {
		t.Fatal("job did not succeed")
	}
	if got := runs.Load(); got != 1 {
		t.Fatalf("handler runs = %d, want exactly 1", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := q.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	m := q.Metrics()
	if m.Enqueued != 1 || m.Succeeded != 1 {
		t.Fatalf("metrics = %+v", m)
	}
	t.Logf("duplicate submission metrics: %+v", m)
}
