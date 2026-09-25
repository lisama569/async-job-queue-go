package jobqueue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testLogger 把任务标识与每次状态迁移打印到单测日志。
type testLogger struct{ t *testing.T }

func (l testLogger) Printf(format string, args ...any) {
	l.t.Logf("[queue] "+format, args...)
}

// flakyHandler 按任务 ID 配置前 failN 次失败、是否 panic，并用 channel 模拟慢任务。
type flakyHandler struct {
	mu       sync.Mutex
	failN    map[string]int
	panicIDs map[string]bool
	started  map[string]chan struct{}
	release  map[string]chan struct{}
	runs     map[string]int
	block    bool
}

func newFlakyHandler() *flakyHandler {
	return &flakyHandler{
		failN:    map[string]int{},
		panicIDs: map[string]bool{},
		started:  map[string]chan struct{}{},
		release:  map[string]chan struct{}{},
		runs:     map[string]int{},
	}
}

func (h *flakyHandler) gate(id string) (<-chan struct{}, chan<- struct{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.started[id]; !ok {
		h.started[id] = make(chan struct{})
		h.release[id] = make(chan struct{})
	}
	return h.started[id], h.release[id]
}

func (h *flakyHandler) runsOf(id string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.runs[id]
}

func (h *flakyHandler) Handle(ctx context.Context, t *Task) error {
	h.mu.Lock()
	h.runs[t.ID]++
	n := h.runs[t.ID]
	fail := h.failN[t.ID]
	doPanic := h.panicIDs[t.ID]
	start := h.started[t.ID]
	rel := h.release[t.ID]
	h.mu.Unlock()

	if start != nil {
		select {
		case <-start:
		default:
			close(start)
		}
	}
	if rel != nil {
		select {
		case <-rel:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if doPanic && n == 1 {
		panic("boom")
	}
	if n <= fail {
		return fmt.Errorf("simulated failure #%d for %s", n, t.ID)
	}
	return nil
}

func mustNew(t *testing.T, cfg Config, h Handler) *Queue {
	t.Helper()
	q, err := New(cfg, h, testLogger{t})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return q
}

func assertConserved(t *testing.T, s Stats) {
	t.Helper()
	if !s.Conserved() {
		t.Fatalf("counts not conserved: %+v (done=%d, inFlight=%d)",
			s, s.Done(), s.InFlight())
	}
	if s.InFlight() != 0 {
		t.Fatalf("in-flight tasks remain after close: %+v", s)
	}
}

func TestEnqueueRejectReasons(t *testing.T) {
	h := newFlakyHandler()
	q := mustNew(t, Config{
		Capacity: 2, Workers: 2, MaxAttempts: 1,
		InitialBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond, BackoffMultiplier: 2,
	}, h)

	if err := q.Enqueue("", "x", nil); !errors.Is(err, ErrInvalidTask) {
		t.Fatalf("empty id: want ErrInvalidTask, got %v", err)
	}
	// 占住前两个任务（gate 阻塞执行），使队列保持在途满载。
	for _, id := range []string{"j1", "j2"} {
		h.gate(id)
		if err := q.Enqueue(id, "t", nil); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}
	waitStarted(t, h, "j1")
	waitStarted(t, h, "j2")

	if err := q.Enqueue("j3", "t", nil); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("full queue: want ErrQueueFull, got %v", err)
	}
	if !IsQueueFull(q.Enqueue("j4", "t", nil)) {
		t.Fatalf("IsQueueFull predicate failed")
	}

	closeAll(h, "j1", "j2")
	stats, err := q.Close(context.Background())
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	assertConserved(t, stats)

	// 关闭后的拒绝必须明确为 ErrQueueClosed。
	if err := q.Enqueue("j5", "t", nil); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("closed queue: want ErrQueueClosed, got %v", err)
	}
	if !IsQueueClosed(q.Enqueue("j6", "t", nil)) {
		t.Fatalf("IsQueueClosed predicate failed")
	}
}

func closeAll(h *flakyHandler, ids ...string) {
	for _, id := range ids {
		_, rel := h.gate(id)
		close(rel)
	}
}

// waitStarted 经加锁的 gate 访问 started channel，避免与消费 goroutine 竞争。
func waitStarted(t *testing.T, h *flakyHandler, id string) {
	t.Helper()
	ch, _ := h.gate(id)
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatalf("task %s never started", id)
	}
}

func TestDedupConcurrentRunsOnce(t *testing.T) {
	h := newFlakyHandler()
	q := mustNew(t, DefaultConfig(), h)

	const submitters = 32
	var wg sync.WaitGroup
	var dupCount, okCount int64
	_, rel := h.gate("dedup-1")
	for i := 0; i < submitters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := q.Enqueue("dedup-1", "t", nil)
			switch {
			case err == nil:
				atomic.AddInt64(&okCount, 1)
			case errors.Is(err, ErrDuplicateTask):
				atomic.AddInt64(&dupCount, 1)
			default:
				t.Errorf("unexpected enqueue error: %v", err)
			}
		}()
	}
	wg.Wait()
	if okCount != 1 || dupCount != submitters-1 {
		t.Fatalf("accepted=%d duplicates=%d, want 1/%d", okCount, dupCount, submitters-1)
	}
	close(rel)

	stats, err := q.Close(context.Background())
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	assertConserved(t, stats)
	if h.runsOf("dedup-1") != 1 {
		t.Fatalf("handler ran %d times, want exactly 1", h.runsOf("dedup-1"))
	}
	if state, _ := q.TaskState("dedup-1"); state != StateSucceeded {
		t.Fatalf("state=%s, want SUCCEEDED", state)
	}
}

func TestRetryThenSuccess(t *testing.T) {
	h := newFlakyHandler()
	cfg := Config{
		Capacity: 8, Workers: 1, MaxAttempts: 4,
		InitialBackoff: 5 * time.Millisecond, MaxBackoff: 50 * time.Millisecond, BackoffMultiplier: 2,
	}
	q := mustNew(t, cfg, h)
	h.failN["flaky"] = 2 // 前两次失败，第三次成功

	if err := q.Enqueue("flaky", "t", nil); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s, _ := q.TaskState("flaky"); s == StateSucceeded {
			break
		}
		time.Sleep(time.Millisecond)
	}
	stats, err := q.Close(context.Background())
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	assertConserved(t, stats)
	if h.runsOf("flaky") != 3 {
		t.Fatalf("runs=%d, want 3", h.runsOf("flaky"))
	}
	if n, _ := q.TaskAttempts("flaky"); n != 3 {
		t.Fatalf("attempts=%d, want 3", n)
	}
	if stats.Succeeded != 1 {
		t.Fatalf("succeeded=%d, want 1", stats.Succeeded)
	}
}

func TestDeadLetterAfterMaxAttempts(t *testing.T) {
	h := newFlakyHandler()
	cfg := Config{
		Capacity: 4, Workers: 1, MaxAttempts: 3,
		InitialBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond, BackoffMultiplier: 2,
	}
	q := mustNew(t, cfg, h)
	h.failN["always-fail"] = 100

	if err := q.Enqueue("always-fail", "t", nil); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s, _ := q.TaskState("always-fail"); s == StateDeadLetter {
			break
		}
		time.Sleep(time.Millisecond)
	}
	stats, err := q.Close(context.Background())
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	assertConserved(t, stats)
	if h.runsOf("always-fail") != 3 {
		t.Fatalf("runs=%d, want 3 (no infinite retry)", h.runsOf("always-fail"))
	}
	if stats.DeadLetter != 1 {
		t.Fatalf("deadletter=%d, want 1", stats.DeadLetter)
	}
	lastErr, _ := q.TaskLastError("always-fail")
	if lastErr == "" {
		t.Fatalf("dead-letter task must keep observable failure reason")
	}
	t.Logf("dead letter reason: %s", lastErr)
}

func TestPanicCountsAsFailure(t *testing.T) {
	h := newFlakyHandler()
	cfg := Config{
		Capacity: 4, Workers: 1, MaxAttempts: 1,
		InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, BackoffMultiplier: 2,
	}
	q := mustNew(t, cfg, h)
	h.panicIDs["panicky"] = true

	if err := q.Enqueue("panicky", "t", nil); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s, _ := q.TaskState("panicky"); s.Terminal() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	stats, err := q.Close(context.Background())
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	assertConserved(t, stats)
	if h.runsOf("panicky") != 1 {
		t.Fatalf("runs=%d, want 1", h.runsOf("panicky"))
	}
	if stats.DeadLetter != 1 {
		t.Fatalf("deadletter=%d, want 1", stats.DeadLetter)
	}
}

func TestBackoffMonotonicAndBounded(t *testing.T) {
	q := mustNew(t, Config{
		Capacity: 4, Workers: 1, MaxAttempts: 5,
		InitialBackoff: 10 * time.Millisecond, MaxBackoff: 100 * time.Millisecond, BackoffMultiplier: 2,
	}, newFlakyHandler())
	defer q.Close(context.Background())

	var prev time.Duration
	for attempt := 1; attempt <= 6; attempt++ {
		d := q.backoff(attempt)
		if d < 10*time.Millisecond || d > 100*time.Millisecond {
			t.Fatalf("backoff(%d)=%s out of bounds", attempt, d)
		}
		if attempt > 1 && d+2*time.Millisecond < prev {
			t.Fatalf("backoff not monotonic: %s -> %s", prev, d)
		}
		prev = d
	}
}

func TestConcurrentProduceConsumeConservation(t *testing.T) {
	h := newFlakyHandler()
	cfg := Config{
		Capacity: 64, Workers: 8, MaxAttempts: 3,
		InitialBackoff: time.Millisecond, MaxBackoff: 4 * time.Millisecond, BackoffMultiplier: 2,
	}
	q := mustNew(t, cfg, h)

	const producers = 16
	const perProducer = 40
	var wg sync.WaitGroup
	var rejected int64
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				id := fmt.Sprintf("p%d-j%d", p, i)
				if err := q.Enqueue(id, "t", nil); err != nil {
					// 容量拒绝是明确的响应；本场景不做重试，单独计数。
					if errors.Is(err, ErrQueueFull) {
						atomic.AddInt64(&rejected, 1)
						continue
					}
					t.Errorf("unexpected enqueue err: %v", err)
				}
			}
		}(p)
	}
	wg.Wait()
	t.Logf("accepted=%d rejected(full)=%d",
		producers*perProducer-int(rejected), rejected)

	stats, err := q.Close(context.Background())
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	assertConserved(t, stats)
	total := producers * perProducer
	if stats.Accepted != total-int(rejected) {
		t.Fatalf("accepted=%d, want %d", stats.Accepted, total-int(rejected))
	}
	// 正常停机（无慢任务）不应产生待重试或丢弃。
	if stats.PendingRetry != 0 || stats.DeadLetter != 0 {
		t.Fatalf("unexpected shutdown states: %+v", stats)
	}
	if stats.Succeeded != stats.Accepted {
		t.Fatalf("succeeded=%d accepted=%d", stats.Succeeded, stats.Accepted)
	}
}

func TestShutdownInterruptionConservation(t *testing.T) {
	h := newFlakyHandler()
	cfg := Config{
		Capacity: 32, Workers: 4, MaxAttempts: 3,
		InitialBackoff: 5 * time.Millisecond, MaxBackoff: 50 * time.Millisecond, BackoffMultiplier: 2,
	}
	q := mustNew(t, cfg, h)

	// 4 个任务开始执行后阻塞（在途 RUNNING）。
	for i := 0; i < 4; i++ {
		id := fmt.Sprintf("running-%d", i)
		h.gate(id)
		if err := q.Enqueue(id, "t", nil); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}
	for i := 0; i < 4; i++ {
		waitStarted(t, h, fmt.Sprintf("running-%d", i))
	}

	// 4 个持续失败任务：关闭时应处于 RUNNING（gate 阻塞）或 RETRY_WAIT。
	for i := 0; i < 4; i++ {
		id := fmt.Sprintf("retry-%d", i)
		h.failN[id] = 100
		if err := q.Enqueue(id, "t", nil); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}

	// 再投递一批来不及执行的任务（PENDING）。
	for i := 0; i < 8; i++ {
		id := fmt.Sprintf("pending-%d", i)
		h.gate(id)
		if err := q.Enqueue(id, "t", nil); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}

	// 等一小段时间让 retry-* 至少失败一次进入退避。
	time.Sleep(30 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stats, err := q.Close(ctx)
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	assertConserved(t, stats)
	if stats.Accepted != 16 {
		t.Fatalf("accepted=%d, want 16", stats.Accepted)
	}
	if stats.Succeeded != 0 {
		t.Fatalf("succeeded=%d, want 0", stats.Succeeded)
	}
	if stats.DeadLetter != 0 {
		t.Fatalf("deadletter=%d, want 0 (backoff interrupted before max)", stats.DeadLetter)
	}
	if stats.PendingRetry != 16 {
		t.Fatalf("pendingRetry=%d, want 16 (all unfinished tasks handed back)", stats.PendingRetry)
	}
	t.Logf("shutdown stats: %+v", stats)
}

func TestShutdownMidBackoffDeadLetterPossible(t *testing.T) {
	h := newFlakyHandler()
	cfg := Config{
		Capacity: 8, Workers: 1, MaxAttempts: 2,
		InitialBackoff: time.Millisecond, MaxBackoff: 3 * time.Millisecond, BackoffMultiplier: 2,
	}
	q := mustNew(t, cfg, h)
	// 不停顿：任务快速失败 2 次后进入死信，验证重试上限与停机互不干扰。
	h.failN["fast-fail"] = 100
	if err := q.Enqueue("fast-fail", "t", nil); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s, _ := q.TaskState("fast-fail"); s == StateDeadLetter {
			break
		}
		time.Sleep(time.Millisecond)
	}
	stats, err := q.Close(context.Background())
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	assertConserved(t, stats)
	if stats.DeadLetter != 1 || stats.PendingRetry != 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestCloseCanceledThenReclose(t *testing.T) {
	h := newFlakyHandler()
	cfg := Config{
		Capacity: 8, Workers: 1, MaxAttempts: 2,
		InitialBackoff: 5 * time.Millisecond, MaxBackoff: 20 * time.Millisecond, BackoffMultiplier: 2,
	}
	q := mustNew(t, cfg, h)
	h.gate("j")
	if err := q.Enqueue("j", "t", nil); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	waitStarted(t, h, "j")

	// 已取消的 ctx：Close 应立即返回 ctx.Err()，且不得 panic。
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := q.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}

	// 释放阻塞任务后再次 Close：收尾继续完成、计数守恒、无 panic。
	closeAll(h, "j")
	stats, err := q.Close(context.Background())
	if err != nil {
		t.Fatalf("second close: %v", err)
	}
	assertConserved(t, stats)
	if stats.Accepted != 1 {
		t.Fatalf("accepted=%d, want 1", stats.Accepted)
	}
}
