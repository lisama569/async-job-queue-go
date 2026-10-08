package jobqueue

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// orderedHandler 记录每个业务键内任务的执行顺序，并校验：
//   - 同一 key 任意时刻至多一个任务在执行（串行）；
//   - 执行顺序等于提交顺序；
//   - 跨 key 可观察到并发（maxConcurrent >= 2）。
type orderedHandler struct {
	mu sync.Mutex
	// running 记录每个 key 当前在执行的任务 ID（至多 1 个）。
	running map[string]string
	// order 记录每个 key 实际开始执行的任务 ID 顺序（重试不重复入列）。
	order map[string][]string
	// seen 记录任务是否已计入 order。
	seen map[string]bool
	// overlaps 记录被观测到的跨 key 并发次数。
	overlaps int
	// totalRunning 当前全局在执行数量。
	totalRunning int
	maxRunning   int
	// gate: id -> release；任务开始后阻塞直到 release 关闭。
	block   map[string]bool
	started map[string]chan struct{}
	release map[string]chan struct{}
	// runningAt 记录每次执行时的全局并发数。
}

func newOrderedHandler() *orderedHandler {
	return &orderedHandler{
		running: map[string]string{},
		order:   map[string][]string{},
		seen:    map[string]bool{},
		block:   map[string]bool{},
		started: map[string]chan struct{}{},
		release: map[string]chan struct{}{},
	}
}

func (h *orderedHandler) gate(id string) chan<- struct{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.block[id] = true
	if h.started[id] == nil {
		h.started[id] = make(chan struct{})
		h.release[id] = make(chan struct{})
	}
	return h.release[id]
}

func (h *orderedHandler) Handle(_ context.Context, t *Task) error {
	h.enter(t)
	if block, rel, ok := h.gateOf(t.ID); ok {
		if block {
			<-rel
		}
	}
	h.leave(t)
	return nil
}

// gateOf 返回该任务是否阻塞及其 release channel（无 gate 时 ok=false）。
func (h *orderedHandler) gateOf(id string) (bool, chan struct{}, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	rel, ok := h.release[id]
	return h.block[id], rel, ok
}

// enter/leave 记录一次执行的开始与结束，用于串行/顺序/并发度校验。
func (h *orderedHandler) enter(t *Task) {
	// 无分组任务以 ID 作为独立串行域，互不约束。
	domain := t.key
	if domain == "" {
		domain = "\x00" + t.ID
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if cur, ok := h.running[domain]; ok {
		panic(fmt.Sprintf("ORDER VIOLATION key=%s task=%s overlaps with %s", t.key, t.ID, cur))
	}
	h.running[domain] = t.ID
	if t.key != "" {
		// 仅在该任务第一次开始执行时入列（重试不重复入列）。
		if !h.seen[t.ID] {
			h.seen[t.ID] = true
			h.order[t.key] = append(h.order[t.key], t.ID)
		}
	}
	h.totalRunning++
	if h.totalRunning > h.maxRunning {
		h.maxRunning = h.totalRunning
	}
	if h.totalRunning >= 2 {
		h.overlaps++
	}
	if c := h.started[t.ID]; c != nil {
		select {
		case <-c:
		default:
			close(c)
		}
	}
}

func (h *orderedHandler) leave(t *Task) {
	domain := t.key
	if domain == "" {
		domain = "\x00" + t.ID
	}
	h.mu.Lock()
	delete(h.running, domain)
	h.totalRunning--
	h.mu.Unlock()
}

func (h *orderedHandler) waitStart(t *testing.T, id string) {
	t.Helper()
	h.mu.Lock()
	c := h.started[id]
	h.mu.Unlock()
	select {
	case <-c:
	case <-time.After(3 * time.Second):
		t.Fatalf("task %s never started", id)
	}
}

func (h *orderedHandler) orderOf(key string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, len(h.order[key]))
	copy(out, h.order[key])
	return out
}

func (h *orderedHandler) overlapsCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.overlaps
}

func (h *orderedHandler) maxRunningCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.maxRunning
}

func orderedCfg() Config {
	return Config{
		Capacity: 128, Workers: 8, MaxAttempts: 1,
		InitialBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond, BackoffMultiplier: 2,
	}
}

// TestGroupOrderedFIFO 同一业务键内多个任务严格按提交顺序串行消费。
func TestGroupOrderedFIFO(t *testing.T) {
	h := newOrderedHandler()
	q := mustNew(t, orderedCfg(), h)

	const key = "order-123"
	const n = 20
	// 全部阻塞：若乱序，开始顺序即暴露；若同组并发，handler 直接 panic。
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("%s-t%02d", key, i)
		h.gate(id)
		if err := q.EnqueueKey(key, id, "t", nil); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}
	// 只有第一个应开始。
	h.waitStart(t, fmt.Sprintf("%s-t%02d", key, 0))
	time.Sleep(20 * time.Millisecond)
	if got := len(h.orderOf(key)); got != 1 {
		t.Fatalf("same key started %d tasks concurrently, want 1", got)
	}

	// 依次释放，验证每个都严格在前一个之后开始，且顺序为提交顺序。
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("%s-t%02d", key, i)
		close(h.gate(id))
		if i+1 < n {
			h.waitStart(t, fmt.Sprintf("%s-t%02d", key, i+1))
		}
	}

	stats, err := q.Close(context.Background())
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	assertConserved(t, stats)
	got := h.orderOf(key)
	if len(got) != n {
		t.Fatalf("ran %d, want %d", len(got), n)
	}
	for i, id := range got {
		want := fmt.Sprintf("%s-t%02d", key, i)
		if id != want {
			t.Fatalf("execution order mismatch at %d: got %s want %s; full=%v", i, id, want, got)
		}
	}
	t.Logf("key=%s executed %d tasks in exact FIFO order", key, n)
}

// TestGroupBlocksDuringRetry 同组前一个任务处于失败退避期间，后一个不得开始。
func TestGroupBlocksDuringRetry(t *testing.T) {
	h := newOrderedHandler()
	cfg := Config{
		Capacity: 16, Workers: 4, MaxAttempts: 3,
		InitialBackoff: 30 * time.Millisecond, MaxBackoff: 200 * time.Millisecond, BackoffMultiplier: 2,
	}
	q := mustNew(t, cfg, h)

	const key = "acct-9"
	first := key + "-a"
	second := key + "-b"

	// first：第 1 次失败；第 2 次尝试会阻塞在 retryGate，直到测试放行后成功。
	retryGate := make(chan struct{})
	ctrl := &retryGateHandler{wrapped: h, failFirst: first, gate: retryGate}
	q.handler = ctrl

	for _, id := range []string{first, second} {
		if err := q.EnqueueKey(key, id, "t", nil); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}

	// 第一次尝试直接失败，进入 RETRY_WAIT。
	waitState(t, q, first, StateRetryWait)

	// 退避约 30ms 后第 2 次尝试开始并阻塞在 retryGate；此时 first 仍在途（RUNNING）。
	waitState(t, q, first, StateRunning)

	// first 第 2 次尝试还在执行（阻塞），second 绝不能开始。
	time.Sleep(20 * time.Millisecond)
	if got := h.orderOf(key); len(got) != 1 || got[0] != first {
		t.Fatalf("second task started while predecessor unfinished: %v", got)
	}

	// 放行第二次尝试：first 成功 → 立即派发 second。
	close(retryGate)

	// 确认 second 已开始后再停机，避免停机与 handoff 的固有竞态。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(h.orderOf(key)) == 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	stats, err := q.Close(context.Background())
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	assertConserved(t, stats)
	got := h.orderOf(key)
	if len(got) != 2 || got[0] != first || got[1] != second {
		t.Fatalf("order after retry: %v, want [%s %s]", got, first, second)
	}
	if stats.Succeeded != 2 {
		t.Fatalf("succeeded=%d, want 2", stats.Succeeded)
	}
	if n, _ := q.TaskAttempts(first); n != 2 {
		t.Fatalf("first attempts=%d, want 2", n)
	}
	if n, _ := q.TaskAttempts(second); n != 1 {
		t.Fatalf("second attempts=%d, want 1", n)
	}
	t.Logf("key=%s blocked second task across RETRY_WAIT; final order=%v", key, got)
}

func waitState(t *testing.T, q *Queue, id string, want TaskState) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s, ok := q.TaskState(id); ok && s == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	s, _ := q.TaskState(id)
	t.Fatalf("task %s never reached %s (last=%s)", id, want, s)
}

// retryGateHandler：failFirst 的第一次执行返回错误；其后的执行先在 gate 上
// 阻塞，放行后委托 wrapped（成功）。其他任务直接委托。
type retryGateHandler struct {
	wrapped   Handler
	failFirst string
	gate      <-chan struct{}
	mu        sync.Mutex
	calls     int
}

func (r *retryGateHandler) Handle(ctx context.Context, t *Task) error {
	if t.ID == r.failFirst {
		r.mu.Lock()
		r.calls++
		n := r.calls
		r.mu.Unlock()
		oh := r.wrapped.(*orderedHandler)
		oh.enter(t)
		defer oh.leave(t)
		if n == 1 {
			// 第一次尝试直接失败，进入 RETRY_WAIT。
			return fmt.Errorf("transient failure for %s", t.ID)
		}
		// 后续尝试阻塞在 gate，直到测试放行后成功。
		select {
		case <-r.gate:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return r.wrapped.Handle(ctx, t)
}

// TestCrossGroupConcurrent 不同业务键之间仍可并发，不被串行化。
func TestCrossGroupConcurrent(t *testing.T) {
	h := newOrderedHandler()
	q := mustNew(t, orderedCfg(), h)

	const groups = 6
	for g := 0; g < groups; g++ {
		id := fmt.Sprintf("g%d-only", g)
		h.gate(id)
		if err := q.EnqueueKey(fmt.Sprintf("key-%d", g), id, "t", nil); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}
	for g := 0; g < groups; g++ {
		h.waitStart(t, fmt.Sprintf("g%d-only", g))
	}
	// 6 个不同 key 的任务全部同时在执行（Workers=8）。
	if mx := h.maxRunningCount(); mx < groups {
		t.Fatalf("cross-group parallelism=%d, want >= %d", mx, groups)
	}
	for g := 0; g < groups; g++ {
		close(h.gate(fmt.Sprintf("g%d-only", g)))
	}

	stats, err := q.Close(context.Background())
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	assertConserved(t, stats)
	if stats.Succeeded != groups {
		t.Fatalf("succeeded=%d, want %d", stats.Succeeded, groups)
	}
	if h.overlapsCount() == 0 {
		t.Fatalf("expected cross-group overlap, observed none")
	}
	t.Logf("cross-group max concurrent=%d (groups=%d), overlaps observed=%d",
		h.maxRunningCount(), groups, h.overlapsCount())
}

// TestGroupMixedWithUngrouped 分组任务与无分组任务可同时并发。
func TestGroupMixedWithUngrouped(t *testing.T) {
	h := newOrderedHandler()
	q := mustNew(t, orderedCfg(), h)

	ids := []string{"k0-a", "k1-a", "free-a", "free-b"}
	keys := []string{"K0", "K1", "-", "-"}
	for i, id := range ids {
		h.gate(id)
		key := keys[i]
		var k string
		if key != "-" {
			k = key
		}
		if err := q.EnqueueKey(k, id, "t", nil); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}
	for _, id := range ids {
		h.waitStart(t, id)
	}
	if mx := h.maxRunningCount(); mx != len(ids) {
		t.Fatalf("parallelism=%d, want %d (grouped + ungrouped)", mx, len(ids))
	}
	for _, id := range ids {
		close(h.gate(id))
	}
	stats, err := q.Close(context.Background())
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	assertConserved(t, stats)
}

// TestGroupImmediateShutdownHandsBack 同组任务入队后立刻停机：来不及执行的
// 后继（含排队中的）必须交还为 PENDING_RETRY，数量守恒，不静默丢弃。
func TestGroupImmediateShutdownHandsBack(t *testing.T) {
	h := newFlakyHandler()
	q := mustNew(t, Config{
		Capacity: 32, Workers: 2, MaxAttempts: 1,
		InitialBackoff: time.Millisecond, MaxBackoff: 3 * time.Millisecond, BackoffMultiplier: 2,
	}, h)
	const key = "imm-1"
	for i := 0; i < 5; i++ {
		if err := q.EnqueueKey(key, fmt.Sprintf("%s-%d", key, i), "t", nil); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}
	stats, err := q.Close(context.Background())
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	if !stats.Conserved() || stats.InFlight() != 0 {
		t.Fatalf("not conserved: %+v", stats)
	}
	if stats.Succeeded+stats.PendingRetry != 5 {
		t.Fatalf("succeeded=%d pendingRetry=%d, want sum 5", stats.Succeeded, stats.PendingRetry)
	}
	t.Logf("immediate grouped shutdown: succeeded=%d handedBack=%d",
		stats.Succeeded, stats.PendingRetry)
}
