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

// orderedHandler 记录每个业务键下任务进入/退出 Handler 的顺序，
// 并可用 barrier 让指定任务阻塞以制造前序任务未结束的窗口。
type orderedHandler struct {
	mu      sync.Mutex
	started []string
	done    []string
	running map[string]bool
	gates   map[string]chan struct{}
	failN   map[string]int
	runs    map[string]int
	logf    func(string, ...any)
}

func newOrderedHandler() *orderedHandler {
	return &orderedHandler{
		running: map[string]bool{},
		gates:   map[string]chan struct{}{},
		failN:   map[string]int{},
		runs:    map[string]int{},
	}
}

func (h *orderedHandler) gate(id string) <-chan struct{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	ch, ok := h.gates[id]
	if !ok {
		ch = make(chan struct{})
		h.gates[id] = ch
	}
	return ch
}

func (h *orderedHandler) snapshot() (started, done []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.started...), append([]string(nil), h.done...)
}

func (h *orderedHandler) runningCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.running)
}

func (h *orderedHandler) Handle(ctx context.Context, task *Task) error {
	h.mu.Lock()
	h.started = append(h.started, task.ID)
	h.running[task.ID] = true
	h.runs[task.ID]++
	n := h.runs[task.ID]
	fail := h.failN[task.ID]
	g := h.gates[task.ID]
	key := task.Key
	h.mu.Unlock()

	h.log("HANDLE START task=%s key=%s gen=%d attempt#=%d", task.ID, key, task.Generation(), n)
	if g != nil {
		select {
		case <-g:
		case <-ctx.Done():
			h.finish(task)
			return ctx.Err()
		}
	}
	h.finish(task)
	if n <= fail {
		h.log("HANDLE FAIL  task=%s key=%s attempt#=%d", task.ID, key, n)
		return fmt.Errorf("fail #%d", n)
	}
	h.log("HANDLE END   task=%s key=%s attempt#=%d ok", task.ID, key, n)
	return nil
}

func (h *orderedHandler) log(format string, args ...any) {
	if h.logf != nil {
		h.logf(format, args...)
	}
}

func (h *orderedHandler) finish(t *Task) {
	h.mu.Lock()
	h.done = append(h.done, t.ID)
	delete(h.running, t.ID)
	h.mu.Unlock()
}

func (h *orderedHandler) runsOf(id string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.runs[id]
}

// TestGroupedOrderedFIFO 验证同一业务键严格按提交顺序执行，
// 且前一个处于失败退避期间时后一个不会开始。
func TestGroupedOrderedFIFO(t *testing.T) {
	h := newOrderedHandler()
	h.logf = t.Logf
	cfg := Config{
		Capacity: 32, Workers: 4, MaxAttempts: 5,
		InitialBackoff: time.Millisecond, MaxBackoff: 3 * time.Millisecond, BackoffMultiplier: 2,
	}
	q := mustNew(t, cfg, h)

	// o1 前两次失败：验证 o2 不会在 o1 的退避窗口中被提前领取。
	h.failN["o1"] = 2
	ids := []string{"o1", "o2", "o3"}
	for _, id := range ids {
		if err := q.EnqueueWithKey(id, "order-1", "t", nil); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}

	waitRunCount(t, h, "o3", 1, 5*time.Second)
	stats, err := q.Close(context.Background())
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	assertConserved(t, stats)

	started, done := h.snapshot()
	if got := firstOccurrence(started); !equalSeq(got, ids) {
		t.Fatalf("start order=%v (raw=%v), want %v", got, started, ids)
	}
	if got := firstOccurrence(done); !equalSeq(got, ids) {
		t.Fatalf("done order=%v (raw=%v), want %v", got, done, ids)
	}
	if h.runsOf("o1") != 3 {
		t.Fatalf("o1 runs=%d, want 3", h.runsOf("o1"))
	}
	for _, id := range ids {
		if st, _ := q.TaskState(id); st != StateSucceeded {
			t.Fatalf("%s state=%s, want SUCCEEDED", id, st)
		}
	}
}

func waitRunCount(t *testing.T, h *orderedHandler, id string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if h.runsOf(id) >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("task %s never reached %d runs (got %d)", id, want, h.runsOf(id))
}

func firstOccurrence(seq []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range seq {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

func equalSeq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestGroupedHeadBlocking 头任务阻塞期间，同组后续任务不会被领取，
// 且同组任意时刻至多一个任务在 Handler 中执行。
func TestGroupedHeadBlocking(t *testing.T) {
	h := newOrderedHandler()
	h.logf = t.Logf
	q := mustNew(t, Config{
		Capacity: 32, Workers: 8, MaxAttempts: 3,
		InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, BackoffMultiplier: 2,
	}, h)

	// head 阻塞；sibling 同组必须等待。
	_ = h.gate("head")
	if err := q.EnqueueWithKey("head", "acct-9", "t", nil); err != nil {
		t.Fatalf("enqueue head: %v", err)
	}
	waitStartedOrdered(t, h, "head")
	for _, id := range []string{"sib-a", "sib-b"} {
		if err := q.EnqueueWithKey(id, "acct-9", "t", nil); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}

	// 头任务阻塞期间：同组没有任何后续任务开始。
	time.Sleep(40 * time.Millisecond)
	if n := h.runsOf("sib-a"); n != 0 {
		t.Fatalf("sib-a started %d times while head blocked", n)
	}
	if n := h.runsOf("sib-b"); n != 0 {
		t.Fatalf("sib-b started %d times while head blocked", n)
	}
	if h.runningCount() != 1 {
		t.Fatalf("running=%d, want exactly 1 (only head)", h.runningCount())
	}

	// 释放 head：两个后续任务按序执行，且全程同组并发度 <= 1。
	stop := make(chan struct{})
	var maxConcurrent int64
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			// 该计数只统计 acct-9 组：此刻系统里只有这三个任务。
			if n := h.runningCount(); n > 1 {
				atomic.StoreInt64(&maxConcurrent, int64(n))
			}
			time.Sleep(time.Millisecond)
		}
	}()
	close(h.gates["head"])
	waitRunCount(t, h, "sib-b", 1, 5*time.Second)
	close(stop)
	if atomic.LoadInt64(&maxConcurrent) > 1 {
		t.Fatalf("same-key concurrent execution observed: %d", maxConcurrent)
	}

	stats, err := q.Close(context.Background())
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	assertConserved(t, stats)
	for _, id := range []string{"head", "sib-a", "sib-b"} {
		if st, _ := q.TaskState(id); st != StateSucceeded {
			t.Fatalf("%s=%s, want SUCCEEDED", id, st)
		}
	}
}

func waitStartedOrdered(t *testing.T, h *orderedHandler, id string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if h.runsOf(id) > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("%s never started", id)
}

// TestDifferentKeysConcurrent 验证不同业务键（以及无键任务）之间仍可并发，
// 不会因为引入顺序保证而整体串行化。
func TestDifferentKeysConcurrent(t *testing.T) {
	h := newOrderedHandler()
	h.logf = t.Logf
	const keys = 4
	q := mustNew(t, Config{
		Capacity: 64, Workers: 8, MaxAttempts: 1,
		InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, BackoffMultiplier: 2,
	}, h)

	// 每个键一个阻塞头任务，共 4 个；另加 2 个无键阻塞任务。
	all := []string{}
	for k := 0; k < keys; k++ {
		id := fmt.Sprintf("k%d-head", k)
		_ = h.gate(id)
		if err := q.EnqueueWithKey(id, fmt.Sprintf("key-%d", k), "t", nil); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
		all = append(all, id)
	}
	for _, id := range []string{"free-1", "free-2"} {
		_ = h.gate(id)
		if err := q.Enqueue(id, "t", nil); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
		all = append(all, id)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if h.runningCount() == len(all) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if got := h.runningCount(); got != len(all) {
		t.Fatalf("parallel running=%d, want %d (different keys must run concurrently)", got, len(all))
	}

	for _, id := range all {
		close(h.gates[id])
	}
	stats, err := q.Close(context.Background())
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	assertConserved(t, stats)
	if stats.Succeeded != len(all) {
		t.Fatalf("succeeded=%d, want %d", stats.Succeeded, len(all))
	}
}

// TestRequeueDeadLetterRunsFreshLifecycle 死信任务重投后作为全新生命周期执行：
// attempts 清零、generation+1、计数独立且守恒、最终成功。
func TestRequeueDeadLetterRunsFreshLifecycle(t *testing.T) {
	h := newFlakyHandler()
	cfg := Config{
		Capacity: 8, Workers: 2, MaxAttempts: 2,
		InitialBackoff: time.Millisecond, MaxBackoff: 3 * time.Millisecond, BackoffMultiplier: 2,
	}
	q := mustNew(t, cfg, h)
	h.failN["rq"] = 100 // 第一轮 2 次必失败 -> DEAD_LETTER
	if err := q.Enqueue("rq", "t", nil); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	waitState(t, q, "rq", StateDeadLetter, 3*time.Second)

	firstAttempts, _ := q.TaskAttempts("rq")
	if firstAttempts != 2 {
		t.Fatalf("first lifecycle attempts=%d, want 2", firstAttempts)
	}

	// 改成重投后立即成功。
	h.mu.Lock()
	h.failN["rq"] = 0
	h.mu.Unlock()

	if err := q.Requeue("rq"); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	waitState(t, q, "rq", StateSucceeded, 3*time.Second)

	stats, err := q.Close(context.Background())
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	assertConserved(t, stats)
	// 旧生命周期的死信计数保留，新生命周期计入成功；Accepted=2、Requeued=1。
	if stats.Accepted != 2 || stats.Requeued != 1 {
		t.Fatalf("accepted=%d requeued=%d, want 2/1", stats.Accepted, stats.Requeued)
	}
	if stats.DeadLetter != 1 || stats.Succeeded != 1 {
		t.Fatalf("deadLetter=%d succeeded=%d, want 1/1", stats.DeadLetter, stats.Succeeded)
	}
	// 新生命周期尝试次数从 1 起算，不与旧的 2 次混淆。
	attempts, _ := q.TaskAttempts("rq")
	if attempts != 1 {
		t.Fatalf("fresh lifecycle attempts=%d, want 1", attempts)
	}
	// Handler 实际被调用 3 次（旧 2 + 新 1）。
	if runs := h.runsOf("rq"); runs != 3 {
		t.Fatalf("handler runs=%d, want 3", runs)
	}
	lastErr, _ := q.TaskLastError("rq")
	if lastErr != "" {
		t.Fatalf("fresh lifecycle lastErr=%q, want empty", lastErr)
	}
}

func waitState(t *testing.T, q *Queue, id string, want TaskState, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if st, ok := q.TaskState(id); ok && st == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	st, _ := q.TaskState(id)
	t.Fatalf("task %s state=%s, never reached %s", id, st, want)
}

// TestRequeueRejectionReasons 覆盖 Requeue 的各类明确拒绝原因。
func TestRequeueRejectionReasons(t *testing.T) {
	h := newFlakyHandler()
	cfg := Config{
		Capacity: 4, Workers: 1, MaxAttempts: 1,
		InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, BackoffMultiplier: 2,
	}
	q := mustNew(t, cfg, h)
	h.failN["dead"] = 100
	if err := q.Enqueue("dead", "t", nil); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := q.Enqueue("ok", "t", nil); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	waitState(t, q, "dead", StateDeadLetter, 3*time.Second)
	waitState(t, q, "ok", StateSucceeded, 3*time.Second)

	if err := q.Requeue("missing"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("missing: want ErrTaskNotFound, got %v", err)
	}
	if !IsTaskNotFound(q.Requeue("missing")) {
		t.Fatalf("IsTaskNotFound predicate failed")
	}
	if err := q.Requeue("ok"); !errors.Is(err, ErrNotRequeueable) {
		t.Fatalf("succeeded task: want ErrNotRequeueable, got %v", err)
	}
	if !IsNotRequeueable(q.Requeue("ok")) {
		t.Fatalf("IsNotRequeueable predicate failed")
	}

	stats, err := q.Close(context.Background())
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	assertConserved(t, stats)

	// 关闭后重投一律 ErrQueueClosed。
	if err := q.Requeue("dead"); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("after close: want ErrQueueClosed, got %v", err)
	}
}

// TestRequeueInFlightRejected 非死信（含 RUNNING）状态的任务不能重投；
// 并发对同一死信重投至多成功一次，其余得到 ErrDuplicateTask，绝不重复执行。
func TestRequeueInFlightAndConcurrentDedup(t *testing.T) {
	h := newFlakyHandler()
	cfg := Config{
		Capacity: 16, Workers: 2, MaxAttempts: 1,
		InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, BackoffMultiplier: 2,
	}
	q := mustNew(t, cfg, h)

	// RUNNING 中的任务不可重投。
	h.gate("live")
	if err := q.Enqueue("live", "t", nil); err != nil {
		t.Fatalf("enqueue live: %v", err)
	}
	waitStarted(t, h, "live")
	// RUNNING 在途任务重投：按去重拒绝（同一逻辑任务已有生命周期在跑）。
	if err := q.Requeue("live"); !errors.Is(err, ErrDuplicateTask) {
		t.Fatalf("running task: want ErrDuplicateTask, got %v", err)
	}
	closeAll(h, "live")

	// 制造一个死信，然后并发重投。
	h.failN["hot"] = 100
	if err := q.Enqueue("hot", "t", nil); err != nil {
		t.Fatalf("enqueue hot: %v", err)
	}
	waitState(t, q, "hot", StateDeadLetter, 3*time.Second)
	// 重投后改为成功，避免再次失败进入死信，确保只有一次重投机会。
	h.mu.Lock()
	h.failN["hot"] = 0
	h.mu.Unlock()

	const n = 32
	var wg sync.WaitGroup
	var ok, rejected, other int64
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := q.Requeue("hot")
			switch {
			case err == nil:
				atomic.AddInt64(&ok, 1)
			case errors.Is(err, ErrDuplicateTask) || errors.Is(err, ErrNotRequeueable):
				// 输掉竞争时：新生命周期可能仍在途（duplicate），
				// 也可能已飞快成功（succeeded -> not requeueable）。
				atomic.AddInt64(&rejected, 1)
			default:
				atomic.AddInt64(&other, 1)
				t.Errorf("unexpected requeue err: %v", err)
			}
		}()
	}
	wg.Wait()
	if ok != 1 || rejected != n-1 || other != 0 {
		t.Fatalf("requeue accepted=%d rejected=%d other=%d, want 1/%d/0", ok, rejected, other, n-1)
	}
	waitState(t, q, "hot", StateSucceeded, 3*time.Second)
	// 重投仅执行一次（新生命周期 MaxAttempts=1 且本次成功）。
	if runs := h.runsOf("hot"); runs != 2 {
		t.Fatalf("handler runs=%d, want 2 (dead-letter once + fresh once)", runs)
	}

	stats, err := q.Close(context.Background())
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	assertConserved(t, stats)
}

// TestRequeueRestoresGroupOrdering 带业务键的死信任务重投后，仍按该业务键
// 参与有序消费：它排在同组已有在途任务之后。
func TestRequeueRestoresGroupOrdering(t *testing.T) {
	h := newOrderedHandler()
	h.logf = t.Logf
	cfg := Config{
		Capacity: 16, Workers: 4, MaxAttempts: 1,
		InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, BackoffMultiplier: 2,
	}
	q := mustNew(t, cfg, h)

	// 1) dl 失败进死信（属于 order-7）。
	h.failN["dl"] = 100
	if err := q.EnqueueWithKey("dl", "order-7", "t", nil); err != nil {
		t.Fatalf("enqueue dl: %v", err)
	}
	waitState(t, q, "dl", StateDeadLetter, 3*time.Second)

	// 2) 先入一个会阻塞的同组任务 blocker 作为头；再重投 dl。
	_ = h.gate("blocker")
	if err := q.EnqueueWithKey("blocker", "order-7", "t", nil); err != nil {
		t.Fatalf("enqueue blocker: %v", err)
	}
	waitStartedOrdered(t, h, "blocker")

	h.mu.Lock()
	h.failN["dl"] = 0
	h.mu.Unlock()
	if err := q.Requeue("dl"); err != nil {
		t.Fatalf("requeue dl: %v", err)
	}

	// blocker 未释放：重投的 dl 虽是新生命周期，仍被同组头阻塞，不能开始。
	time.Sleep(30 * time.Millisecond)
	if n := h.runsOf("dl"); n != 1 {
		t.Fatalf("dl fresh run started %d times while blocker active, want 0 (only old dead run)", n-1)
	}

	close(h.gates["blocker"])
	waitRunCount(t, h, "dl", 2, 3*time.Second) // 旧死信 1 次 + 新生命周期 1 次

	started, _ := h.snapshot()
	firsts := firstOccurrence(started)
	if len(firsts) != 2 || firsts[0] != "dl" || firsts[1] != "blocker" {
		// dl 的首次运行是在 blocker 入队之前，因此顺序为 dl, blocker；
		// 重投后的第二次 dl 必须在 blocker 之后。
		t.Fatalf("unexpected first-run order: %v", firsts)
	}
	// 关键断言：重投后 dl 的第二次运行必须晚于 blocker 完成。
	dlSecondStart := indexNth(started, "dl", 2)
	blockerStart := indexNth(started, "blocker", 1)
	if dlSecondStart < 0 || blockerStart < 0 || dlSecondStart <= blockerStart {
		t.Fatalf("requeued dl ran before blocker head finished: %v", started)
	}

	stats, err := q.Close(context.Background())
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	assertConserved(t, stats)
	if st, _ := q.TaskState("dl"); st != StateSucceeded {
		t.Fatalf("dl=%s, want SUCCEEDED", st)
	}
}

func indexNth(seq []string, id string, nth int) int {
	count := 0
	for i, s := range seq {
		if s == id {
			count++
			if count == nth {
				return i
			}
		}
	}
	return -1
}

// stubbornHandler 的阻塞任务【忽略 ctx 取消】，只在 release 后返回，
// 用于真实模拟“限时停机到期时 Handler 尚未返回”的在途 RUNNING 任务。
type stubbornHandler struct {
	mu      sync.Mutex
	runs    map[string]int
	release map[string]chan struct{}
	started map[string]chan struct{}
}

func newStubbornHandler() *stubbornHandler {
	return &stubbornHandler{
		runs:    map[string]int{},
		release: map[string]chan struct{}{},
		started: map[string]chan struct{}{},
	}
}

func (h *stubbornHandler) gate(id string) (<-chan struct{}, chan<- struct{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.started[id]; !ok {
		h.started[id] = make(chan struct{})
		h.release[id] = make(chan struct{})
	}
	return h.started[id], h.release[id]
}

func (h *stubbornHandler) runsOf(id string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.runs[id]
}

func (h *stubbornHandler) Handle(ctx context.Context, t *Task) error {
	h.mu.Lock()
	h.runs[t.ID]++
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
		<-rel // 故意忽略 ctx.Done：模拟不可抢占的在途任务
	}
	return nil
}

func waitStartedStubborn(t *testing.T, h *stubbornHandler, id string) {
	t.Helper()
	ch, _ := h.gate(id)
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatalf("%s never started", id)
	}
}

// TestBoundedShutdownObservabilityAndConservation 限时停机：
// 超时返回时，除仍在执行的 RUNNING 外，其余未完成任务必须全部进入
// PENDING_RETRY；AllConserved 恒成立；释放在途任务后再次 Close 彻底守恒。
func TestBoundedShutdownObservabilityAndConservation(t *testing.T) {
	h := newStubbornHandler()
	cfg := Config{
		Capacity: 64, Workers: 4, MaxAttempts: 3,
		InitialBackoff: 5 * time.Millisecond, MaxBackoff: 50 * time.Millisecond, BackoffMultiplier: 2,
	}
	q := mustNew(t, cfg, h)

	// 4 个 worker 全部被阻塞在途任务占满（RUNNING，忽略 ctx），
	// 停机到期时它们必然仍在执行；后续排队任务没有空闲 worker 可领取。
	for i := 0; i < 4; i++ {
		id := fmt.Sprintf("run-%d", i)
		h.gate(id)
		if err := q.Enqueue(id, "t", nil); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}
	for i := 0; i < 4; i++ {
		waitStartedStubborn(t, h, fmt.Sprintf("run-%d", i))
	}
	// 一批来不及执行的排队任务（PENDING）。
	for i := 0; i < 12; i++ {
		id := fmt.Sprintf("pend-%d", i)
		if err := q.Enqueue(id, "t", nil); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}

	// 极短等待上限：在途任务必然来不及收尾。
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	s1, err := q.Close(ctx)
	if err == nil {
		t.Fatalf("bounded close: want deadline error, got nil; stats=%+v", s1)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want context.DeadlineExceeded, got %v", err)
	}

	// 超时报错的快照里：不能静默丢失——非 RUNNING 的在途任务全部终态化，
	// RUNNING 保留为明确可观测状态，总量守恒。
	if !s1.AllConserved() {
		t.Fatalf("bounded snapshot not conserved: %+v (done=%d inFlight=%d)",
			s1, s1.Done(), s1.InFlight())
	}
	if s1.Running != 4 {
		t.Fatalf("running=%d, want 4 observable in-flight tasks", s1.Running)
	}
	// 此时 PENDING/RETRY_WAIT 必须为 0：所有非执行中任务都已落终态。
	if s1.Pending != 0 || s1.RetryWait != 0 {
		t.Fatalf("non-running in-flight remain: pending=%d retryWait=%d",
			s1.Pending, s1.RetryWait)
	}
	if s1.PendingRetry != 12 {
		t.Fatalf("pendingRetry=%d, want 12 queued tasks handed back", s1.PendingRetry)
	}
	t.Logf("bounded shutdown stats: %+v", s1)

	// 关闭后投递必须明确拒绝。
	if err := q.Enqueue("after", "t", nil); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("enqueue during shutdown: want ErrQueueClosed, got %v", err)
	}

	// 释放在途任务：它们返回 nil（stubborn 忽略 ctx），归 SUCCEEDED；
	// 12 个排队任务已在超时时被强制归入 PENDING_RETRY，不会再被执行。
	for i := 0; i < 4; i++ {
		_, rel := h.gate(fmt.Sprintf("run-%d", i))
		close(rel)
	}
	s2, err := q.Close(context.Background())
	if err != nil {
		t.Fatalf("final close: %v", err)
	}
	assertConserved(t, s2)
	if s2.Accepted != 16 {
		t.Fatalf("accepted=%d, want 16", s2.Accepted)
	}
	if s2.Succeeded != 4 || s2.PendingRetry != 12 {
		t.Fatalf("want 4 succeeded + 12 pendingRetry, got %+v", s2)
	}
}

// TestConcurrentChaosConservation 生产、消费、重投与限时停机并发进行：
// 任意时刻总量守恒；最终严格守恒；重投不产生重复生命周期计数。
func TestConcurrentChaosConservation(t *testing.T) {
	h := newFlakyHandler()
	cfg := Config{
		Capacity: 32, Workers: 6, MaxAttempts: 2,
		InitialBackoff: time.Millisecond, MaxBackoff: 3 * time.Millisecond, BackoffMultiplier: 2,
	}
	q := mustNew(t, cfg, h)

	// 部分 ID 注定先死信，供重投 goroutine 使用。
	const groups = 8
	const perGroup = 6
	var wg sync.WaitGroup
	var enqueueOK int64

	// 生产者：带业务键，每键内多任务。
	for g := 0; g < groups; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			key := fmt.Sprintf("ord-%d", g)
			for i := 0; i < perGroup; i++ {
				id := fmt.Sprintf("g%d-j%d", g, i)
				if g%2 == 0 {
					h.mu.Lock()
					h.failN[id] = 100
					h.mu.Unlock()
				}
				err := q.EnqueueWithKey(id, key, "t", nil)
				switch {
				case err == nil:
					atomic.AddInt64(&enqueueOK, 1)
				case errors.Is(err, ErrQueueFull), errors.Is(err, ErrQueueClosed):
					// 满 / 关闭都是明确拒绝，不算丢失。
				default:
					t.Errorf("unexpected enqueue err: %v", err)
				}
			}
		}(g)
	}

	// 重投者：持续尝试把死信任务重投（直到队列关闭）。
	stopRequeue := make(chan struct{})
	var requeueOK int64
	for g := 0; g < groups; g += 2 {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGroup; i++ {
				id := fmt.Sprintf("g%d-j%d", g, i)
				deadline := time.Now().Add(time.Second)
				for time.Now().Before(deadline) {
					select {
					case <-stopRequeue:
						return
					default:
					}
					switch err := q.Requeue(id); {
					case err == nil:
						// 重投成功后改为最终成功，避免再次死信被二次重投。
						h.mu.Lock()
						h.failN[id] = 0
						h.mu.Unlock()
						atomic.AddInt64(&requeueOK, 1)
						goto next
					case errors.Is(err, ErrQueueClosed):
						return
					}
					time.Sleep(time.Millisecond)
				}
			next:
			}
		}()
	}

	// 停机观测者：在不同等待上限下反复拿快照，验证中途 AllConserved 恒成立。
	observerDone := make(chan struct{})
	go func() {
		defer close(observerDone)
		for {
			select {
			case <-q.finalDone:
				return
			case <-stopRequeue:
				return
			default:
				s := q.Stats()
				if !s.AllConserved() {
					t.Errorf("mid-shutdown conservation broken: %+v", s)
					return
				}
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	wg.Wait()

	// 限时停机：等待上限很短，必然经历超时返回。
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	s1, err1 := q.Close(ctx)
	cancel()
	if err1 != nil && !errors.Is(err1, context.DeadlineExceeded) &&
		!errors.Is(err1, context.Canceled) {
		t.Fatalf("bounded close err=%v", err1)
	}
	if err1 != nil && !s1.AllConserved() {
		t.Fatalf("bounded snapshot not conserved: %+v", s1)
	}

	// 等待收尾彻底完成后再次 Close。
	s2, err := q.Close(context.Background())
	if err != nil {
		t.Fatalf("final close: %v", err)
	}
	close(stopRequeue)
	<-observerDone

	assertConserved(t, s2)
	// Accepted 必须等于“唯一入队的任务数 + 成功重投次数”。
	wantAccepted := int(enqueueOK) + int(requeueOK)
	if s2.Accepted != wantAccepted {
		t.Fatalf("accepted=%d, want enqueued=%d + requeued=%d",
			s2.Accepted, groups*perGroup, requeueOK)
	}
	if s2.Requeued != int(requeueOK) {
		t.Fatalf("stats requeued=%d, observed=%d", s2.Requeued, requeueOK)
	}
	// 守恒式：成功 = 奇数组建造成功 + 成功重投数；死信 = 偶数组中未被重投的。
	t.Logf("chaos final: %+v requeueOK=%d", s2, requeueOK)
	if s2.Done() != s2.Accepted {
		t.Fatalf("done=%d accepted=%d", s2.Done(), s2.Accepted)
	}
}

// TestRequeueDuringShutdown 死信已存在时发起停机，停机期间并发 Requeue：
// 只有关闭前/关闭瞬间的重投可能成功，关闭后一律 ErrQueueClosed；
// 最终计数严格守恒，无任务丢失或状态覆盖。
func TestRequeueDuringShutdown(t *testing.T) {
	h := newFlakyHandler()
	cfg := Config{
		Capacity: 32, Workers: 4, MaxAttempts: 1,
		InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, BackoffMultiplier: 2,
	}
	q := mustNew(t, cfg, h)

	// 制造多个死信：failN 在 worker 启动前就配置好，避免运行中并发改 map。
	const deadN = 16
	h.mu.Lock()
	for i := 0; i < deadN; i++ {
		h.failN[fmt.Sprintf("d-%d", i)] = 100
	}
	h.mu.Unlock()
	for i := 0; i < deadN; i++ {
		id := fmt.Sprintf("d-%d", i)
		if err := q.Enqueue(id, "t", nil); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}
	for i := 0; i < deadN; i++ {
		waitState(t, q, fmt.Sprintf("d-%d", i), StateDeadLetter, 3*time.Second)
	}
	// 死信任务不再被 worker 执行，此时改其 failN 没有并发读取，安全。
	for i := 0; i < deadN; i++ {
		h.failN[fmt.Sprintf("d-%d", i)] = 0
	}

	// 阻塞一个在途任务，让收尾窗口足够长，以便在停机进行中并发重投。
	h.gate("anchor")
	if err := q.Enqueue("anchor", "t", nil); err != nil {
		t.Fatalf("enqueue anchor: %v", err)
	}
	waitStarted(t, h, "anchor")

	var wg sync.WaitGroup
	var accepted int64
	var closed int64
	var other int64
	// 一边发起限时停机。
	bounded, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_, _ = q.Close(bounded)

	// 停机已在进行：并发重投所有死信。
	for i := 0; i < deadN; i++ {
		id := fmt.Sprintf("d-%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch err := q.Requeue(id); {
			case err == nil:
				atomic.AddInt64(&accepted, 1)
			case errors.Is(err, ErrQueueClosed):
				atomic.AddInt64(&closed, 1)
			default:
				atomic.AddInt64(&other, 1)
				t.Errorf("unexpected requeue err during shutdown: %v", err)
			}
		}()
	}
	wg.Wait()
	t.Logf("requeue during shutdown: accepted=%d closed=%d", accepted, closed)

	closeAll(h, "anchor")
	s, err := q.Close(context.Background())
	if err != nil {
		t.Fatalf("final close: %v", err)
	}
	assertConserved(t, s)
	if accepted+closed != deadN || other != 0 {
		t.Fatalf("accepted=%d closed=%d other=%d, sum must be %d",
			accepted, closed, other, deadN)
	}
	// 每个死信的归宿：要么被成功重投并成功，要么留在死信；anchor 成功。
	if s.Accepted != deadN+1+int(accepted) {
		t.Fatalf("accepted=%d, want %d", s.Accepted, deadN+1+accepted)
	}
	if s.DeadLetter != deadN-int(accepted) {
		t.Fatalf("deadLetter=%d, want %d", s.DeadLetter, deadN-int(accepted))
	}
}

// TestRepeatedCloseSafe 多次、并发调用 Close（含已取消 ctx）不 panic、不卡死，
// 所有调用最终看到同一份守恒结果。
func TestRepeatedCloseSafe(t *testing.T) {
	h := newFlakyHandler()
	q := mustNew(t, Config{
		Capacity: 8, Workers: 2, MaxAttempts: 2,
		InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, BackoffMultiplier: 2,
	}, h)
	for i := 0; i < 6; i++ {
		if err := q.Enqueue(fmt.Sprintf("c-%d", i), "t", nil); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	var wg sync.WaitGroup
	const callers = 12
	finalStats := make([]Stats, callers)
	finalErr := make([]error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx := context.Background()
			if i%3 == 0 {
				c, cancel := context.WithCancel(context.Background())
				cancel() // 已取消 ctx：立即返回错误但收尾继续
				ctx = c
				defer cancel()
			}
			s, err := q.Close(ctx)
			finalStats[i] = s
			finalErr[i] = err
		}(i)
	}
	wg.Wait()

	var observed int
	for i := 0; i < callers; i++ {
		if finalErr[i] == nil {
			observed++
			assertConserved(t, finalStats[i])
		}
	}
	if observed == 0 {
		t.Fatalf("no Close caller observed completion")
	}
	// 再做一次确定的最终 Close，拿到权威结果。
	s, err := q.Close(context.Background())
	if err != nil {
		t.Fatalf("authoritative close: %v", err)
	}
	assertConserved(t, s)
	if s.Accepted != 6 {
		t.Fatalf("accepted=%d, want 6", s.Accepted)
	}
}
