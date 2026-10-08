package jobqueue

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

func requeueCfg() Config {
	return Config{
		Capacity: 16, Workers: 4, MaxAttempts: 2,
		InitialBackoff: time.Millisecond, MaxBackoff: 4 * time.Millisecond, BackoffMultiplier: 2,
	}
}

// enqueueDeadLetter 投递一个必定失败的任务并等待它进入 DEAD_LETTER。
func enqueueDeadLetter(t *testing.T, q *Queue, id string) {
	t.Helper()
	if err := q.Enqueue(id, "t", nil); err != nil {
		t.Fatalf("enqueue %s: %v", id, err)
	}
	waitState(t, q, id, StateDeadLetter)
}

// TestRequeueFreshLifecycle 死信重投后作为全新生命周期：次数清零、可成功、统计不混算。
func TestRequeueFreshLifecycle(t *testing.T) {
	h := newFlakyHandler()
	q := mustNew(t, requeueCfg(), h)
	h.failN["dl-1"] = 100 // 前一个生命周期必然死信

	enqueueDeadLetter(t, q, "dl-1")
	if n, _ := q.TaskAttempts("dl-1"); n != 2 {
		t.Fatalf("pre-requeue attempts=%d, want 2", n)
	}

	// 重投后让新生命周期第一次就成功。
	h.failN["dl-1"] = 0
	if err := q.Requeue("dl-1"); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	waitState(t, q, "dl-1", StateSucceeded)

	stats, err := q.Close(context.Background())
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	assertConserved(t, stats)
	if stats.Accepted != 2 || stats.Enqueued != 1 || stats.Requeued != 1 {
		t.Fatalf("stats origin split wrong: %+v", stats)
	}
	if stats.DeadLetter != 1 || stats.Succeeded != 1 {
		t.Fatalf("want 1 deadletter(history)+1 success(new), got %+v", stats)
	}
	if n, _ := q.TaskAttempts("dl-1"); n != 1 {
		t.Fatalf("post-requeue attempts=%d, want 1 (fresh lifecycle)", n)
	}
	info, _ := q.TaskInfo("dl-1")
	if info.Origin != OriginRequeue || info.Epoch != 2 || info.State != StateSucceeded {
		t.Fatalf("current lifecycle info wrong: %+v", info)
	}
	if lerr, _ := q.TaskLastError("dl-1"); lerr != "" {
		t.Fatalf("fresh lifecycle lastErr should be empty, got %q", lerr)
	}
	hist := q.TaskHistory("dl-1")
	if len(hist) != 1 || hist[0].State != StateDeadLetter || hist[0].Epoch != 1 {
		t.Fatalf("history should retain dead-letter lifecycle, got %+v", hist)
	}
	t.Logf("requeued lifecycle: %+v; history: epoch=%d state=%s",
		info, hist[0].Epoch, hist[0].State)
}

// TestRequeueRejectReasons 覆盖重投的各类拒绝路径。
func TestRequeueRejectReasons(t *testing.T) {
	h := newFlakyHandler()
	cfg := Config{
		Capacity: 3, Workers: 3, MaxAttempts: 1,
		InitialBackoff: time.Millisecond, MaxBackoff: 3 * time.Millisecond, BackoffMultiplier: 2,
	}
	q := mustNew(t, cfg, h)
	h.failN["dl"] = 100
	h.failN["dl2"] = 100

	// 空 ID
	if err := q.Requeue(""); !errors.Is(err, ErrInvalidTask) {
		t.Fatalf("empty id: want ErrInvalidTask, got %v", err)
	}
	// 不存在
	if err := q.Requeue("ghost"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("missing: want ErrTaskNotFound, got %v", err)
	}

	enqueueDeadLetter(t, q, "dl")

	// 让重投的新生命周期阻塞在执行中（在途），此时不可再次重投。
	h.failN["dl"] = 0 // 重投的新生命周期应当成功（计数按生命周期独立）
	h.gate("dl")
	if err := q.Requeue("dl"); err != nil {
		t.Fatalf("first requeue: %v", err)
	}
	waitStarted(t, h, "dl")
	if err := q.Requeue("dl"); !errors.Is(err, ErrNotRequeueable) {
		t.Fatalf("in-flight requeued task: want ErrNotRequeueable, got %v", err)
	}

	// 再造一个死信，并用两个在途任务占满剩余容量（Capacity=3），验证满载拒绝。
	enqueueDeadLetter(t, q, "dl2")
	for _, id := range []string{"occupy-1", "occupy-2"} {
		h.gate(id)
		if err := q.Enqueue(id, "t", nil); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}
	for _, id := range []string{"occupy-1", "occupy-2"} {
		waitStarted(t, h, id)
	}
	if err := q.Requeue("dl2"); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("full: want ErrQueueFull, got %v", err)
	}
	if s, _ := q.TaskState("dl2"); s != StateDeadLetter {
		t.Fatalf("dl2 must remain DEAD_LETTER after capacity reject, got %s", s)
	}

	// 放行后成功，已成功任务同样不可重投。
	closeAll(h, "dl", "occupy-1", "occupy-2")
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s, _ := q.TaskState("dl"); s == StateSucceeded {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err := q.Requeue("dl"); !errors.Is(err, ErrNotRequeueable) {
		t.Fatalf("succeeded task: want ErrNotRequeueable, got %v", err)
	}
	stats, err := q.Close(context.Background())
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	assertConserved(t, stats)

	if err := q.Requeue("dl"); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("after close: want ErrQueueClosed, got %v", err)
	}

	// 另起队列验证“非死信（在途）”拒绝原因。
	q2 := mustNew(t, Config{
		Capacity: 4, Workers: 1, MaxAttempts: 1,
		InitialBackoff: time.Millisecond, MaxBackoff: 3 * time.Millisecond, BackoffMultiplier: 2,
	}, newFlakyHandler())
	h2 := q2.handler.(*flakyHandler)
	h2.gate("busy")
	if err := q2.Enqueue("busy", "t", nil); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	waitStarted(t, h2, "busy")
	if err := q2.Requeue("busy"); !errors.Is(err, ErrNotRequeueable) {
		t.Fatalf("in-flight task: want ErrNotRequeueable, got %v", err)
	}
	closeAll(h2, "busy")
	if s, err := q2.Close(context.Background()); err != nil || !s.Conserved() {
		t.Fatalf("close q2: %v %+v", err, s)
	}
}

// TestRequeueCapacityReject 容量满时重投返回 ErrQueueFull 且任务保持死信。
func TestRequeueCapacityReject(t *testing.T) {
	h := newFlakyHandler()
	cfg := Config{
		Capacity: 2, Workers: 2, MaxAttempts: 1,
		InitialBackoff: time.Millisecond, MaxBackoff: 3 * time.Millisecond, BackoffMultiplier: 2,
	}
	q := mustNew(t, cfg, h)
	h.failN["dl"] = 100
	enqueueDeadLetter(t, q, "dl")

	// 用两个在途任务占满容量（Capacity=2，2 个 worker 可同时阻塞）。
	for _, id := range []string{"occupy-1", "occupy-2"} {
		h.gate(id)
		if err := q.Enqueue(id, "t", nil); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}
	for _, id := range []string{"occupy-1", "occupy-2"} {
		waitStarted(t, h, id)
	}

	if err := q.Requeue("dl"); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("full: want ErrQueueFull, got %v", err)
	}
	if s, _ := q.TaskState("dl"); s != StateDeadLetter {
		t.Fatalf("dl state=%s, must remain DEAD_LETTER after capacity reject", s)
	}
	closeAll(h, "occupy-1", "occupy-2")
	stats, err := q.Close(context.Background())
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	assertConserved(t, stats)
	if stats.Requeued != 0 {
		t.Fatalf("requeued=%d, want 0 (reject must not count)", stats.Requeued)
	}
}

// TestRequeueDeadLettersFiltered 批量按条件重投，且每个 ID 只重投一次。
func TestRequeueDeadLettersFiltered(t *testing.T) {
	h := newFlakyHandler()
	q := mustNew(t, Config{
		Capacity: 64, Workers: 8, MaxAttempts: 1,
		InitialBackoff: time.Millisecond, MaxBackoff: 3 * time.Millisecond, BackoffMultiplier: 2,
	}, h)

	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("dl-%d", i)
		// 仅首个生命周期失败（MaxAttempts=1 → 死信）；重投后的 epoch=2 成功。
		h.failN[id] = 1
		h.failEpoch1[id] = true
		enqueueDeadLetter(t, q, id)
	}
	// 额外一个成功任务，不应被批量重投。
	if err := q.Enqueue("ok-1", "t", nil); err != nil {
		t.Fatalf("enqueue ok: %v", err)
	}
	waitState(t, q, "ok-1", StateSucceeded)

	// 只重投偶数编号。
	requeued, err := q.RequeueDeadLetters(func(info TaskInfo) bool {
		return strings.HasSuffix(info.ID, "0") || strings.HasSuffix(info.ID, "2") ||
			strings.HasSuffix(info.ID, "4") || strings.HasSuffix(info.ID, "6") ||
			strings.HasSuffix(info.ID, "8")
	})
	if err != nil {
		t.Fatalf("batch requeue: %v", err)
	}
	if len(requeued) != 5 {
		t.Fatalf("requeued=%d, want 5", len(requeued))
	}
	// 等重投的新生命周期全部成功（它们不再是死信）。
	for _, id := range requeued {
		waitState(t, q, id, StateSucceeded)
	}

	// 再次批量重投：10 个旧 ID 现在是 5 成功 + 5 死信；match=nil 会命中剩余 5 个
	// 从未重投的奇数死信。为验证“同一 ID 不重复重投”，这里只允许偶数（已重投）。
	evenOnly := func(info TaskInfo) bool {
		return strings.HasSuffix(info.ID, "0") || strings.HasSuffix(info.ID, "2") ||
			strings.HasSuffix(info.ID, "4") || strings.HasSuffix(info.ID, "6") ||
			strings.HasSuffix(info.ID, "8")
	}
	again, err := q.RequeueDeadLetters(evenOnly)
	if err != nil {
		t.Fatalf("second batch: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("same IDs requeued twice: %v", again)
	}

	// match=nil 仍能重投剩余 5 个奇数死信。
	rest, err := q.RequeueDeadLetters(nil)
	if err != nil || len(rest) != 5 {
		t.Fatalf("remaining odd deadletters: ids=%v err=%v, want 5", rest, err)
	}
	for _, id := range rest {
		waitState(t, q, id, StateSucceeded)
	}

	stats, err := q.Close(context.Background())
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	assertConserved(t, stats)
	if stats.Requeued != 10 || stats.DeadLetter != 10 {
		t.Fatalf("want 10 requeued / 10 dead(history), got %+v", stats)
	}
	if stats.Succeeded != 1+10 { // ok-1 + 10 重投成功
		t.Fatalf("succeeded=%d, want 11", stats.Succeeded)
	}
	t.Logf("batch requeue stats: %+v", stats)
}

// TestRequeueGroupedOrdering 重投带业务键的死信时，仍遵守分组有序。
func TestRequeueGroupedOrdering(t *testing.T) {
	h := newOrderedHandler()
	cfg := Config{
		Capacity: 16, Workers: 4, MaxAttempts: 1,
		InitialBackoff: time.Millisecond, MaxBackoff: 3 * time.Millisecond, BackoffMultiplier: 2,
	}
	q := mustNew(t, cfg, h)

	const key = "ord-7"
	dlID := key + "-dl"
	// 第一个生命周期：仅 dlID 失败一次（MaxAttempts=1 → 死信），其重投及其他任务成功。
	ctrl := &failEpochOneHandler{wrapped: h, failID: dlID}
	q.handler = ctrl
	if err := q.EnqueueKey(key, dlID, "t", nil); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	waitState(t, q, dlID, StateDeadLetter)

	// 入队一个同组后继；再重投死信。死信必须排在该 key 其他在途之后。
	next := key + "-next"
	h.gate(next)
	if err := q.EnqueueKey(key, next, "t", nil); err != nil {
		t.Fatalf("enqueue next: %v", err)
	}
	h.waitStart(t, next)

	if err := q.Requeue(dlID); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	// next 仍在执行（阻塞），重投的 dl 不得开始。
	time.Sleep(20 * time.Millisecond)
	if ord := h.orderOf(key); len(ord) != 1 || ord[0] != next {
		t.Fatalf("requeued task must wait behind in-flight group task, got %v", ord)
	}
	close(h.gate(next))

	// 等待 handoff：重投的 dl 开始执行后再停机，避免停机/派发竞态。
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
	ord := h.orderOf(key)
	if len(ord) != 2 || ord[0] != next || ord[1] != dlID {
		t.Fatalf("group order with requeue: %v", ord)
	}
	info, _ := q.TaskInfo(dlID)
	if info.Origin != OriginRequeue || info.Epoch != 2 {
		t.Fatalf("requeued info: %+v", info)
	}
}

// failEpochOneHandler：failID 的 epoch==1 返回错误，其余委托 wrapped（成功）。
type failEpochOneHandler struct {
	wrapped Handler
	failID  string
}

func (f *failEpochOneHandler) Handle(ctx context.Context, t *Task) error {
	if t.ID == f.failID && t.epoch == 1 {
		return fmt.Errorf("fail first lifecycle of %s", t.ID)
	}
	return f.wrapped.Handle(ctx, t)
}

// TestRequeueConcurrentNoDuplicate 并发重投同一死信，至多一个成功、只执行一次新生命周期。
func TestRequeueConcurrentNoDuplicate(t *testing.T) {
	h := newFlakyHandler()
	q := mustNew(t, Config{
		Capacity: 64, Workers: 4, MaxAttempts: 1,
		InitialBackoff: time.Millisecond, MaxBackoff: 3 * time.Millisecond, BackoffMultiplier: 2,
	}, h)
	h.failN["dl-x"] = 100
	enqueueDeadLetter(t, q, "dl-x")
	h.failN["dl-x"] = 0

	const n = 32
	var wg sync.WaitGroup
	var ok, rejected int64
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := q.Requeue("dl-x")
			switch {
			case err == nil:
				atomic.AddInt64(&ok, 1)
			case errors.Is(err, ErrNotRequeueable):
				atomic.AddInt64(&rejected, 1)
			default:
				t.Errorf("unexpected requeue err: %v", err)
			}
		}()
	}
	wg.Wait()
	if ok != 1 || rejected != n-1 {
		t.Fatalf("requeue accepted=%d rejected=%d, want 1/%d", ok, rejected, n-1)
	}
	waitState(t, q, "dl-x", StateSucceeded)

	stats, err := q.Close(context.Background())
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	assertConserved(t, stats)
	if h.runsOf("dl-x") != 2 { // 死信生命周期 1 次（MaxAttempts=1）+ 重投 1 次
		t.Fatalf("handler ran %d times total, want 2", h.runsOf("dl-x"))
	}
	if stats.Requeued != 1 {
		t.Fatalf("requeued=%d, want 1", stats.Requeued)
	}
}
