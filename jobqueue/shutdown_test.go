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

// stuckHandler 模拟无法协作取消的慢任务：忽略 ctx，在 release 关闭前一直阻塞。
// 用于确定触发“等待上限到达后的强制收尾”。
type stuckHandler struct {
	mu      sync.Mutex
	release map[string]chan struct{}
	started map[string]chan struct{}
	runs    map[string]int
}

func newStuckHandler() *stuckHandler {
	return &stuckHandler{
		release: map[string]chan struct{}{},
		started: map[string]chan struct{}{},
		runs:    map[string]int{},
	}
}

func (s *stuckHandler) gate(id string) chan<- struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.release[id] == nil {
		s.release[id] = make(chan struct{})
		s.started[id] = make(chan struct{})
	}
	return s.release[id]
}

func (s *stuckHandler) waitStart(t *testing.T, id string) {
	t.Helper()
	s.mu.Lock()
	c := s.started[id]
	s.mu.Unlock()
	select {
	case <-c:
	case <-time.After(3 * time.Second):
		t.Fatalf("task %s never started", id)
	}
}

func (s *stuckHandler) Handle(_ context.Context, t *Task) error {
	s.mu.Lock()
	s.runs[t.ID]++
	rel := s.release[t.ID]
	start := s.started[t.ID]
	s.mu.Unlock()
	if start != nil {
		select {
		case <-start:
		default:
			close(start)
		}
	}
	if rel != nil {
		<-rel // 忽略 ctx：模拟不可中断的慢调用
	}
	return nil
}

// TestBoundedShutdownForcesPendingRetry 等待上限到达后，仍在途的任务必须立即
// 落到 PENDING_RETRY，返回计数守恒，且 Close 不被慢任务卡死。
func TestBoundedShutdownForcesPendingRetry(t *testing.T) {
	h := newStuckHandler()
	cfg := Config{
		Capacity: 32, Workers: 4, MaxAttempts: 3,
		InitialBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond, BackoffMultiplier: 2,
	}
	q := mustNew(t, cfg, h)

	// 4 个不可中断的在途任务（忽略 ctx，模拟无法协作取消的慢 Handler）。
	for i := 0; i < 4; i++ {
		id := fmt.Sprintf("stuck-%d", i)
		h.gate(id)
		if err := q.Enqueue(id, "t", nil); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}
	for i := 0; i < 4; i++ {
		h.waitStart(t, fmt.Sprintf("stuck-%d", i))
	}
	// 8 个排队中、来不及执行的任务。
	for i := 0; i < 8; i++ {
		id := fmt.Sprintf("queued-%d", i)
		if err := q.Enqueue(id, "t", nil); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}

	// 只给 50ms：不可中断的慢任务绝不可能在该窗口结束。
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	start := time.Now()
	stats, err := q.Close(ctx)
	cancel()
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("bounded close blocked for %s", elapsed)
	}

	// 关键断言：返回时计数已守恒，在途为 0，所有未完成都可观测。
	if !stats.Conserved() || stats.InFlight() != 0 {
		t.Fatalf("timed-out close must return conserved zero-inflight stats, got %+v", stats)
	}
	if stats.Accepted != 12 {
		t.Fatalf("accepted=%d, want 12", stats.Accepted)
	}
	if stats.PendingRetry != 12 {
		t.Fatalf("pendingRetry=%d, want 12 (all unfinished forced to observable state)", stats.PendingRetry)
	}
	for i := 0; i < 4; i++ {
		if s, _ := q.TaskState(fmt.Sprintf("stuck-%d", i)); s != StatePendingRetry {
			t.Fatalf("stuck-%d state=%s, want PENDING_RETRY", i, s)
		}
	}
	t.Logf("bounded shutdown returned after %s with conserved stats: %+v", elapsed, stats)

	// 释放仍在执行的慢任务，让后台收尾 goroutine 与 worker 干净退出（避免泄漏）。
	// 它们晚到的成功结果不得改变已冻结计数（任务已是终态 PENDING_RETRY）。
	for i := 0; i < 4; i++ {
		close(h.gate(fmt.Sprintf("stuck-%d", i)))
	}
	final, ferr := q.Close(context.Background())
	if ferr != nil {
		t.Fatalf("drain close: %v", ferr)
	}
	if final != stats {
		t.Fatalf("late handler results must not change frozen counts:\nforced=%+v\nfinal=%+v", stats, final)
	}
}

// TestBoundedShutdownRecloseConsistent 超时强制收尾后，再次 Close 返回同一份冻结计数。
func TestBoundedShutdownRecloseConsistent(t *testing.T) {
	h := newStuckHandler()
	cfg := Config{
		Capacity: 8, Workers: 2, MaxAttempts: 2,
		InitialBackoff: time.Millisecond, MaxBackoff: 3 * time.Millisecond, BackoffMultiplier: 2,
	}
	q := mustNew(t, cfg, h)
	h.gate("slow") // 不可中断：释放前一直阻塞
	if err := q.Enqueue("slow", "t", nil); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	h.waitStart(t, "slow")

	ctx1, c1 := context.WithTimeout(context.Background(), 20*time.Millisecond)
	s1, err1 := q.Close(ctx1)
	c1()
	if !errors.Is(err1, context.DeadlineExceeded) {
		t.Fatalf("first close: %v", err1)
	}
	if !s1.Conserved() || s1.InFlight() != 0 {
		t.Fatalf("first stats not conserved: %+v", s1)
	}

	// 收尾已冻结：再次 Close（即便给已取消 ctx）都立即返回同一份计数。
	s2, err2 := q.Close(context.Background())
	if err2 != nil {
		t.Fatalf("second close err=%v, want nil", err2)
	}
	if s2 != s1 {
		t.Fatalf("stats drift between closes:\nfirst=%+v\nsecond=%+v", s1, s2)
	}

	ctx3, c3 := context.WithCancel(context.Background())
	c3()
	s3, err3 := q.Close(ctx3)
	if err3 != nil || s3 != s1 {
		t.Fatalf("third close: stats=%+v err=%v", s3, err3)
	}
	t.Logf("repeated closes return identical frozen stats: %+v", s1)

	// 释放慢任务，便于后台 worker 退出（不改变已冻结计数）。
	close(h.gate("slow"))
	if _, err := q.Close(context.Background()); err != nil {
		t.Fatalf("drain close: %v", err)
	}
}

// TestCloseWhileProducingConserved 边停机边并发投递：拒绝与接受数量守恒，无崩溃。
func TestCloseWhileProducingConserved(t *testing.T) {
	h := newFlakyHandler()
	cfg := Config{
		Capacity: 32, Workers: 8, MaxAttempts: 1,
		InitialBackoff: time.Millisecond, MaxBackoff: 3 * time.Millisecond, BackoffMultiplier: 2,
	}
	q := mustNew(t, cfg, h)

	const producers = 16
	var wg sync.WaitGroup
	var accepted, closedRej, fullRej int64
	stop := make(chan struct{})

	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				id := fmt.Sprintf("p%d-%d", p, i)
				i++
				err := q.Enqueue(id, "t", nil)
				switch {
				case err == nil:
					atomic.AddInt64(&accepted, 1)
				case errors.Is(err, ErrQueueClosed):
					atomic.AddInt64(&closedRej, 1)
				case errors.Is(err, ErrQueueFull):
					atomic.AddInt64(&fullRej, 1)
				default:
					t.Errorf("unexpected err: %v", err)
					return
				}
			}
		}(p)
	}

	// 让生产跑一小会后停机。
	time.Sleep(5 * time.Millisecond)
	stats, err := q.Close(context.Background())
	close(stop)
	wg.Wait()
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	if !stats.Conserved() || stats.InFlight() != 0 {
		t.Fatalf("stats not conserved: %+v", stats)
	}
	if stats.Accepted != int(atomic.LoadInt64(&accepted)) {
		t.Fatalf("accepted mismatch: stats=%d observed=%d (closedRej=%d fullRej=%d)",
			stats.Accepted, atomic.LoadInt64(&accepted),
			atomic.LoadInt64(&closedRej), atomic.LoadInt64(&fullRej))
	}
	if atomic.LoadInt64(&closedRej) == 0 {
		t.Fatalf("expected some ErrQueueClosed rejections during shutdown")
	}
	if stats.Accepted != stats.Succeeded+stats.PendingRetry+stats.DeadLetter {
		t.Fatalf("fate mismatch: %+v", stats)
	}
	t.Logf("produce-during-close: accepted=%d closedRej=%d fullRej=%d stats=%+v",
		atomic.LoadInt64(&accepted), atomic.LoadInt64(&closedRej),
		atomic.LoadInt64(&fullRej), stats)
}

// TestCloseConcurrentNoCrash 多个 goroutine 同时反复 Close / 投递 / 查询，
// 要求不 panic、不卡死、最终计数守恒。
func TestCloseConcurrentNoCrash(t *testing.T) {
	h := newFlakyHandler()
	// 在启动消费者前配置失败集合，避免与 Handle 并发读写 map。
	for i := 0; i < 8; i++ {
		h.failN[fmt.Sprintf("seed-%d", i)] = 1 // 部分会重试
	}
	cfg := Config{
		Capacity: 16, Workers: 4, MaxAttempts: 2,
		InitialBackoff: time.Millisecond, MaxBackoff: 4 * time.Millisecond, BackoffMultiplier: 2,
	}
	q := mustNew(t, cfg, h)
	for i := 0; i < 8; i++ {
		_ = q.Enqueue(fmt.Sprintf("seed-%d", i), "t", nil)
	}

	var wg sync.WaitGroup
	// 每个并发 Close 返回的计数都必须守恒（超时返回的是强制收尾后的冻结快照）。
	var badSnap int64
	// 多个并发 Close（混合短超时与后台 ctx）。
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1+i*5)*time.Millisecond)
			defer cancel()
			s, _ := q.Close(ctx)
			if !s.Conserved() || s.InFlight() != 0 {
				atomic.AddInt64(&badSnap, 1)
			}
		}(i)
	}
	// 并发投递（应全部被 ErrQueueClosed 拒绝或极少数在关闭前被接受）。
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = q.Enqueue(fmt.Sprintf("late-%d", i), "t", nil)
			_, _ = q.TaskState(fmt.Sprintf("late-%d", i))
			_ = q.Stats()
		}(i)
	}
	wg.Wait()

	if atomic.LoadInt64(&badSnap) != 0 {
		t.Fatalf("%d concurrent close snapshots were not conserved/zero-inflight", badSnap)
	}

	// 最后用长 ctx 拿到确定的最终计数：守恒、在途为 0。
	final, err := q.Close(context.Background())
	if err != nil {
		t.Fatalf("final close: %v", err)
	}
	if !final.Conserved() || final.InFlight() != 0 {
		t.Fatalf("final stats not conserved: %+v", final)
	}
	t.Logf("concurrent close final stats: %+v", final)
}

// TestBoundedShutdownGroupedConserved 限时停机下，分组有序任务同样全部落账、守恒。
func TestBoundedShutdownGroupedConserved(t *testing.T) {
	h := newStuckHandler()
	cfg := Config{
		Capacity: 64, Workers: 4, MaxAttempts: 1,
		InitialBackoff: time.Millisecond, MaxBackoff: 3 * time.Millisecond, BackoffMultiplier: 2,
	}
	q := mustNew(t, cfg, h)

	// 3 个组，每组 1 个不可中断 head + 若干排队后继。
	for g := 0; g < 3; g++ {
		key := fmt.Sprintf("G%d", g)
		head := fmt.Sprintf("%s-head", key)
		h.gate(head)
		if err := q.EnqueueKey(key, head, "t", nil); err != nil {
			t.Fatalf("enqueue %s: %v", head, err)
		}
		for j := 0; j < 4; j++ {
			id := fmt.Sprintf("%s-tail-%d", key, j)
			if err := q.EnqueueKey(key, id, "t", nil); err != nil {
				t.Fatalf("enqueue %s: %v", id, err)
			}
		}
	}
	for g := 0; g < 3; g++ {
		h.waitStart(t, fmt.Sprintf("G%d-head", g))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	stats, err := q.Close(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline, got %v", err)
	}
	if !stats.Conserved() || stats.InFlight() != 0 {
		t.Fatalf("grouped bounded shutdown not conserved: %+v", stats)
	}
	if stats.Accepted != 15 || stats.PendingRetry != 15 {
		t.Fatalf("want 15 accepted / 15 pendingRetry, got %+v", stats)
	}
	t.Logf("grouped bounded shutdown stats: %+v", stats)

	for g := 0; g < 3; g++ {
		close(h.gate(fmt.Sprintf("G%d-head", g)))
	}
	if _, err := q.Close(context.Background()); err != nil {
		t.Fatalf("drain close: %v", err)
	}
}

// TestRequeueWhileClosingNoLoss 并发重投、投递、停机同时发生：
// 不丢任务、不重复计数、不崩溃，最终计数守恒。
func TestRequeueWhileClosingNoLoss(t *testing.T) {
	h := newFlakyHandler()
	h.perLifecycle = false
	cfg := Config{
		Capacity: 64, Workers: 8, MaxAttempts: 1,
		InitialBackoff: time.Millisecond, MaxBackoff: 3 * time.Millisecond, BackoffMultiplier: 2,
	}
	q := mustNew(t, cfg, h)

	// 预置一批死信。
	const dl = 40
	for i := 0; i < dl; i++ {
		id := fmt.Sprintf("dl-%d", i)
		h.failN[id] = 1
		h.failEpoch1[id] = true // 重投后的 epoch=2 成功
		enqueueDeadLetter(t, q, id)
	}

	var wg sync.WaitGroup
	var requeued, requeueClosed, requeueFull, enqOK, enqClosed int64
	stop := make(chan struct{})

	// 多组 goroutine 并发重投死信。
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := w; i < dl; i += 8 {
				select {
				case <-stop:
					return
				default:
				}
				err := q.Requeue(fmt.Sprintf("dl-%d", i))
				switch {
				case err == nil:
					atomic.AddInt64(&requeued, 1)
				case errors.Is(err, ErrQueueClosed):
					atomic.AddInt64(&requeueClosed, 1)
				case errors.Is(err, ErrQueueFull):
					atomic.AddInt64(&requeueFull, 1)
				case errors.Is(err, ErrNotRequeueable):
					// 被其他 goroutine 抢先重投（变为在途），合法。
				default:
					t.Errorf("unexpected requeue err: %v", err)
					return
				}
			}
		}(w)
	}
	// 并发普通投递。
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				id := fmt.Sprintf("fresh-%d-%d", w, i)
				err := q.Enqueue(id, "t", nil)
				switch {
				case err == nil:
					atomic.AddInt64(&enqOK, 1)
				case errors.Is(err, ErrQueueClosed):
					atomic.AddInt64(&enqClosed, 1)
					return
				case errors.Is(err, ErrQueueFull):
					// 自旋重试直到关闭。
				default:
					t.Errorf("unexpected enqueue err: %v", err)
					return
				}
			}
		}(w)
	}

	time.Sleep(3 * time.Millisecond)
	stats, err := q.Close(context.Background())
	close(stop)
	wg.Wait()
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	if !stats.Conserved() || stats.InFlight() != 0 {
		t.Fatalf("stats not conserved: %+v", stats)
	}
	wantAccepted := int(atomic.LoadInt64(&requeued)) + dl + int(atomic.LoadInt64(&enqOK))
	if stats.Accepted != wantAccepted {
		t.Fatalf("accepted=%d want=%d (requeued=%d enqOK=%d)",
			stats.Accepted, wantAccepted, requeued, enqOK)
	}
	if stats.Requeued != int(atomic.LoadInt64(&requeued)) {
		t.Fatalf("stats.Requeued=%d observed=%d", stats.Requeued, requeued)
	}
	if stats.Done() != stats.Accepted {
		t.Fatalf("fate total=%d accepted=%d", stats.Done(), stats.Accepted)
	}
	// 每个死信 ID 至多重投一次。
	if got := int(atomic.LoadInt64(&requeued)); got > dl {
		t.Fatalf("requeued=%d > deadletters=%d (duplicate lifecycle)", got, dl)
	}
	t.Logf("requeue-while-closing: dl=%d requeued=%d requeueClosed=%d enqOK=%d enqClosed=%d stats=%+v",
		dl, requeued, requeueClosed, enqOK, enqClosed, stats)
}
