package jobqueue

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"
)

// runWorker 是单个消费者循环：从 ready 领取任务并执行。
//
// 停机语义：收到 shutdown 广播后进入“排空”阶段——非阻塞地把已就绪的任务
// 执行完（尽量优雅收尾，保持业务键顺序），直到 ready 中暂时没有任务为止。
// 排空结束后仍残留在 ready / 业务键队列中的任务不会被静默丢弃，而由
// Queue.finalize 的最终兜底统一转入 PENDING_RETRY。
// ready 通道在全部 worker 与收尾完成后才关闭，因此任何发送都不会 panic。
func (q *Queue) runWorker(id int, _ context.Context) {
	defer q.workersWG.Done()
	for {
		select {
		case <-q.shutdown:
			q.drainReady(id)
			return
		case t := <-q.ready:
			q.execute(id, t)
		}
	}
}

// drainReady 在停机后排空当前已就绪的任务，使正常优雅停机（等待上限足够长）
// 能把排队中的任务也执行完，而不是一律退化为 PENDING_RETRY。
//
// 非阻塞：排空过程中新残留在 ready 里的任务由 finalize 兜底归账，
// 因此这里取空即返回，不会与“限时停机”死锁。
func (q *Queue) drainReady(workerID int) {
	for {
		select {
		case t := <-q.ready:
			q.execute(workerID, t)
		default:
			return
		}
	}
}

// execute 执行一次任务尝试，并依据结果推进状态机：
//
//	PENDING   -> RUNNING
//	RUNNING   -> SUCCEEDED | RETRY_WAIT | DEAD_LETTER | PENDING_RETRY
//
// panic 与普通 error 一样计为一次失败尝试。
// 任务进入终态时，若它持有某个业务键的头位置，同组下一个任务会被解锁下发。
func (q *Queue) execute(workerID int, t *Task) {
	q.mu.Lock()
	// 停机收尾可能已把任务转为终态（例如 PendingRetry），跳过它。
	if t.state.Terminal() {
		q.mu.Unlock()
		return
	}
	if !q.transitionLocked(t, StateRunning, fmt.Sprintf("picked by worker=%d", workerID)) {
		q.mu.Unlock()
		return
	}
	t.attempts++
	attempt := t.attempts
	q.mu.Unlock()

	// 执行业务，panic 被恢复为失败，保证消费者不会崩溃退出。
	err := q.safeHandle(t)

	q.mu.Lock()
	if err != nil {
		t.lastErr = err.Error()
	}
	// 幂等防护：理论上 RUNNING 期间状态不会被其他路径改写。
	if t.state.Terminal() {
		q.mu.Unlock()
		return
	}

	var dispatch *Task
	switch {
	case err == nil:
		if q.transitionLocked(t, StateSucceeded, "handler succeeded") {
			dispatch = q.releaseGroupLocked(t.Key)
		}
	case errors.Is(err, context.Canceled) || errors.Is(err, ErrShuttingDown) || q.closed:
		// 停机导致的失败，或失败时停机已经开始：交还给外部重投，保证至少一次。
		// 不再安排退避协程——finalize 可能正在等待 wakeWG，新建协程会造成
		// WaitGroup Add/Wait 并发误用。
		if q.transitionLocked(t, StatePendingRetry, "shutdown interrupted: "+errText(err)) {
			dispatch = q.releaseGroupLocked(t.Key)
		}
	case attempt >= t.maxAttempts:
		if q.transitionLocked(t, StateDeadLetter,
			fmt.Sprintf("max attempts=%d reached, last error: %s", t.maxAttempts, errText(err))) {
			dispatch = q.releaseGroupLocked(t.Key)
		}
	default:
		// 退避后重试。任务仍持有业务键的头位置（不退组），退避结束后重新投递。
		if q.transitionLocked(t, StateRetryWait, "attempt failed: "+errText(err)) && !q.closed {
			q.scheduleRetryLocked(t, attempt)
		}
	}
	q.mu.Unlock()

	// 锁外下发同组下一个任务（deliver 自带停机保护）。
	q.deliver(dispatch)
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// ErrShuttingDown 在停机时传给 Handler 的 context 中标记（保留语义说明）。
var ErrShuttingDown = errors.New("jobqueue: shutting down")

// safeHandle 调用 Handler 并恢复 panic。
func (q *Queue) safeHandle(t *Task) (err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// 停机时取消 ctx，使协作式 Handler 尽快结束。
	stopCancel := make(chan struct{})
	go func() {
		select {
		case <-q.shutdown:
			cancel()
		case <-stopCancel:
		}
	}()
	defer close(stopCancel)

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return q.handler.Handle(ctx, t)
}

// scheduleRetryLocked 在退避结束后把任务重新投递给 ready。
// 调用方持有 q.mu；goroutine 启动后自行加锁。
//
// 退避期间 Close 会关闭 shutdown，等待立即结束并转入 PendingRetry，
// 同时释放它占有的业务键头位置（残留同组任务交停机兜底处理）。
func (q *Queue) scheduleRetryLocked(t *Task, attempt int) {
	delay := q.backoff(attempt)
	q.wakeWG.Add(1)
	go func() {
		defer q.wakeWG.Done()
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-q.shutdown:
			q.mu.Lock()
			if !t.state.Terminal() {
				if q.transitionLocked(t, StatePendingRetry,
					fmt.Sprintf("shutdown interrupted backoff after attempt=%d", attempt)) {
					_ = q.releaseGroupLocked(t.Key)
				}
			}
			q.mu.Unlock()
		case <-timer.C:
			q.mu.Lock()
			// 只有仍处于 RETRY_WAIT 才重新投递；状态机拒绝任何覆盖。
			ok := q.transitionLocked(t, StatePending, "backoff elapsed, requeue")
			q.mu.Unlock()
			if !ok {
				return
			}
			// 仍是同组头任务（active 保持），直接重新投递。
			select {
			case q.ready <- t:
			case <-q.shutdown:
				// Close 已开始：转 PendingRetry 并释放组头位置。
				q.mu.Lock()
				if !t.state.Terminal() {
					if q.transitionLocked(t, StatePendingRetry, "shutdown during requeue") {
						_ = q.releaseGroupLocked(t.Key)
					}
				}
				q.mu.Unlock()
			}
		}
	}()
}

// backoff 计算第 attempt 次失败（attempt 从 1 开始）后的退避时长：
//
//	min(MaxBackoff, InitialBackoff * Multiplier^(attempt-1))
//
// 随失败次数单调不减；再叠加 [0,20%) 的随机抖动以避免重试风暴。
func (q *Queue) backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := float64(q.cfg.InitialBackoff)
	for i := 1; i < attempt; i++ {
		d *= q.cfg.BackoffMultiplier
		if float64(q.cfg.MaxBackoff) <= d {
			d = float64(q.cfg.MaxBackoff)
			break
		}
	}
	if d > float64(q.cfg.MaxBackoff) {
		d = float64(q.cfg.MaxBackoff)
	}
	jitter := 1.0 + rand.Float64()*0.2 // [1.0,1.2)
	d *= jitter
	if d > float64(q.cfg.MaxBackoff) {
		d = float64(q.cfg.MaxBackoff)
	}
	return time.Duration(d)
}
