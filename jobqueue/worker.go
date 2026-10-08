package jobqueue

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"
)

// runWorker 是单个消费者循环：从 ready 领取任务并执行。
// ready 关闭并排空后退出（range 结束）。
func (q *Queue) runWorker(id int, _ context.Context) {
	defer q.workersWG.Done()
	for t := range q.ready {
		q.execute(id, t)
	}
}

// execute 执行一次任务尝试，并依据结果推进状态机：
//
//	PENDING   -> RUNNING
//	RUNNING   -> SUCCEEDED | RETRY_WAIT | DEAD_LETTER | PENDING_RETRY
//
// panic 与普通 error 一样计为一次失败尝试。
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
	// 已被停机流程标记为终态（强制收尾可能在 Handler 返回前发生）：
	// 保持幂等防护；该任务的同组后继也由收尾统一处理，不在这里派发。
	if t.state.Terminal() {
		q.mu.Unlock()
		return
	}

	var handoff *Task
	switch {
	case err == nil:
		if q.transitionLocked(t, StateSucceeded, "handler succeeded") {
			handoff = q.afterTerminalLocked(t)
		}
	case errors.Is(err, context.Canceled) || errors.Is(err, ErrShuttingDown):
		// 停机导致的失败：交还给外部重投，保证至少一次。
		if q.transitionLocked(t, StatePendingRetry, "shutdown interrupted: "+err.Error()) {
			handoff = q.afterTerminalLocked(t)
		}
	case attempt >= t.maxAttempts:
		if q.transitionLocked(t, StateDeadLetter,
			fmt.Sprintf("max attempts=%d reached, last error: %s", t.maxAttempts, err.Error())) {
			handoff = q.afterTerminalLocked(t)
		}
	default:
		// 退避后重试。先迁移状态并由调度 goroutine 负责后续投递；同组后继
		// 必须等退避重试也结束（进入终态）后才派发，因此此处不 handoff。
		if q.transitionLocked(t, StateRetryWait, "attempt failed: "+err.Error()) {
			q.scheduleRetryLocked(t, attempt)
		}
	}
	q.mu.Unlock()

	// 锁外登记并派发同组下一个任务，避免在 q.mu 上阻塞发送。
	// registerDispatch 与 close(ready) 由 q.mu 互斥：停机已开始则放弃发送，
	// 后继任务由停机收尾统一转入 PENDING_RETRY。
	if handoff != nil && q.registerDispatch(handoff) {
		q.dispatchScheduled(handoff, "previous group task finished")
	}
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
// 调用方持有 q.mu；wakeWG.Add 也在此处完成（与 Close 的 wakeWG.Wait 由
// q.mu 互斥），goroutine 内部只负责等待与状态迁移。
//
// 退避期间 Close 会关闭 shutdown，等待立即结束并转入 PendingRetry。
func (q *Queue) scheduleRetryLocked(t *Task, attempt int) {
	delay := q.backoff(attempt)
	q.wakeWG.Add(1)
	q.pendingWakes++
	go func() {
		defer q.wakeWG.Done()
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
			q.mu.Lock()
			// 只有仍处于 RETRY_WAIT 才重新投递；状态机拒绝任何覆盖。
			// 该任务仍是同组 head（退避期间后继不派发），故只投递自身。
			canSend := q.transitionLocked(t, StatePending, "backoff elapsed, requeue")
			q.pendingWakes--
			q.mu.Unlock()
			// 解锁后再登记发送（与 close(ready) 互斥）；停机则放弃，任务由
			// 停机收尾转入 PENDING_RETRY（它现在已是 PENDING，收尾会处理）。
			if canSend && q.registerDispatch(t) {
				q.dispatchScheduled(t, "backoff elapsed")
			}
		case <-q.shutdown:
			q.mu.Lock()
			// head 退避被停机打断：转入 PENDING_RETRY；同组等待任务由收尾统一处理。
			q.transitionLocked(t, StatePendingRetry,
				fmt.Sprintf("shutdown interrupted backoff after attempt=%d", attempt))
			q.pendingWakes--
			q.mu.Unlock()
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
