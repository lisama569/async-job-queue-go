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
	defer q.mu.Unlock()
	if err != nil {
		t.lastErr = err.Error()
	}
	// 已被停机流程标记为终态（理论上 RUNNING 期间不会，因为只有 Close 兜底处理，
	// 而兜底在 workers 退出之后），保持幂等防护。
	if t.state.Terminal() {
		return
	}

	switch {
	case err == nil:
		q.transitionLocked(t, StateSucceeded, "handler succeeded")
		return
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, ErrShuttingDown) {
		// 停机导致的失败：交还给外部重投，保证至少一次。
		q.transitionLocked(t, StatePendingRetry, "shutdown interrupted: "+err.Error())
		return
	}

	if attempt >= t.maxAttempts {
		q.transitionLocked(t, StateDeadLetter,
			fmt.Sprintf("max attempts=%d reached, last error: %s", t.maxAttempts, err.Error()))
		return
	}

	// 退避后重试。先迁移状态并由调度 goroutine 负责后续投递。
	if q.transitionLocked(t, StateRetryWait, "attempt failed: "+err.Error()) {
		q.scheduleRetryLocked(t, attempt)
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
// 调用方持有 q.mu；goroutine 启动后自行加锁。
//
// 退避期间 Close 会关闭 shutdown，等待立即结束并转入 PendingRetry。
func (q *Queue) scheduleRetryLocked(t *Task, attempt int) {
	delay := q.backoff(attempt)
	q.wakeWG.Add(1)
	go func() {
		defer q.wakeWG.Done()
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
			q.mu.Lock()
			// 只有仍处于 RETRY_WAIT 才重新投递；状态机拒绝任何覆盖。
			if q.transitionLocked(t, StatePending, "backoff elapsed, requeue") {
				q.mu.Unlock()
				select {
				case q.ready <- t:
				case <-q.shutdown:
					// Close 已开始：让收尾兜底转 PendingRetry。
					q.mu.Lock()
					if !t.state.Terminal() {
						q.transitionLocked(t, StatePendingRetry, "shutdown during requeue")
					}
					q.mu.Unlock()
				}
			} else {
				q.mu.Unlock()
			}
		case <-q.shutdown:
			q.mu.Lock()
			if q.transitionLocked(t, StatePendingRetry,
				fmt.Sprintf("shutdown interrupted backoff after attempt=%d", attempt)) {
				_ = t.lastErr
			}
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
