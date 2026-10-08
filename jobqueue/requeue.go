package jobqueue

import "time"

// Requeue 把一个已进入 DEAD_LETTER 终态的任务重新放回队列，作为一次全新的
// 生命周期重跑：尝试次数清零、失败原因清空，按其业务键重新参与分组有序调度。
//
// 重投规则：
//   - 只有 DEAD_LETTER 状态的任务可重投；PENDING_RETRY 表示停机交还给外部的
//     任务，应直接重新 Enqueue；SUCCEEDED 任务不可重投，返回 ErrNotRequeueable；
//   - ID 不存在返回 ErrTaskNotFound；队列已关闭返回 ErrQueueClosed；
//     容量不足返回 ErrQueueFull；
//   - 任务正处于在途状态（PENDING/RUNNING/RETRY_WAIT）返回 ErrDuplicateTask：
//     它等价于“同一逻辑任务已有一轮在执行”。因此并发对同一 ID 重投（或与
//     Enqueue 竞争）至多一次成功，不会重复执行同一逻辑任务；
//   - 重投成功时旧生命周期的计数保持不变，新生命周期计入 Accepted（并累加
//     Requeued 统计），因此前后次数与结果互不混淆、总量守恒。
func (q *Queue) Requeue(id string) error {
	q.mu.Lock()
	switch {
	case q.closed:
		q.mu.Unlock()
		return ErrQueueClosed
	}
	t, ok := q.tasks[id]
	if !ok {
		q.mu.Unlock()
		return ErrTaskNotFound
	}
	if t.state != StateDeadLetter {
		state := t.state
		// 在途任务：去重拒绝（可能是并发重投/入队已让它进入新生命周期）。
		err := ErrNotRequeueable
		if !state.Terminal() {
			err = ErrDuplicateTask
		}
		// 日志必须在锁内：在途任务的字段正被 worker 并发更新。
		q.logTransition(t, state, state, "reject requeue: state="+state.String())
		q.mu.Unlock()
		return err
	}
	if q.counts.InFlight() >= q.cfg.Capacity {
		q.logTransition(t, StateDeadLetter, StateDeadLetter, "reject requeue: queue full")
		q.mu.Unlock()
		return ErrQueueFull
	}

	// 开启新生命周期：保留 ID/Type/Payload/Key，重置运行期字段。
	t.generation++
	t.requeued++
	t.attempts = 0
	t.lastErr = ""
	t.state = StatePending
	now := time.Now()
	t.enqueuedAt = now
	t.updatedAt = now
	q.counts.Requeued++
	q.counts.Accepted++
	q.counts.Pending++
	dispatch := q.admitLocked(t)
	key := t.Key
	cause := "requeued as new lifecycle"
	if key != "" {
		cause = "requeued as new lifecycle key=" + key
	}
	q.logTransition(t, StateDeadLetter, StatePending, cause)
	q.mu.Unlock()

	q.deliver(dispatch)
	return nil
}
