package jobqueue

import (
	"fmt"
	"time"
)

// Requeue 把一个已进入 DEAD_LETTER 终态的任务重新放回队列，开启一次全新的
// 生命周期（尝试次数、上次错误、结果统计均重新开始，不与历史生命周期混算）。
//
// 顺序：同一业务键上，重投任务作为该键的新成员，排在该键其他在途任务之后。
//
// 拒绝原因可用 errors.Is 区分：
//   - ErrInvalidTask：id 为空；
//   - ErrQueueClosed：队列已关闭；
//   - ErrTaskNotFound：该 ID 在队列中没有任务记录；
//   - ErrNotRequeueable：任务存在但当前不是 DEAD_LETTER（仍在途、已成功或停机交还）；
//   - ErrQueueFull：未完成任务数已达 Capacity。
func (q *Queue) Requeue(id string) error {
	if id == "" {
		return ErrInvalidTask
	}
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return ErrQueueClosed
	}
	old, ok := q.tasks[id]
	if !ok {
		q.mu.Unlock()
		return ErrTaskNotFound
	}
	if old.state != StateDeadLetter {
		from := old.state
		q.mu.Unlock()
		return fmt.Errorf("%w: current state=%s", ErrNotRequeueable, from)
	}
	if q.counts.InFlight() >= q.cfg.Capacity {
		q.mu.Unlock()
		return ErrQueueFull
	}
	t, deliverNow := q.respawnLocked(old)
	q.acceptLocked(t, OriginRequeue)
	q.logTransition(t, StatePending, StatePending,
		"requeued from dead letter, previous epoch finished with "+old.lastErr)
	if deliverNow {
		q.deliverLocked(t)
	}
	q.mu.Unlock()

	if deliverNow {
		q.dispatchScheduled(t, "requeued")
	}
	return nil
}

// respawnLocked 基于一个 DEAD_LETTER 任务构造一次全新生命周期并决定派发方式。
// 返回新任务与“是否应立即派发”。调用方必须持有 q.mu，且已通过容量/状态检查。
func (q *Queue) respawnLocked(old *Task) (t *Task, deliverNow bool) {
	now := time.Now()
	t = &Task{
		ID:          old.ID,
		Type:        old.Type,
		Payload:     old.Payload,
		key:         old.key,
		origin:      OriginRequeue,
		epoch:       old.epoch + 1,
		maxAttempts: q.cfg.MaxAttempts,
		state:       StatePending,
		enqueuedAt:  now,
		updatedAt:   now,
	}
	if old.key == "" {
		return t, true
	}
	g := q.groups[old.key]
	if g == nil {
		// 旧组已在 head 终态时清空，重投作为新组的 head。
		g = &keyGroup{active: true}
		q.groups[old.key] = g
		return t, true
	}
	// 理论上旧死信作为 head 终态时其分组要么被删除、要么仍有排队后继；
	// 若组仍活跃（说明后继在途），重投任务排到队尾保序。
	if g.active {
		g.waiting = append(g.waiting, t)
		return t, false
	}
	g.active = true
	return t, true
}

// RequeueDeadLetters 按条件批量重投死信任务。
//
// match 对每个当前处于 DEAD_LETTER 的任务快照做判断；返回 true 的会被重投。
// match 为 nil 时重投全部死信。返回本次成功重投的任务 ID 列表。
// 若因容量不足提前停止，返回已重投 ID 与 ErrQueueFull（errors.Is 可识别）；
// 队列已关闭返回 ErrQueueClosed；id 列表为空且无错误表示没有可重投任务。
//
// 每个 ID 至多重投一次：已经在途的任务不会因本次调用重复执行。
func (q *Queue) RequeueDeadLetters(match func(TaskInfo) bool) ([]string, error) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil, ErrQueueClosed
	}
	// 收集候选快照（当前最新生命周期处于 DEAD_LETTER）。match 在锁外执行，
	// 避免用户回调与队列内部锁耦合。
	var candidates []*Task
	for _, t := range q.tasks {
		if t.state == StateDeadLetter && (match == nil || match(t.infoLocked())) {
			candidates = append(candidates, t)
		}
	}
	q.mu.Unlock()

	requeued := make([]string, 0, len(candidates))
	var scheduled []*Task
	var stopped error
	for _, old := range candidates {
		q.mu.Lock()
		// 重新校验：并发的 Requeue/Enqueue/停机可能已改变状态。
		cur, ok := q.tasks[old.ID]
		switch {
		case q.closed:
			q.mu.Unlock()
			stopped = ErrQueueClosed
		case !ok || cur != old || cur.state != StateDeadLetter:
			// 已被别的路径处理（重投/覆盖），跳过，避免重复执行。
			q.mu.Unlock()
			continue
		case q.counts.InFlight() >= q.cfg.Capacity:
			q.mu.Unlock()
			stopped = ErrQueueFull
		default:
			t, deliverNow := q.respawnLocked(old)
			q.acceptLocked(t, OriginRequeue)
			requeued = append(requeued, t.ID)
			if deliverNow {
				q.deliverLocked(t)
				scheduled = append(scheduled, t)
			}
			q.logTransition(t, StatePending, StatePending,
				"requeued in batch from dead letter")
			q.mu.Unlock()
		}
		if stopped != nil {
			break
		}
	}
	for _, t := range scheduled {
		q.dispatchScheduled(t, "batch requeued")
	}
	return requeued, stopped
}

// TaskInfo 返回指定 ID 最新生命周期任务的快照；不存在时返回 false。
func (q *Queue) TaskInfo(id string) (TaskInfo, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	t, ok := q.tasks[id]
	if !ok {
		return TaskInfo{}, false
	}
	return t.infoLocked(), true
}

// TaskHistory 返回指定 ID 的历史生命周期快照（不含当前最新的），旧 → 新 排序。
func (q *Queue) TaskHistory(id string) []TaskInfo {
	q.mu.Lock()
	defer q.mu.Unlock()
	old := q.history[id]
	if len(old) == 0 {
		return nil
	}
	out := make([]TaskInfo, len(old))
	for i, t := range old {
		out[i] = t.infoLocked()
	}
	return out
}
