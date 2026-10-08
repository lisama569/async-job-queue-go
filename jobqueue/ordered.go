package jobqueue

import "time"

// keyGroup 保存一个业务分组键的有序消费状态。
//
// 不变量（由 Queue.mu 保护）：
//   - 一个 key 至多有一个任务在途（已派发到 ready / RUNNING / RETRY_WAIT）；
//   - waiting 严格按提交先后排列，head 进入终态后按序派发下一个；
//   - 不同 key 之间互不阻塞，可跨组并发。
type keyGroup struct {
	// waiting 是尚未派发的任务队列；waiting[0] 是下一个应派发的任务。
	waiting []*Task
	// active 表示该组已有一个任务在途。
	active bool
}

// EnqueueKey 提交一个带业务分组键的任务。
//
// 顺序语义：同一 key 的任务严格按提交先后被消费——前一个任务未进入终态
// （含失败退避重试期间）不会派发同组下一个任务；不同 key 之间仍并发执行。
// 空 key 等价于 Enqueue（不分组，走全局并发池）。
//
// 拒绝原因与 Enqueue 相同，可用 errors.Is 区分：
//   - ErrInvalidTask：ID 为空；
//   - ErrQueueClosed：队列已关闭；
//   - ErrQueueFull：未完成任务数已达 Capacity；
//   - ErrDuplicateTask：相同 ID 的任务仍未达终态（去重）。
func (q *Queue) EnqueueKey(key, id, taskType string, payload []byte) error {
	if id == "" {
		return ErrInvalidTask
	}
	now := time.Now()
	t := &Task{
		ID:          id,
		Type:        taskType,
		Payload:     payload,
		key:         key,
		origin:      OriginEnqueue,
		epoch:       1,
		maxAttempts: q.cfg.MaxAttempts,
		state:       StatePending,
		enqueuedAt:  now,
		updatedAt:   now,
	}

	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return ErrQueueClosed
	}
	if existing, ok := q.tasks[id]; ok && !existing.state.Terminal() {
		existingState := existing.state.String()
		q.mu.Unlock()
		q.logTransition(t, t.state, t.state, "reject duplicate: existing="+existingState)
		return ErrDuplicateTask
	}
	if q.counts.InFlight() >= q.cfg.Capacity {
		q.mu.Unlock()
		q.logTransition(t, StatePending, StatePending, "reject: queue full")
		return ErrQueueFull
	}

	// 决定该任务是立即派发还是在组内排队。
	var next *Task
	if key == "" {
		next = t
	} else {
		g := q.groups[key]
		if g == nil {
			g = &keyGroup{}
			q.groups[key] = g
		}
		if g.active {
			g.waiting = append(g.waiting, t)
		} else {
			g.active = true
			next = t
		}
	}
	q.acceptLocked(t, OriginEnqueue)
	q.logTransition(t, StatePending, StatePending, "enqueued") // 锁内记录：任务可能已被 worker 领取
	if next != nil {
		q.deliverLocked(t)
	}
	q.mu.Unlock()

	if next != nil {
		q.dispatchScheduled(t, "enqueued")
	}
	return nil
}

// acceptLocked 登记一个新接受的生命周期：归档旧记录、写入 tasks、维护计数。
// 调用方必须持有 q.mu。
func (q *Queue) acceptLocked(t *Task, origin TaskOrigin) {
	if old, ok := q.tasks[t.ID]; ok {
		q.history[t.ID] = append(q.history[t.ID], old)
	}
	q.tasks[t.ID] = t
	q.counts.Accepted++
	q.counts.Pending++
	if origin == OriginRequeue {
		q.counts.Requeued++
	} else {
		q.counts.Enqueued++
	}
}

// afterTerminalLocked 在组内 head 任务进入终态后挑选同组下一个任务。
// 返回 nil 表示无后继需要在锁外派发。调用方必须持有 q.mu。
//
// 即使停机已经开始（q.closed），只要 ready 尚未关闭、worker 仍在排空，
// head 完成后仍应把同组后继 handoff 出去（与无分组任务排空语义一致）；
// 若 ready 已关闭，deliver 会把任务安全转为 PENDING_RETRY，不会丢失。
func (q *Queue) afterTerminalLocked(t *Task) *Task {
	if t.key == "" {
		return nil
	}
	g := q.groups[t.key]
	if g == nil {
		return nil
	}
	if len(g.waiting) == 0 {
		// 组内已无等待任务：删除空分组，active 复位。
		delete(q.groups, t.key)
		g.active = false
		return nil
	}
	next := g.waiting[0]
	g.waiting[0] = nil
	g.waiting = g.waiting[1:]
	return next // g.active 保持 true，next 成为新的 head
}

// deliverLocked 在锁内登记一个 ready 发送；调用方在解锁后调用
// dispatchScheduled 完成发送。Add 与 Close 的关闭动作由 q.mu 串行化，
// 杜绝“Wait 归零后又 Add”以及“向已关闭 channel 发送”。
//
// 调用方必须持有 q.mu 且已确认 q.closed == false（确定要派发）。
func (q *Queue) deliverLocked(t *Task) {
	q.dispatching.Add(1)
}

// registerDispatch 在解锁后登记一个待发送任务（execute handoff 等场景：
// 挑选后继时持锁、实际发送在解锁后）。返回 false 表示停机已开始，不应再发送，
// 任务由停机收尾统一处理。
func (q *Queue) registerDispatch(t *Task) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false
	}
	q.dispatching.Add(1)
	return true
}

// dispatchScheduled 在锁外完成由 deliverLocked 登记的发送。
func (q *Queue) dispatchScheduled(t *Task, cause string) {
	defer q.dispatching.Done()
	// 关闭 ready 前会先持锁等待 dispatching 归零；能走到这里说明登记时未停机。
	// 但停机可能在解锁后立刻发生（close(shutdown) 先于 close(ready)），
	// 因此发送仍需在 shutdown 时改走 PENDING_RETRY；此时 ready 尚未关闭
	// （Close 正阻塞在 dispatching.Wait 上），两个分支都安全。
	select {
	case q.ready <- t:
	case <-q.shutdown:
		q.mu.Lock()
		if !t.state.Terminal() {
			q.transitionLocked(t, StatePendingRetry, "shutdown during dispatch: "+cause)
		}
		q.mu.Unlock()
	}
}
