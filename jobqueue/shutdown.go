package jobqueue

// forceFinalize 在停机等待上限到达后，把所有仍未达终态的任务强制落到
// PENDING_RETRY（明确、可观测，不静默丢弃、不凭空消失），并冻结最终计数。
//
// 该操作幂等：优雅收尾路径与超时路径共用，由 finalizeOnce 保证只执行一次。
// 执行后关闭 finalized，后续所有 Close 都返回同一份 finalStats。
//
// 并发安全：被强制收尾时，仍在执行的 worker、退避 goroutine 之后的状态迁移
// 会被状态机（终态不可迁出）拒绝并打印 ILLEGAL 日志，计数不会被覆盖。
func (q *Queue) forceFinalize(cause string) {
	q.finalizeOnce.Do(func() {
		q.mu.Lock()
		for _, t := range q.tasks {
			if !t.state.Terminal() {
				q.transitionLocked(t, StatePendingRetry, cause)
			}
		}
		q.finalStats = q.counts
		q.mu.Unlock()
		close(q.finalized)
	})
}

// finalSnapshot 返回冻结的最终计数；若尚未收尾返回当前快照与 false。
func (q *Queue) finalSnapshot() (Stats, bool) {
	select {
	case <-q.finalized:
		q.mu.Lock()
		s := q.finalStats
		q.mu.Unlock()
		return s, true
	default:
		return Stats{}, false
	}
}
