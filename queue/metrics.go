package queue

import "sync/atomic"

// Metrics 为队列运行期的可观测计数器。
type Metrics struct {
	enqueued   atomic.Int64 // 成功入队的任务数（去重后的唯一任务）
	rejected   atomic.Int64 // 因关闭或容量不足被拒绝的提交数
	succeeded  atomic.Int64 // 到达成功终态的任务数
	discarded  atomic.Int64 // 到达丢弃终态的任务数
	duplicates atomic.Int64 // 因去重键冲突被拒绝的提交数
	retried    atomic.Int64 // 进入退避重试的次数（每次重试 +1）
}

// Snapshot 为某一时刻计数器的值拷贝。
type Snapshot struct {
	Enqueued   int64
	Rejected   int64
	Succeeded  int64
	Discarded  int64
	Duplicates int64
	Retried    int64
}

func (m *Metrics) snapshot() Snapshot {
	return Snapshot{
		Enqueued:   m.enqueued.Load(),
		Rejected:   m.rejected.Load(),
		Succeeded:  m.succeeded.Load(),
		Discarded:  m.discarded.Load(),
		Duplicates: m.duplicates.Load(),
		Retried:    m.retried.Load(),
	}
}

func (m *Metrics) incEnqueued()   { m.enqueued.Add(1) }
func (m *Metrics) incRejected()   { m.rejected.Add(1) }
func (m *Metrics) incSucceeded()  { m.succeeded.Add(1) }
func (m *Metrics) incDiscarded()  { m.discarded.Add(1) }
func (m *Metrics) incDuplicates() { m.duplicates.Add(1) }
func (m *Metrics) incRetried()    { m.retried.Add(1) }
