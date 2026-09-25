package queue

import (
	"context"
	"time"
)

// Status 表示任务在其生命周期中所处的状态。
type Status int

const (
	// StatusPending 已入队，等待被消费者拾取。
	StatusPending Status = iota
	// StatusRunning 已被某个消费者拾取，正在执行。
	StatusRunning
	// StatusRetrying 本次执行失败，正在退避等待下一次尝试。
	StatusRetrying
	// StatusSucceeded 执行成功，终态。
	StatusSucceeded
	// StatusDiscarded 达到最大尝试次数后被丢弃，终态。
	StatusDiscarded
)

func (s Status) String() string { return statusName(s) }

func statusName(s Status) string {
	switch s {
	case StatusPending:
		return "PENDING"
	case StatusRunning:
		return "RUNNING"
	case StatusRetrying:
		return "RETRYING"
	case StatusSucceeded:
		return "SUCCEEDED"
	case StatusDiscarded:
		return "DISCARDED"
	default:
		return "UNKNOWN"
	}
}

// Job 是提交给队列的一个逻辑任务。
type Job struct {
	// Key 为业务去重键。同一 Key 在未到达终态前重复提交会被拒绝。
	Key string
	// Payload 为任务负载。
	Payload []byte
}

// Handler 处理单个任务，返回 error 表示本次执行失败。
type Handler func(ctx context.Context, payload []byte) error

// JobResult 用于描述一个任务的最终结局。
type JobResult struct {
	Key       string
	Status    Status
	Attempts  int
	LastError string
}

// entry 是队列内部登记的任务运行记录。
type entry struct {
	key      string
	job      Job
	status   Status
	attempts int
	lastErr  error
	enqAt    time.Time
}

func (e *entry) snapshot() JobResult {
	lastErr := ""
	if e.lastErr != nil {
		lastErr = e.lastErr.Error()
	}
	return JobResult{
		Key:       e.key,
		Status:    e.status,
		Attempts:  e.attempts,
		LastError: lastErr,
	}
}
