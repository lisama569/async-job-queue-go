package queue

import "errors"

// 入队被拒绝时返回的哨兵错误，拒绝原因可区分。
var (
	// ErrQueueClosed 队列已关闭，不再接受新任务。
	ErrQueueClosed = errors.New("queue: queue is closed")
	// ErrQueueFull 队列容量不足，任务无法入队。
	ErrQueueFull = errors.New("queue: queue is full")
	// ErrDuplicate 相同去重键的任务尚在生命周期中，重复提交被拒绝。
	ErrDuplicate = errors.New("queue: duplicate job key is active")
)
