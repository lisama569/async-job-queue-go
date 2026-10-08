package jobqueue

import (
	"context"
	"errors"
	"time"
)

// TaskState 表示任务在其生命周期中的状态。
type TaskState int

const (
	StatePending    TaskState = iota // 已入队，等待调度
	StateRunning                     // 已被消费者领取，正在执行
	StateRetryWait                   // 执行失败，退避等待重试
	StateSucceeded                   // 终态：成功完成
	StateDeadLetter                  // 终态：达到最大尝试次数后丢弃
	// StatePendingRetry 终态（相对本队列）：优雅停机时尚未完成，
	// 已交还给外部，等待重新入队/持久化后再次投递。保证至少一次。
	StatePendingRetry
)

// String 返回状态名，便于日志与测试输出。
func (s TaskState) String() string {
	switch s {
	case StatePending:
		return "PENDING"
	case StateRunning:
		return "RUNNING"
	case StateRetryWait:
		return "RETRY_WAIT"
	case StateSucceeded:
		return "SUCCEEDED"
	case StateDeadLetter:
		return "DEAD_LETTER"
	case StatePendingRetry:
		return "PENDING_RETRY"
	default:
		return "UNKNOWN"
	}
}

// Terminal 报告状态是否为本队列视角下的终态。
//
// 三种终态分别对应优雅停机后任务的三种归宿：
// 已完成（Succeeded）、待重试（PendingRetry）、已丢弃（DeadLetter）。
func (s TaskState) Terminal() bool {
	return s == StateSucceeded || s == StateDeadLetter || s == StatePendingRetry
}

var (
	// ErrQueueClosed 队列已关闭，拒绝新的入队请求。
	ErrQueueClosed = errors.New("jobqueue: queue is closed")
	// ErrQueueFull 队列容量不足，拒绝入队。
	ErrQueueFull = errors.New("jobqueue: queue is full")
	// ErrDuplicateTask 同一逻辑任务（相同 ID）已在队列中，去重拒绝。
	ErrDuplicateTask = errors.New("jobqueue: duplicate task")
	// ErrInvalidTask 入队参数非法。
	ErrInvalidTask = errors.New("jobqueue: invalid task")
	// ErrTaskNotFound 指定 ID 的任务在队列中没有记录。
	ErrTaskNotFound = errors.New("jobqueue: task not found")
	// ErrNotRequeueable 任务当前状态不允许重投（只有 DEAD_LETTER 可重投）。
	ErrNotRequeueable = errors.New("jobqueue: task is not requeueable")
)

// 下列辅助让调用方可以区分不同的拒绝原因。

func IsQueueClosed(err error) bool { return errors.Is(err, ErrQueueClosed) }
func IsQueueFull(err error) bool   { return errors.Is(err, ErrQueueFull) }
func IsDuplicate(err error) bool   { return errors.Is(err, ErrDuplicateTask) }

// Handler 执行单个任务的业务逻辑。返回 error 表示本次尝试失败。
// 实现应通过 ctx 感知停机；panic 会被消费者恢复并计为一次失败。
type Handler interface {
	Handle(ctx context.Context, task *Task) error
}

// HandlerFunc 让普通函数满足 Handler。
type HandlerFunc func(ctx context.Context, task *Task) error

func (f HandlerFunc) Handle(ctx context.Context, task *Task) error { return f(ctx, task) }

// Task 是队列调度的最小单元。ID 相同即视为同一逻辑任务，用于去重。
type Task struct {
	ID      string
	Type    string
	Payload []byte

	// key 是业务分组键（顺序消费）；空串表示不分组，走全局并发池。
	key string
	// origin 标记本次生命周期的来源：入队 / 终态重投。
	origin TaskOrigin
	// epoch 是同一 ID 的第几次生命周期（1 起），每次重投 +1。
	epoch int

	// maxAttempts 在入队时由队列按配置写入，>0。
	maxAttempts int

	// 下列字段仅在 Queue.mu 保护下更新（见 Queue 字段注释）。
	attempts   int
	state      TaskState
	lastErr    string
	enqueuedAt time.Time
	updatedAt  time.Time
}

// TaskOrigin 标记任务本次生命周期的来源。
type TaskOrigin int

const (
	// OriginEnqueue 普通入队。
	OriginEnqueue TaskOrigin = iota
	// OriginRequeue 终态（DEAD_LETTER）任务重投。
	OriginRequeue
)

func (o TaskOrigin) String() string {
	switch o {
	case OriginRequeue:
		return "REQUEUE"
	default:
		return "ENQUEUE"
	}
}

// Key 返回任务所属的业务分组键；空串表示未分组。
func (t *Task) Key() string { return t.key }

// Origin 返回本次生命周期的来源。
func (t *Task) Origin() TaskOrigin { return t.origin }

// Epoch 返回同一 ID 的生命周期序号（首次入队为 1）。
func (t *Task) Epoch() int { return t.epoch }

// Attempts 返回已发生的尝试次数。
func (t *Task) Attempts() int { return t.attempts }

// State 返回任务当前状态（快照读取，调用方通常持有 Queue.mu）。
func (t *Task) State() TaskState { return t.state }

// LastErr 返回最近一次失败原因，没有则为空串。
func (t *Task) LastErr() string { return t.lastErr }

// MaxAttempts 返回该任务允许的最大尝试次数（含首次）。
func (t *Task) MaxAttempts() int { return t.maxAttempts }

// Config 控制队列容量、并发与重试退避。
type Config struct {
	// Capacity 队列中允许同时存在的未完成任务上限，必须 > 0。
	Capacity int
	// Workers 并发消费者数量，必须 > 0。
	Workers int
	// MaxAttempts 每个任务的最大尝试次数（含首次），必须 > 0。
	MaxAttempts int
	// InitialBackoff 第一次失败后的基础退避，必须 > 0。
	InitialBackoff time.Duration
	// MaxBackoff 退避上限，必须 >= InitialBackoff。
	MaxBackoff time.Duration
	// BackoffMultiplier 每次失败后退避的乘数，必须 >= 1。
	BackoffMultiplier float64
}

// DefaultConfig 返回一套可用的默认配置。
func DefaultConfig() Config {
	return Config{
		Capacity:          1024,
		Workers:           4,
		MaxAttempts:       3,
		InitialBackoff:    10 * time.Millisecond,
		MaxBackoff:        time.Second,
		BackoffMultiplier: 2.0,
	}
}

// validate 校验并规范化配置，非法时返回 ErrInvalidTask 包装错误。
func (c Config) validate() (Config, error) {
	cfg := c
	def := DefaultConfig()
	if cfg.Capacity == 0 {
		cfg.Capacity = def.Capacity
	}
	if cfg.Workers == 0 {
		cfg.Workers = def.Workers
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = def.MaxAttempts
	}
	if cfg.InitialBackoff == 0 {
		cfg.InitialBackoff = def.InitialBackoff
	}
	if cfg.MaxBackoff == 0 {
		cfg.MaxBackoff = def.MaxBackoff
	}
	if cfg.BackoffMultiplier == 0 {
		cfg.BackoffMultiplier = def.BackoffMultiplier
	}
	switch {
	case cfg.Capacity < 0, cfg.Workers < 0, cfg.MaxAttempts < 0,
		cfg.InitialBackoff < 0, cfg.MaxBackoff < 0, cfg.BackoffMultiplier < 0:
		return cfg, errors.New("jobqueue: config field must not be negative")
	}
	if cfg.Capacity == 0 || cfg.Workers == 0 || cfg.MaxAttempts == 0 ||
		cfg.InitialBackoff == 0 || cfg.BackoffMultiplier < 1 {
		return cfg, errors.New("jobqueue: invalid config")
	}
	if cfg.MaxBackoff < cfg.InitialBackoff {
		return cfg, errors.New("jobqueue: MaxBackoff must be >= InitialBackoff")
	}
	return cfg, nil
}

// Stats 是某一时刻队列的任务计数快照。
type Stats struct {
	Pending      int
	Running      int
	RetryWait    int
	Succeeded    int
	DeadLetter   int
	PendingRetry int
	// Accepted = 已被接受的任务生命周期总数（含终态、含重投）。
	Accepted int
	// Enqueued = 通过 Enqueue/EnqueueKey 接受的生命周期数（不含重投）。
	Enqueued int
	// Requeued = 通过 Requeue/RequeueDeadLetters 接受的重投生命周期数。
	Requeued int
}

// InFlight 返回未达终态的任务数。
func (s Stats) InFlight() int {
	return s.Pending + s.Running + s.RetryWait
}

// Done 返回已达终态的任务生命周期数：已完成 + 待重试 + 已丢弃。
func (s Stats) Done() int {
	return s.Succeeded + s.DeadLetter + s.PendingRetry
}

// Conserved 报告接收总数与三种归宿总数是否守恒：
// Accepted == Succeeded + DeadLetter + PendingRetry，且
// Accepted == Enqueued + Requeued。
func (s Stats) Conserved() bool {
	return s.Accepted == s.Done() && s.Accepted == s.Enqueued+s.Requeued
}

// TaskInfo 是任务快照，供重投条件判断与外部观测使用（值拷贝，不含内部字段）。
type TaskInfo struct {
	ID          string
	Type        string
	Payload     []byte
	Key         string
	Origin      TaskOrigin
	Epoch       int
	Attempts    int
	State       TaskState
	MaxAttempts int
	LastErr     string
	EnqueuedAt  time.Time
	UpdatedAt   time.Time
}

func (t *Task) infoLocked() TaskInfo {
	return TaskInfo{
		ID:          t.ID,
		Type:        t.Type,
		Payload:     t.Payload,
		Key:         t.key,
		Origin:      t.origin,
		Epoch:       t.epoch,
		Attempts:    t.attempts,
		State:       t.state,
		MaxAttempts: t.maxAttempts,
		LastErr:     t.lastErr,
		EnqueuedAt:  t.enqueuedAt,
		UpdatedAt:   t.updatedAt,
	}
}
