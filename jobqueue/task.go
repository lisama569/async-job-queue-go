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

	// maxAttempts 在入队时由队列按配置写入，>0。
	maxAttempts int

	// mu 保护以下运行期字段；由 Queue.mu 统一持有（不单独加锁），
	// 仅状态迁移时更新，因此无需额外锁。
	attempts   int
	state      TaskState
	lastErr    string
	enqueuedAt time.Time
	updatedAt  time.Time
}

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
	// Accepted = 已被接受的任务总数（含终态）。
	Accepted int
}

// InFlight 返回未达终态的任务数。
func (s Stats) InFlight() int {
	return s.Pending + s.Running + s.RetryWait
}

// Done 返回已达终态的任务数：已完成 + 待重试 + 已丢弃。
func (s Stats) Done() int {
	return s.Succeeded + s.DeadLetter + s.PendingRetry
}

// Conserved 报告接收总数与三种归宿总数是否守恒：
// Accepted == Succeeded + DeadLetter + PendingRetry。
func (s Stats) Conserved() bool {
	return s.Accepted == s.Done()
}
