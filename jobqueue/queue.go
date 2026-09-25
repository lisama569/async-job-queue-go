package jobqueue

import (
	"context"
	"sync"
	"time"
)

// Logger 用于打印任务标识与每次状态迁移。
type Logger interface {
	Printf(format string, args ...any)
}

// nopLogger 是默认的空日志实现。
type nopLogger struct{}

func (nopLogger) Printf(string, ...any) {}

// Queue 是进程内异步任务队列：本地内存模拟生产、投递与失败重试。
//
// 并发安全。所有方法可被多个 goroutine 同时调用。
type Queue struct {
	cfg     Config
	handler Handler
	logger  Logger

	mu     sync.Mutex
	tasks  map[string]*Task // 所有被接受的任务（含终态）
	counts Stats            // 各状态计数
	closed bool

	// ready 投递已就绪、可立即执行的任务。
	ready chan *Task

	// wakeWG 等待退避调度 goroutine 退出。
	wakeWG sync.WaitGroup
	// workersWG 等待消费者退出。
	workersWG sync.WaitGroup

	stopOnce  sync.Once
	readyOnce sync.Once
	shutdown  chan struct{}
}

// New 创建队列并启动 cfg.Workers 个消费者。
func New(cfg Config, handler Handler, logger Logger) (*Queue, error) {
	cfg, err := cfg.validate()
	if err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, ErrInvalidTask
	}
	if logger == nil {
		logger = nopLogger{}
	}
	q := &Queue{
		cfg:      cfg,
		handler:  handler,
		logger:   logger,
		tasks:    make(map[string]*Task),
		ready:    make(chan *Task, cfg.Capacity),
		shutdown: make(chan struct{}),
	}
	ctx := context.Background()
	for i := 0; i < cfg.Workers; i++ {
		q.workersWG.Add(1)
		go q.runWorker(i, ctx)
	}
	return q, nil
}

// Enqueue 提交一个逻辑任务。拒绝原因可用 errors.Is 区分：
//   - ErrInvalidTask：ID 为空；
//   - ErrQueueClosed：队列已关闭；
//   - ErrQueueFull：未完成任务数已达 Capacity；
//   - ErrDuplicateTask：相同 ID 的任务仍未达终态（去重）。
func (q *Queue) Enqueue(id, taskType string, payload []byte) error {
	if id == "" {
		return ErrInvalidTask
	}
	now := time.Now()
	t := &Task{
		ID:          id,
		Type:        taskType,
		Payload:     payload,
		maxAttempts: q.cfg.MaxAttempts,
		state:       StatePending,
		enqueuedAt:  now,
		updatedAt:   now,
	}

	q.mu.Lock()
	switch {
	case q.closed:
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
	// 同一 ID 的历史任务已达终态时允许重新提交，覆盖旧记录。
	q.tasks[id] = t
	q.counts.Accepted++
	q.counts.Pending++
	q.mu.Unlock()

	q.logTransition(t, StatePending, StatePending, "enqueued")

	// 持有锁外发送：ready 容量等于 Capacity，而在途任务数受 Capacity 约束，
	// 且每个在途任务至多占一个 ready 槽位，因此发送不会阻塞、不会失败。
	select {
	case q.ready <- t:
	case <-q.shutdown:
		// 极端竞态：入队解锁后、发送前 Close 触发。停机收尾会统一处理该任务。
	}
	return nil
}

// Stats 返回当前任务计数的一致快照。
func (q *Queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.counts
}

// TaskState 返回指定任务的最新状态；不存在时返回 false。
func (q *Queue) TaskState(id string) (TaskState, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	t, ok := q.tasks[id]
	if !ok {
		return StatePending, false
	}
	return t.state, true
}

// TaskAttempts 返回指定任务已发生的尝试次数。
func (q *Queue) TaskAttempts(id string) (int, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	t, ok := q.tasks[id]
	if !ok {
		return 0, false
	}
	return t.attempts, true
}

// TaskLastError 返回最近一次失败的原因。
func (q *Queue) TaskLastError(id string) (string, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	t, ok := q.tasks[id]
	if !ok {
		return "", false
	}
	return t.lastErr, true
}

// Close 发起优雅停机并等待全部任务进入终态后返回最终计数。
//
// 收尾规则：
//   - 正在执行的任务：等待其返回，成功计入 Succeeded，失败计入 PendingRetry；
//   - 退避中 / 尚未执行的任务：计入 PendingRetry（至少一次，交还给外部重投）；
//   - 达到 MaxAttempts 的失败任务计入 DeadLetter。
//
// 守恒：Accepted == Succeeded + DeadLetter + PendingRetry。
// ctx 超时则返回 ctx.Err()，但内部收尾仍会继续直至完成。
func (q *Queue) Close(ctx context.Context) (Stats, error) {
	q.stopOnce.Do(func() {
		q.mu.Lock()
		q.closed = true
		q.mu.Unlock()
		close(q.shutdown) // 打断退避等待与入队发送
	})

	// 1) 等待退避调度 goroutine 退出：它们会把退避中的任务转入 PendingRetry。
	waitWake := make(chan struct{})
	go func() { q.wakeWG.Wait(); close(waitWake) }()
	select {
	case <-waitWake:
	case <-ctx.Done():
		return q.Stats(), ctx.Err()
	}

	// 2) 退避者已清空后关闭 ready，消费者排空已就绪任务后退出。
	q.readyOnce.Do(func() { close(q.ready) })
	waitWorkers := make(chan struct{})
	go func() { q.workersWG.Wait(); close(waitWorkers) }()
	select {
	case <-waitWorkers:
	case <-ctx.Done():
		return q.Stats(), ctx.Err()
	}

	// 3) 兜底：把仍未达终态的任务（ready 中残留等）全部转入 PendingRetry。
	q.mu.Lock()
	for _, t := range q.tasks {
		if !t.state.Terminal() {
			q.transitionLocked(t, StatePendingRetry, "shutdown: unfinished task handed back")
		}
	}
	final := q.counts
	q.mu.Unlock()
	return final, nil
}

// allowedTransitions 定义合法状态迁移表；非法迁移会被拒绝并打印告警，
// 防止停机与重试之间出现状态互相覆盖。
var allowedTransitions = map[TaskState]map[TaskState]bool{
	StatePending: {
		StateRunning:      true,
		StatePendingRetry: true,
	},
	StateRunning: {
		StateSucceeded:    true,
		StateRetryWait:    true,
		StateDeadLetter:   true,
		StatePendingRetry: true,
	},
	StateRetryWait: {
		StatePending:      true,
		StatePendingRetry: true,
	},
}

// transitionLocked 执行一次状态迁移并同步维护计数。
// 调用方必须持有 q.mu。返回是否迁移成功。
func (q *Queue) transitionLocked(t *Task, to TaskState, cause string) bool {
	from := t.state
	if from == to {
		// 同态迁移仅用于记录（如 enqueued 日志），不动计数。
		return true
	}
	if next, ok := allowedTransitions[from]; !ok || !next[to] {
		q.logger.Printf("task=%s ILLEGAL transition %s -> %s cause=%q", t.ID, from, to, cause)
		return false
	}
	q.decCountLocked(from)
	t.state = to
	t.updatedAt = time.Now()
	q.incCountLocked(to)
	q.logTransition(t, from, to, cause)
	return true
}

func (q *Queue) incCountLocked(s TaskState) {
	switch s {
	case StatePending:
		q.counts.Pending++
	case StateRunning:
		q.counts.Running++
	case StateRetryWait:
		q.counts.RetryWait++
	case StateSucceeded:
		q.counts.Succeeded++
	case StateDeadLetter:
		q.counts.DeadLetter++
	case StatePendingRetry:
		q.counts.PendingRetry++
	}
}

func (q *Queue) decCountLocked(s TaskState) {
	switch s {
	case StatePending:
		q.counts.Pending--
	case StateRunning:
		q.counts.Running--
	case StateRetryWait:
		q.counts.RetryWait--
	}
}

// logTransition 打印任务标识、尝试次数与状态迁移。
func (q *Queue) logTransition(t *Task, from, to TaskState, cause string) {
	q.logger.Printf("task=%s type=%s attempt=%d %s -> %s cause=%q",
		t.ID, t.Type, t.attempts, from, to, cause)
}
