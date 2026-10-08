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

	mu    sync.Mutex
	tasks map[string]*Task // 每个 ID 最新生命周期的任务（含终态）
	// history 保存每个 ID 被替换/重投前的历史生命周期，最新的在 tasks 中。
	history map[string][]*Task
	// groups 维护每个业务键的有序消费状态。
	groups map[string]*keyGroup
	counts Stats // 各状态计数
	closed bool

	// ready 投递已就绪、可立即执行的任务。
	ready chan *Task

	// wakeWG 等待退避调度 goroutine 退出。
	wakeWG sync.WaitGroup
	// pendingWakes 是当前存活的退避调度 goroutine 数（仅在 q.mu 下增减）。
	// 协调器据此在锁内判定“不会再有新调度”，再到锁外 wakeWG.Wait。
	pendingWakes int
	// workersWG 等待消费者退出。
	workersWG sync.WaitGroup
	// dispatching 跟踪所有在途的 ready 发送，关闭 ready 前先等待其归零，
	// 杜绝“向已关闭 channel 发送”。
	dispatching sync.WaitGroup

	stopOnce     sync.Once
	readyOnce    sync.Once
	finalizeOnce sync.Once
	shutdown     chan struct{}

	// finalized 在强制收尾完成后关闭；finalStats 为收尾时刻的一致快照。
	finalized  chan struct{}
	finalStats Stats
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
		cfg:       cfg,
		handler:   handler,
		logger:    logger,
		tasks:     make(map[string]*Task),
		history:   make(map[string][]*Task),
		groups:    make(map[string]*keyGroup),
		ready:     make(chan *Task, cfg.Capacity),
		shutdown:  make(chan struct{}),
		finalized: make(chan struct{}),
	}
	ctx := context.Background()
	for i := 0; i < cfg.Workers; i++ {
		q.workersWG.Add(1)
		go q.runWorker(i, ctx)
	}
	return q, nil
}

// Enqueue 提交一个逻辑任务（不分组，走全局并发池）。拒绝原因可用 errors.Is 区分：
//   - ErrInvalidTask：ID 为空；
//   - ErrQueueClosed：队列已关闭；
//   - ErrQueueFull：未完成任务数已达 Capacity；
//   - ErrDuplicateTask：相同 ID 的任务仍未达终态（去重）。
//
// 需要按业务键有序消费时使用 EnqueueKey。
func (q *Queue) Enqueue(id, taskType string, payload []byte) error {
	return q.EnqueueKey("", id, taskType, payload)
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

// Close 发起停机，并在 ctx 给定的等待上限内等待在途任务收尾。
//
// 收尾规则（优雅路径，等待上限充足时）：
//   - 正在执行的任务：等待其返回，成功计入 Succeeded，业务失败计入重试/死信，
//     因停机 ctx 取消而中断的计入 PendingRetry；
//   - 退避中 / 尚未执行的任务：计入 PendingRetry（至少一次，交还给外部重投）；
//   - 达到 MaxAttempts 的失败任务计入 DeadLetter。
//
// 限时安全：等待上限到达（ctx 超时/取消）时，所有仍未达终态的任务会被
// 立即强制落到 PENDING_RETRY（明确可观测，不静默丢弃、计数不凭空消失），
// Close 返回冻结的最终计数与 ctx.Err()。此后仍在执行的 Handler 可继续跑完，
// 但其结果不再改变计数（终态不可覆盖，会打印 ILLEGAL 迁移日志）。
//
// 守恒（无论等待上限多少、是否超时）：
//
//	Accepted == Succeeded + DeadLetter + PendingRetry == Enqueued + Requeued
//
// 多次调用 Close 安全：首次调用启动停机；收尾完成后每次都返回同一份最终计数。
// 停机进行中再投递（Enqueue/Requeue）一律被 ErrQueueClosed 明确拒绝。
func (q *Queue) Close(ctx context.Context) (Stats, error) {
	q.stopOnce.Do(func() {
		q.mu.Lock()
		q.closed = true
		q.mu.Unlock()
		close(q.shutdown) // 打断退避等待、Handler ctx

		// 协调 goroutine：等所有在途 ready 发送结束 → 在 q.mu 内关闭 ready
		// （与持锁登记的发送严格互斥，杜绝向已关闭 channel 发送）→ 等退避/worker
		// 退出 → 优雅兜底收尾。即便调用方因超时提前返回，收尾仍在后台继续直到冻结。
		go func() {
			// 停机已置 closed 并广播 shutdown：execute 不会再走到
			// scheduleRetryLocked（新失败要么 ctx 取消→PENDING_RETRY，要么
			// 状态机拒绝迁移），因此 pendingWakes 只减不增。持锁轮询其归零，
			// 归零后不会再有 wakeWG.Add，可安全在锁外 Wait。
			for {
				q.mu.Lock()
				left := q.pendingWakes
				q.mu.Unlock()
				if left == 0 {
					break
				}
				time.Sleep(time.Millisecond)
			}
			q.wakeWG.Wait()
			// 再等所有已登记的 ready 发送结束，然后在 q.mu 内关闭 ready，
			// 与持锁登记的发送严格互斥，杜绝向已关闭 channel 发送。
			q.dispatching.Wait()
			q.mu.Lock()
			q.readyOnce.Do(func() { close(q.ready) })
			q.mu.Unlock()
			q.workersWG.Wait()
			q.forceFinalize("shutdown: unfinished task handed back")
		}()
	})

	// 已收尾：直接返回冻结的最终计数（等待上限只约束“等待”，不再改变结果）。
	if s, done := q.finalSnapshot(); done {
		return s, nil
	}

	// 在调用方的等待上限内等收尾完成；超时则立即强制收尾（幂等）。
	select {
	case <-q.finalized:
	case <-ctx.Done():
		q.forceFinalize("shutdown deadline exceeded: unfinished task handed back")
		s, _ := q.finalSnapshot()
		return s, ctx.Err()
	}

	s, _ := q.finalSnapshot()
	return s, nil
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

// logTransition 打印任务标识、业务键、生命周期、尝试次数与状态迁移。
func (q *Queue) logTransition(t *Task, from, to TaskState, cause string) {
	key := t.key
	if key == "" {
		key = "-"
	}
	q.logger.Printf("task=%s key=%s origin=%s epoch=%d type=%s attempt=%d %s -> %s cause=%q",
		t.ID, key, t.origin, t.epoch, t.Type, t.attempts, from, to, cause)
}
