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
	groups *groupRegistry   // 业务键分组有序调度
	counts Stats            // 各状态计数
	closed bool

	// ready 投递已就绪、可立即执行的任务。
	ready chan *Task

	// wakeWG 等待退避调度 goroutine 退出。
	wakeWG sync.WaitGroup
	// workersWG 等待消费者退出。
	workersWG sync.WaitGroup

	stopOnce sync.Once
	shutdown chan struct{}

	// finalDone 在唯一的收尾 goroutine 彻底完成（最终 sweep、ready 关闭）后关闭。
	// 多次 Close 都等待同一个结果。
	finalDone chan struct{}
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
		groups:    newGroupRegistry(),
		ready:     make(chan *Task, cfg.Capacity),
		shutdown:  make(chan struct{}),
		finalDone: make(chan struct{}),
	}
	ctx := context.Background()
	for i := 0; i < cfg.Workers; i++ {
		q.workersWG.Add(1)
		go q.runWorker(i, ctx)
	}
	return q, nil
}

// Enqueue 提交一个逻辑任务（不指定业务键，不参与分组有序调度）。拒绝原因可用
// errors.Is 区分：
//   - ErrInvalidTask：ID 为空；
//   - ErrQueueClosed：队列已关闭；
//   - ErrQueueFull：未完成任务数已达 Capacity；
//   - ErrDuplicateTask：相同 ID 的任务仍未达终态（去重）。
func (q *Queue) Enqueue(id, taskType string, payload []byte) error {
	return q.EnqueueWithKey(id, "", taskType, payload)
}

// EnqueueWithKey 提交一个带业务键 key 的逻辑任务。
//
// 顺序语义：同一个非空 key 下的任务严格按提交先后依次消费，前一个任务
// （含其失败退避期间）未进入终态，后一个不会被领取；不同 key 之间、以及
// 无键任务之间仍保持并发。key 为空等价于 Enqueue。
//
// 容量与拒绝语义与 Enqueue 完全一致：未完成任务数（含同键下排队等待的
// PENDING 任务）达到 Capacity 返回 ErrQueueFull，关闭返回 ErrQueueClosed，
// 相同 ID 仍在途返回 ErrDuplicateTask，ID 为空返回 ErrInvalidTask。
func (q *Queue) EnqueueWithKey(id, key, taskType string, payload []byte) error {
	if id == "" {
		return ErrInvalidTask
	}
	now := time.Now()
	t := &Task{
		ID:          id,
		Key:         key,
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
		// 日志放锁内：existing 是在途任务，字段可能正被 worker 更新。
		q.logTransition(t, t.state, t.state, "reject duplicate: existing="+existingState)
		q.mu.Unlock()
		return ErrDuplicateTask
	}
	if q.counts.InFlight() >= q.cfg.Capacity {
		q.logTransition(t, StatePending, StatePending, "reject: queue full")
		q.mu.Unlock()
		return ErrQueueFull
	}
	// 同一 ID 的历史任务已达终态时允许重新提交，覆盖旧记录。
	q.tasks[id] = t
	q.counts.Accepted++
	q.counts.Pending++
	// 无键任务直接返回；有键任务仅当它是该组头任务时返回，
	// 否则留在组队列里等头任务终态后再下发。
	dispatch := q.admitLocked(t)
	cause := "enqueued"
	if key != "" {
		cause = "enqueued key=" + key
	}
	q.logTransition(t, StatePending, StatePending, cause)
	q.mu.Unlock()

	q.deliver(dispatch)
	return nil
}

// deliver 在锁外把任务下发到 ready。t 为 nil（有键任务被头任务阻塞）时不做事。
//
// ready 容量等于 Capacity，而每个在途任务至多占一个 ready 槽位（同键被阻塞的
// PENDING 任务在进入头位置前不会进入 ready），因此正常运行期间发送不会阻塞。
// shutdown 已广播时放弃发送：任务保持 PENDING，由停机收尾统一归入 PENDING_RETRY，
// 不会丢失也不会重复。
func (q *Queue) deliver(t *Task) {
	if t == nil {
		return
	}
	select {
	case q.ready <- t:
	case <-q.shutdown:
	}
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

// Close 发起优雅/限时停机。可安全重复调用、可与 Enqueue/Requeue/消费并发执行。
//
// 停机顺序（仅由唯一的收尾 goroutine 执行一次）：
//  1. 标记关闭并广播 shutdown：此后 Enqueue/Requeue 一律返回 ErrQueueClosed；
//     Handler 的 ctx 被取消；退避 timer 立即被打断并转入 PENDING_RETRY；
//  2. worker 进入排空：在执行完在途任务的同时，非阻塞地执行 ready 中已就绪的
//     任务（包括业务键下排队、因头任务结束而解锁的任务），尽量优雅收尾；
//  3. worker 全部退出后，最终兜底把任何仍非终态的任务（ready 残留、键队列残留）
//     统一转入 PENDING_RETRY，然后关闭 ready。
//
// 限时行为：若 ctx 在彻底收尾前到期，立即把当时所有【不在执行中】的未完成任务
// （PENDING / RETRY_WAIT，含 ready 与业务键队列里残留的任务）强制归入
// PENDING_RETRY 后返回 ctx.Err()。这些任务因此在等待上限内落到明确、可观测的
// 终态，绝不静默丢弃；唯一保留为 RUNNING 的是 Handler 尚未返回的在途任务
// （进程内无法安全抢占 goroutine），它同样可观测，返回后按结果归账。
//
// 守恒：任意时刻 Stats.AllConserved() 恒为真
// （Accepted == Succeeded + DeadLetter + PendingRetry + InFlight）；
// 彻底收尾后 InFlight()==0 且 Stats.Conserved() 为真：
// Accepted == Succeeded + DeadLetter + PendingRetry。
//
// 无论 ctx 等待上限设为多少（即使是已取消的 ctx），内部收尾都会继续到完成；
// 再次调用 Close 会等待同一收尾结果，不会重复关闭 channel，也不会卡死。
func (q *Queue) Close(ctx context.Context) (Stats, error) {
	q.stopOnce.Do(func() {
		q.mu.Lock()
		q.closed = true
		q.mu.Unlock()
		close(q.shutdown) // 打断退避等待、Handler ctx 与入队发送
		go q.finalize()
	})

	select {
	case <-q.finalDone:
		return q.Stats(), nil
	case <-ctx.Done():
		// 限时到达：把所有非 RUNNING 的未完成任务强制落入可观测终态后返回。
		// RUNNING 保留（Handler 仍在执行），内部收尾继续直至完成。
		q.sweepNonRunningLocked(
			"shutdown deadline exceeded: unfinished non-running task handed back")
		return q.Stats(), ctx.Err()
	}
}

// sweepNonRunningLocked 把所有不在执行中（非 RUNNING）且未达终态的任务
// 转入 PENDING_RETRY。RUNNING 任务保持可观测状态，等其自然完成。
func (q *Queue) sweepNonRunningLocked(cause string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, t := range q.tasks {
		if !t.state.Terminal() && t.state != StateRunning {
			q.transitionLocked(t, StatePendingRetry, cause)
		}
	}
}

// finalize 是停机收尾的唯一执行者，只运行一次。
func (q *Queue) finalize() {
	defer close(q.finalDone)

	// 1) 等退避调度协程退出：它们在 shutdown 后把 RETRY_WAIT 任务
	//    转入 PENDING_RETRY。
	q.wakeWG.Wait()

	// 2) 等全部 worker 退出：收到 shutdown 后它们会先非阻塞排空 ready
	//    （尽量执行完排队中的任务），在途 RUNNING 任务执行到返回。
	q.workersWG.Wait()

	// 3) 最终兜底：把任何仍非终态的任务（ready 残留 / 键队列残留 /
	//    排空后仍未处理的任务）统一归入 PENDING_RETRY，随后关闭 ready
	//    （此刻已不可能再有发送者）。
	q.mu.Lock()
	for _, t := range q.tasks {
		if !t.state.Terminal() {
			q.transitionLocked(t, StatePendingRetry,
				"shutdown final sweep: unfinished task handed back")
		}
	}
	q.mu.Unlock()
	close(q.ready)
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
	key := t.Key
	if key == "" {
		key = "-"
	}
	q.logger.Printf("task=%s key=%s gen=%d type=%s attempt=%d %s -> %s cause=%q",
		t.ID, key, t.generation, t.Type, t.attempts, from, to, cause)
}
