package queue

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Queue 是进程内异步任务队列。
//
// 零值不可用，请使用 New 构造。一个 Queue 实例在 Shutdown 后不可复用。
//
// 投递语义：至少一次（at-least-once）。任务一旦成功进入队列，就必然会被
// 执行直至成功或被明确丢弃；同一逻辑 Key 在未到达终态前的重复提交会被
// 拒绝，因此实际执行不会因重复入队而发生多次。
type Queue struct {
	cfg Config
	log Logger

	mu      sync.Mutex
	entries map[string]*entry // 仍在生命周期中的任务（PENDING/RUNNING/RETRYING）
	results map[string]JobResult
	closed  bool

	ch      chan *entry
	metrics Metrics

	// jobWg 对每个“已被接受”的任务计数，到达终态时 Done。
	jobWg sync.WaitGroup
	// retryWg 跟踪所有退避等待协程，保证停机时能唤醒并等待它们重新入队。
	retryWg sync.WaitGroup
	// retryCtx 在 Shutdown 时取消，令所有退避中的任务立即唤醒。
	retryCtx context.Context
	retryCxl context.CancelFunc

	workersWg sync.WaitGroup
	closeOnce sync.Once

	handler Handler
}

// New 创建一个队列并启动后台消费者。
func New(handler Handler, opts ...Option) *Queue {
	cfg := DefaultConfig()
	for _, o := range opts {
		o(&cfg)
	}
	normalize(&cfg)

	retryCtx, retryCxl := context.WithCancel(context.Background())
	q := &Queue{
		cfg:      cfg,
		entries:  make(map[string]*entry),
		results:  make(map[string]JobResult),
		ch:       make(chan *entry, cfg.Capacity),
		retryCtx: retryCtx,
		retryCxl: retryCxl,
		handler:  handler,
	}
	if cfg.logger != nil {
		q.log = cfg.logger
	} else {
		q.log = noopLogger{}
	}
	q.startWorkers()
	return q
}

func (q *Queue) startWorkers() {
	q.workersWg.Add(q.cfg.Workers)
	for i := 0; i < q.cfg.Workers; i++ {
		go q.worker(i)
	}
}

// Enqueue 提交一个任务。
//
// 可能返回：
//   - ErrQueueClosed：队列已关闭；
//   - ErrQueueFull：容量不足；
//   - ErrDuplicate：相同 Key 的任务仍在生命周期中。
func (q *Queue) Enqueue(job Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		q.metrics.incRejected()
		q.log.Printf("job=%s ENQUEUE-REJECTED reason=queue_closed", job.Key)
		return ErrQueueClosed
	}
	if _, ok := q.entries[job.Key]; ok {
		q.metrics.incDuplicates()
		q.log.Printf("job=%s ENQUEUE-REJECTED reason=duplicate", job.Key)
		return ErrDuplicate
	}

	e := &entry{
		key:    job.Key,
		job:    job,
		status: StatusPending,
	}

	// 先登记去重表并对 WaitGroup 计数，再投递：
	// 杜绝“消费者已执行完而 Add 尚未发生”导致 WaitGroup 计数为负。
	q.entries[job.Key] = e
	q.jobWg.Add(1)

	// 持锁进行非阻塞发送，保证“发送成功”与“去重表可见”原子一致。
	select {
	case q.ch <- e:
	default:
		// 容量不足：回滚登记与计数，拒绝原因明确为 ErrQueueFull。
		delete(q.entries, job.Key)
		q.jobWg.Done()
		q.metrics.incRejected()
		q.log.Printf("job=%s ENQUEUE-REJECTED reason=queue_full", job.Key)
		return ErrQueueFull
	}

	q.metrics.incEnqueued()
	q.log.Printf("job=%s ENQUEUED payload_len=%d", job.Key, len(job.Payload))
	return nil
}

func (q *Queue) worker(id int) {
	defer q.workersWg.Done()
	for e := range q.ch {
		// 处理器使用独立 context：停机只立即唤醒“退避等待”，
		// 正在执行的单次调用允许自然完成，实现真正的在途收尾。
		q.process(context.Background(), e)
	}
}

func (q *Queue) process(ctx context.Context, e *entry) {
	q.mu.Lock()
	e.attempts++
	attempt := e.attempts
	q.transition(e, StatusRunning, fmt.Sprintf("attempt=%d", attempt))
	q.mu.Unlock()

	err := q.runHandler(ctx, e.job.Payload)

	q.mu.Lock()
	defer q.mu.Unlock()

	e.lastErr = err
	if err == nil {
		q.finish(e, StatusSucceeded, "")
		return
	}

	if e.attempts >= q.cfg.MaxAttempts {
		q.finish(e, StatusDiscarded, "reason=max_attempts err="+err.Error())
		return
	}

	q.transition(e, StatusRetrying,
		fmt.Sprintf("attempt=%d err=%q next_backoff=%s",
			e.attempts, err.Error(), backoff(q.cfg, e.attempts)))
	q.metrics.incRetried()
	q.retryWg.Add(1)
	go q.scheduleRetry(e)
}

// runHandler 执行业务处理器，并把 panic 转换为普通失败，避免消费者崩溃。
func (q *Queue) runHandler(ctx context.Context, payload []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic recovered: %v", r)
		}
	}()
	return q.handler(ctx, payload)
}

// finish 把任务迁入终态：状态迁移日志、终态留档、计数器与去重表回收。
// 调用方必须持有 q.mu。
func (q *Queue) finish(e *entry, terminal Status, note string) {
	q.transition(e, terminal, note)
	delete(q.entries, e.key)
	q.results[e.key] = e.snapshot()
	switch terminal {
	case StatusSucceeded:
		q.metrics.incSucceeded()
	case StatusDiscarded:
		q.metrics.incDiscarded()
	}
	q.jobWg.Done()
}

// scheduleRetry 在退避结束（或停机唤醒）后把任务重新置为 PENDING 并投递。
func (q *Queue) scheduleRetry(e *entry) {
	defer q.retryWg.Done()

	d := backoff(q.cfg, e.attempts)
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-q.retryCtx.Done():
		// 停机：放弃剩余退避，立即重新投递，保证在途任务被收尾。
	case <-timer.C:
	}

	// 分两段持锁：先在锁内做状态迁移，释放锁后再向通道发送。
	// 否则持锁阻塞发送可能与“空闲 worker 等锁”互相等待形成死锁。
	q.mu.Lock()
	q.transition(e, StatusPending, "requeue")
	q.mu.Unlock()

	// 关闭期间 q.ch 不会被关闭（只有所有任务终态后才关闭），且仍有消费者，
	// 因此此处发送必然被接收，不会向已关闭通道发送。
	q.ch <- e
}

// transition 在锁保护下迁移任务状态并记录日志。调用方必须持有 q.mu。
func (q *Queue) transition(e *entry, to Status, note string) {
	from := e.status
	e.status = to
	if note != "" {
		q.log.Printf("job=%s %s->%s attempts=%d %s", e.key, from, to, e.attempts, note)
	} else {
		q.log.Printf("job=%s %s->%s attempts=%d", e.key, from, to, e.attempts)
	}
}

// Shutdown 优雅停机：不再接受新任务，等待所有已接受任务到达终态。
//
// 返回 nil 表示全部在途任务（含退避中的任务）都已完成或被丢弃；
// 若 ctx 先到期则返回 ctx.Err()，此时任务仍在后台继续处理，可再次调用等待。
func (q *Queue) Shutdown(ctx context.Context) error {
	q.closeOnce.Do(func() {
		q.mu.Lock()
		q.closed = true
		q.mu.Unlock()
		// 立即唤醒所有退避等待中的任务，使其在剩余尝试次数内尽快收尾。
		q.retryCxl()
		q.log.Printf("queue SHUTDOWN initiated")
	})

	done := make(chan struct{})
	go func() {
		q.jobWg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}

	// 所有已接受任务均已终态，此后不会再有任何发送，关闭通道并等待消费者退出。
	close(q.ch)
	q.workersWg.Wait()
	q.log.Printf("queue SHUTDOWN complete")
	return nil
}

// Metrics 返回当前计数器快照。
func (q *Queue) Metrics() Snapshot { return q.metrics.snapshot() }

// ActiveCount 返回仍在生命周期中的任务数（PENDING/RUNNING/RETRYING）。
func (q *Queue) ActiveCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.entries)
}

// Result 返回某 Key 最近一次到达终态的任务结果；尚未终态或从未见过返回 false。
func (q *Queue) Result(key string) (JobResult, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	r, ok := q.results[key]
	return r, ok
}
