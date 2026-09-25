# async-job-queue-go

进程内异步任务队列与消费调度（本地内存模拟生产、投递、失败重试与优雅停机）。
零外部依赖，仅用 Go 标准库；核心目标是让**并发消费、重复投递、异常停机**下的
任务状态始终可预期：

- 同一逻辑任务（相同 `Task.ID`）的重复提交不会导致重复执行；
- 队列关闭 / 容量不足 / 重复提交的拒绝原因可明确区分；
- 退避随失败次数单调增长并受配置约束，达到上限进入可观测的死信终态；
- 优雅停机后任务数量守恒：`Accepted == Succeeded + DeadLetter + PendingRetry`。

## 快速开始

```go
q, err := jobqueue.New(jobqueue.Config{
    Capacity:          128,
    Workers:           4,
    MaxAttempts:       3,
    InitialBackoff:    10 * time.Millisecond,
    MaxBackoff:        time.Second,
    BackoffMultiplier: 2.0,
}, jobqueue.HandlerFunc(func(ctx context.Context, t *jobqueue.Task) error {
    // 业务逻辑；ctx 在停机时会被取消，协作式任务应及时返回
    return doWork(ctx, t.Payload)
}), log.Default()) // logger 可为 nil（静默）

if err := q.Enqueue("order-42", "send-email", payload); err != nil {
    switch {
    case errors.Is(err, jobqueue.ErrDuplicateTask): // 同一逻辑任务仍在队列中
    case errors.Is(err, jobqueue.ErrQueueFull):     // 容量不足
    case errors.Is(err, jobqueue.ErrQueueClosed):   // 队列已关闭
    case errors.Is(err, jobqueue.ErrInvalidTask):   // 参数非法
    }
}

stats, err := q.Close(context.Background()) // 优雅停机并拿到最终计数
```

## 任务状态机

```
            入队                 worker 领取            成功
 (拒绝重复/满载/关闭)  PENDING ───────────────► RUNNING ──────────► SUCCEEDED
                                                  │
                                  失败且未达上限    │ 失败且 attempts == MaxAttempts
                                                  ▼
                                              RETRY_WAIT ──退避到期──► PENDING（重新投递）
 优雅停机（未完成任务）:
   PENDING / RUNNING / RETRY_WAIT ───────────────────────────────► PENDING_RETRY
```

合法迁移受显式状态表约束（`allowedTransitions`），任何非法的状态覆盖都会被
拒绝并打印 `ILLEGAL transition` 日志，避免重试与停机两条路径互相覆盖。

`SUCCEEDED`、`DEAD_LETTER`、`PENDING_RETRY` 是三种终态，可通过
`TaskState(id)` 查询，最近一次失败原因可用 `TaskLastError(id)` 读取。

## 投递语义：至少一次（at-least-once）

- 已进入 ready 通道的任务，worker 一定会领取并调用 Handler；
- Handler 正常返回 `nil` 才算成功；返回 error 或 panic 都计为**一次失败尝试**，
  panic 会被消费者恢复（worker 不会因业务 panic 退出）；
- 停机时尚未成功的任务不会丢失：正在执行的任务执行完毕后按结果归账，
  退避中 / 排队中的任务转入 `PENDING_RETRY`，表示“本进程至少已接收/尝试一次，
  但未确认完成，需要外部持久化后重新投递”。

因此在 Handler 已执行但结果尚未确认的边界上，任务可能在重投后再次执行，
业务 Handler 应基于 `Task.ID` 做幂等。

## 去重规则

- 相同 `ID` 且旧任务仍处于非终态（PENDING/RUNNING/RETRY_WAIT）时，
  `Enqueue` 返回 `ErrDuplicateTask`，32 个 goroutine 并发提交同一 ID，
  保证恰好 1 次被接受、Handler 恰好执行 1 次（见 `TestDedupConcurrentRunsOnce`）；
- 旧任务已达终态后允许用同一 ID 重新提交（新任务覆盖旧记录）。

## 重试与退避

- `MaxAttempts` 为每个任务的最大尝试次数（**含首次**）；达到后任务进入
  `DEAD_LETTER`，不会静默丢弃，也不会无限重试；
- 第 `n` 次失败后的退避：

  ```
  delay = min(MaxBackoff, InitialBackoff * BackoffMultiplier^(n-1)) × (1 + jitter∈[0,0.2))
        且结果不超过 MaxBackoff
  ```

  因此退避随失败次数**单调不减**，始终落在 `[InitialBackoff, MaxBackoff]` 内；
  抖动只缩小重试点的规模，不会让值超过上限；
- `Config` 各字段为 0 时取默认值，负数、乘数 < 1、`MaxBackoff < InitialBackoff`
  会在 `New` 时直接报错。

## 优雅停机行为

`Close(ctx)` 的顺序：

1. 标记关闭并广播 `shutdown`：此后 `Enqueue` 一律返回 `ErrQueueClosed`；
   正在退避的 timer 立即被打断，任务转入 `PENDING_RETRY`；Handler 的 ctx 被取消；
2. 等待所有退避调度 goroutine 退出；
3. 关闭 ready 通道，worker 排空其中已就绪的任务后退出（在途任务执行到返回）；
4. 兜底把仍非终态的任务全部转入 `PENDING_RETRY`，返回最终 `Stats`。

停机守恒式（`Stats.Conserved()`）：

```
Accepted == Succeeded + DeadLetter + PendingRetry   且 InFlight == 0
```

- `ctx` 超时/取消时 `Close` 返回 `ctx.Err()`，但内部收尾仍在继续；再次调用
  `Close` 是安全的（不会重复关闭 channel），会等待收尾完成并返回最终计数；
- 在途任务若因 ctx 取消返回失败，计入 `PENDING_RETRY`（可重投），
  而不是死信；只有正常尝试到 `MaxAttempts` 的任务才进入 `DEAD_LETTER`。

## 日志与可观测性

传入任意实现 `Printf(format, args...)` 的 Logger（如 `log.Default()`），
每次状态迁移都会打印任务标识、类型、当前尝试次数、from → to 与原因，例如：

```
task=retry-3 type=t attempt=1 RUNNING -> RETRY_WAIT cause="attempt failed: ..."
task=retry-3 type=t attempt=1 RETRY_WAIT -> PENDING_RETRY cause="shutdown interrupted backoff after attempt=1"
task=fast-fail type=t attempt=2 RUNNING -> DEAD_LETTER cause="max attempts=2 reached, last error: ..."
```

`Stats` 提供 `Pending / Running / RetryWait / Succeeded / DeadLetter /
PendingRetry / Accepted` 计数快照。

## 验证方法

```bash
go vet ./...
go test ./... -race -count=1 -v          # 查看每次状态迁移日志
go test ./... -race -count=10            # 高重复度下的竞态与守恒
```

测试覆盖：

| 测试 | 场景 |
| --- | --- |
| `TestEnqueueRejectReasons` | 非法 ID / 满载 / 已关闭三类拒绝可区分 |
| `TestDedupConcurrentRunsOnce` | 32 并发重复提交恰好执行 1 次 |
| `TestRetryThenSuccess` | 失败退避后重试成功，尝试次数正确 |
| `TestDeadLetterAfterMaxAttempts` | 达到上限进死信，失败原因可观测，不无限重试 |
| `TestPanicCountsAsFailure` | Handler panic 被恢复并计为失败 |
| `TestBackoffMonotonicAndBounded` | 退避单调且不越界 |
| `TestConcurrentProduceConsumeConservation` | 16 生产者 × 40 任务、8 worker 并发，计数守恒 |
| `TestShutdownInterruptionConservation` | RUNNING / RETRY_WAIT / PENDING 三类在途任务停机收尾，16 个任务全部进入 PENDING_RETRY |
| `TestCloseCanceledThenReclose` | ctx 取消后再次 Close 不 panic 且最终守恒 |

## 范围与边界

本实现是**进程内、内存态**的教学/原型实现：没有跨进程持久化，
`PENDING_RETRY` 表达“交还给外部重投”的语义，实际生产中应由调用方把这些任务
写入持久存储后再重新 `Enqueue`（这也是至少一次而非恰好一次的原因）。
