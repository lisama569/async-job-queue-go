# 进程内异步任务队列（Go）

一个零外部依赖的进程内异步任务队列，用本地内存模拟任务的生产、投递与失败重试，
目标是在**并发消费、重复投递、异常停机**下让任务状态始终可预期。

## 快速开始

```go
q := queue.New(handler,
    queue.WithWorkers(4),
    queue.WithCapacity(128),
    queue.WithMaxAttempts(3),
    queue.WithBackoff(10*time.Millisecond, time.Second),
    queue.WithLogger(myLogger), // 可选：打印每次状态迁移
)

err := q.Enqueue(queue.Job{Key: "order-123", Payload: b})

ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
_ = q.Shutdown(ctx)
fmt.Println(q.Metrics())
```

运行可执行示例：

```bash
go run ./cmd/demo
```

## 任务生命周期与状态

```
            Enqueue
PENDING ───────────────► RUNNING ──成功──► SUCCEEDED（终态）
   ▲                        │
   │ requeue                │ 失败且未达上限
   └────── RETRYING ◄───────┘
                            │ 失败且已达 MaxAttempts
                            └──────────► DISCARDED（终态）
```

每次状态迁移都会通过注入的 `Logger` 打印任务标识（`Key`）、源状态、目标状态、
当前尝试次数与附注（失败原因 / 下次退避时长）。handler 发生 panic 会被恢复为一次
普通失败，不会击穿消费者。

## 投递语义：至少一次（at-least-once）

- 任务**一旦入队成功**，就必然会被处理器执行，直到成功或达到上限被丢弃，
  不会被静默丢弃。
- 失败重试意味着同一任务在极端情况下可能被执行多次（at-least-once）。
  **业务方必须保证 handler 幂等**。
- 去重键 `Key` 保证：同一逻辑任务在**未到达终态前**（PENDING / RUNNING /
  RETRYING）的重复提交会被明确拒绝（`ErrDuplicate`），因此重复入队本身不会
  导致重复执行。任务到达终态后，同一 Key 可作为一个新任务再次提交。
- 入队拒绝原因可区分，分别返回哨兵错误：
  - `ErrQueueClosed`：队列已关闭，不再接受写入；
  - `ErrQueueFull`：待处理通道容量不足（非阻塞入队，立即返回，调用方可退避重试）；
  - `ErrDuplicate`：相同 Key 的任务仍在生命周期中。

## 重试与退避规则

- `MaxAttempts` 为**含首次在内**的最大尝试次数，达到后任务进入 `DISCARDED`
  终态，并保留最后一次失败原因（`JobResult.LastError`），不会无限重试。
- 第 `attempt` 次失败后的退避为指数退避：

  ```
  backoff(attempt) = min(BaseBackoff * 2^(attempt-1), MaxBackoff)
  ```

  该序列随失败次数**单调不减**并受 `MaxBackoff` 上限约束（实现上先翻倍再截断，
  避免乘法溢出）。
- 计数器 `Retried` 统计进入退避重试的次数；`Succeeded + Discarded = Enqueued`
  是始终成立的守恒等式（前提是全部入队成功的任务都已终态）。

## 优雅停机与在途收尾

`Shutdown(ctx)` 行为：

1. 标记关闭，此后所有 `Enqueue` 返回 `ErrQueueClosed`；
2. 取消退避等待上下文，**立即唤醒所有处于 RETRYING 的任务**，它们跳过剩余
   退避被重新投递，在剩余尝试次数内尽快收尾（成功或丢弃）；
3. 正在执行的单次 handler 调用允许**自然跑完**（不被停机信号打断）；
4. 等待所有已接受任务到达终态后，关闭内部通道并回收全部 worker。

停机后的守恒保证：

- 无任务丢失：每个成功入队的任务都有终态，`Succeeded + Discarded = Enqueued`；
- 无重复计数：终态迁移在互斥锁内一次性完成（状态 + 留档 + 去重表回收 +
  计数器 + WaitGroup），状态不会互相覆盖；
- `ActiveCount()` 归零，`Metrics()` 中已完成 / 重试 / 丢弃数量可对账。

若 `ctx` 先到期，`Shutdown` 返回 `ctx.Err()`；任务仍在后台继续收尾，可再次
调用 `Shutdown` 等待。

## 可观测性

`Metrics()` 返回原子计数快照：`Enqueued / Rejected / Succeeded / Discarded /
Duplicates / Retried`；`Result(key)` 可查询某任务终态结果（含尝试次数与最后
失败原因）。

## 验证方法

```bash
# 全量单测（建议带竞态检测）
go test -race -timeout 60s ./...

# 查看带任务标识与状态迁移的详细日志
go test -race -v -timeout 60s ./...

# 多跑几轮确认无偶发抖动
go test -race -count=5 ./...
```

测试覆盖场景：

| 用例 | 覆盖点 |
| --- | --- |
| `TestEnqueueAndSucceed` | 基本入队、执行、成功终态 |
| `TestRetryThenSuccess` | 失败重试、退避后成功、重试计数 |
| `TestDiscardedAfterMaxAttempts` | 达上限进入 DISCARDED，失败原因可区分 |
| `TestPanicRecoveredAsFailure` | handler panic 不击穿消费者 |
| `TestDuplicateRejectedWhileActive` | 活跃期重复提交被拒，终态后可重新提交 |
| `TestConcurrentDuplicateSubmissions` | 同 Key 并发提交恰好执行一次 |
| `TestRejectFullAndClosed` | 容量不足 / 已关闭两类拒绝原因可区分 |
| `TestBackoffMonotonicAndBounded` | 退避单调增长且受上限约束 |
| `TestConcurrentProducersConsumersConservation` | 8×50 并发生产消费，终态守恒 |
| `TestShutdownDrainsRetryingJobs` | 停机唤醒长退避任务并收尾 |
| `TestShutdownMidFlightConservation` | 在途任务中断停机，数量守恒、零丢失 |
