# async-job-queue-go

进程内异步任务队列与消费调度（本地内存模拟生产、投递、失败重试与优雅停机）。
零外部依赖，仅用 Go 标准库。核心目标是让**并发消费、重复投递、异常停机、
顺序要求、终态恢复**下的任务状态始终可预期：

- 同一逻辑任务（相同 `Task.ID`）的重复提交不会导致重复执行；
- 队列关闭 / 容量不足 / 重复提交 / 终态重投非法的拒绝原因可明确区分；
- **按业务键分组有序消费**：同一业务键严格串行、前一个（含退避重试期间）
  不结束不消费下一个，不同业务键之间仍然并发；
- 退避随失败次数单调增长并受配置约束，达到上限进入可观测的死信终态；
- **终态死信可控重投**：可按条件把 `DEAD_LETTER` 任务放回队列，作为一次全新
  生命周期重新统计，不会与旧次数/结果混算，也不会重复执行同一逻辑任务；
- **限时安全停机**：可指定等待上限，超时后在途任务强制落到可观测的
  `PENDING_RETRY`，计数不丢、不凭空消失，多次调用 / 边停机边投递行为可预期；
- 停机后任务数量守恒：
  `Accepted == Succeeded + DeadLetter + PendingRetry == Enqueued + Requeued`。

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

## 分组有序消费（业务键串行、跨键并发）

```go
// 同一账户的操作严格按提交先后消费；不同账户之间仍并发。
q.EnqueueKey("acct-1", "acct-1:deposit", "op", payload1)
q.EnqueueKey("acct-1", "acct-1:coupon",  "op", payload2)
q.EnqueueKey("acct-2", "acct-2:ship",    "op", payload3) // 与 acct-1 并发
```

语义与并发边界：

- **串行域 = 业务键（`key`）**。每个 key 内部维护一条 FIFO 等待队列，任意时刻
  该 key 至多有一个任务在途（已派发 / `RUNNING` / `RETRY_WAIT`）；
- **前一个任务未进入终态，下一个不会被派发**——包括失败后的退避重试期间，
  因此“同一订单/账户”的副作用不会乱序或交错；
- 前一个任务进入终态（成功 / 死信 / 停机交还）的同一刻，按序 handoff 下一个，
  不额外占用调度线程，跨组吞吐不受影响；
- **不同 key 之间完全并发**，并发度上限仍是 `Workers`；
- `key == ""` 等价于 `Enqueue`，任务不进入任何串行域，走全局并发池；
- 分组任务同样受 `Capacity` 与去重约束，关闭 / 满载 / 重复的拒绝原因与
  `Enqueue` 完全一致（`ErrQueueClosed` / `ErrQueueFull` / `ErrDuplicateTask`）。

停机时的顺序行为：停机开始后 worker 会排空已派发的任务，head 在排空期间完成
仍会 handoff 同组后继一起被排空；来不及派发的组内排队任务统一交还为
`PENDING_RETRY`（见“限时安全停机”），不静默丢弃。

## 终态任务的可控重投

只有进入 **`DEAD_LETTER`（达到 `MaxAttempts`、不再自动重试）** 的任务可重投。

```go
// 单个重投
if err := q.Requeue("order-9"); err != nil {
    switch {
    case errors.Is(err, jobqueue.ErrQueueClosed):    // 队列已关闭
    case errors.Is(err, jobqueue.ErrTaskNotFound):   // 没有该 ID 的记录
    case errors.Is(err, jobqueue.ErrNotRequeueable): // 不是死信（在途/成功/已交还）
    case errors.Is(err, jobqueue.ErrQueueFull):      // 容量不足
    }
}

// 按条件批量重投死信；match 为 nil 表示全部。
ids, err := q.RequeueDeadLetters(func(info jobqueue.TaskInfo) bool {
    return info.Type == "charge" && strings.Contains(info.LastErr, "timeout")
})
```

规则：

- **重投 = 一次全新生命周期**：新任务的尝试次数从 0 开始、清空上次错误、
  重新计入接收/归宿统计；旧生命周期的死信结果保留在历史中，不被覆盖。
  可用 `TaskInfo(id)` 读取当前生命周期（`Origin=REQUEUE`、`Epoch` 递增），
  用 `TaskHistory(id)` 读取旧生命周期；
- **不重复执行同一逻辑任务**：重投前会再次校验任务仍是 `DEAD_LETTER`。并发
  重投 / 投递竞争同一 ID 时，只有一个能成功，其余收到 `ErrNotRequeueable`；
  批量重投遍历时也会跳过已被其他路径处理的 ID；
- 重投任务若带业务键，仍遵守该键的分组有序：作为该键的新成员排在其他在途
  任务之后；
- 守恒式扩展为
  `Accepted == Enqueued + Requeued == Succeeded + DeadLetter + PendingRetry`，
  每个生命周期各自归账，因此“先死信、重投后成功”会同时贡献 1 个 `DeadLetter`
  （历史）与 1 个 `Succeeded`（新生命周期）。

## 任务状态机

```
            入队/重投              worker 领取            成功
 (拒绝重复/满载/关闭/不可重投) PENDING ───────────────► RUNNING ──────────► SUCCEEDED
                                                  │
                                  失败且未达上限    │ 失败且 attempts == MaxAttempts
                                                  ▼
                                              RETRY_WAIT ──退避到期──► PENDING（重新投递）
 优雅停机 / 限时超时（所有未完成任务，含上述任一非终态）:
   PENDING / RUNNING / RETRY_WAIT ───────────────────────────────► PENDING_RETRY
```

合法迁移受显式状态表约束（`allowedTransitions`），任何非法的状态覆盖都会被
拒绝并打印 `ILLEGAL transition` 日志。终态（`SUCCEEDED` / `DEAD_LETTER` /
`PENDING_RETRY`）不可迁出，因此强制收尾后晚到的 Handler 结果不会再改动计数。

## 投递语义：至少一次（at-least-once）

- Handler 正常返回 `nil` 才算成功；返回 error 或 panic 都计为**一次失败尝试**，
  panic 会被消费者恢复（worker 不会因业务 panic 退出）；
- 停机时尚未成功的任务不会丢失：在途的按结果归账（成功/死信/交还），
  退避中 / 排队中的任务转入 `PENDING_RETRY`，表示“本进程至少已接收/尝试一次，
  但未确认完成，需要外部持久化后重新投递”。

因此在 Handler 已执行但结果尚未确认的边界上，任务可能在重投后再次执行，
业务 Handler 应基于 `Task.ID` 做幂等。

## 去重规则

- 相同 `ID` 且旧任务仍处于非终态（PENDING/RUNNING/RETRY_WAIT）时，
  `Enqueue` / `EnqueueKey` 返回 `ErrDuplicateTask`；
- 旧任务已达终态后允许用同一 ID 重新提交（新生命周期覆盖“最新记录”，旧记录进
  `TaskHistory`）；
- 重投接口本身也做同样的在途校验，因此去重与重投不会让同一逻辑任务并发执行。

## 重试与退避

- `MaxAttempts` 为每个生命周期的最大尝试次数（**含首次**）；达到后进入
  `DEAD_LETTER`，不静默丢弃、不无限重试；重投开启新的计数；
- 第 `n` 次失败后的退避：

  ```
  delay = min(MaxBackoff, InitialBackoff * BackoffMultiplier^(n-1)) × (1 + jitter∈[0,0.2))
        且结果不超过 MaxBackoff
  ```

  退避随失败次数**单调不减**，始终落在 `[InitialBackoff, MaxBackoff]` 内。

## 限时安全停机

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
stats, err := q.Close(ctx) // 最多等待 5s
```

`Close(ctx)` 行为：

1. 标记关闭并广播 `shutdown`：此后 `Enqueue`/`EnqueueKey`/`Requeue` 一律返回
   `ErrQueueClosed`；退避 timer 立即被打断；Handler 的 ctx 被取消；
2. 在 `ctx` 的等待上限内等待退避 goroutine、已派发任务的 worker 收尾；
3. **等待上限到达（`ctx` 超时/取消）**：立即把所有仍未达终态的任务强制转为
   `PENDING_RETRY`，Close 返回**冻结的守恒计数**与 `ctx.Err()`。这些任务状态
   明确可观测（`TaskState` / `Stats.PendingRetry`），不会静默丢弃、计数不消失；
4. 强制收尾后，仍在执行的不可中断 Handler 可以继续跑完，但其晚到的结果不再
   改变计数（终态不可覆盖，打印 `ILLEGAL transition`）；
5. 收尾幂等：**多次调用 `Close` 安全**，收尾完成后都返回同一份最终计数（即使
   传入已取消的 ctx）；内部收尾在后台进行到底，不会因调用方超时返回而卡死。

无论等待上限多少、是否超时，守恒式恒成立：

```
Accepted == Succeeded + DeadLetter + PendingRetry == Enqueued + Requeued
且 InFlight == 0
```

验证方法见测试 `TestBoundedShutdownForcesPendingRetry`（不可中断慢任务在超时后
全部进入 `PENDING_RETRY`）、`TestBoundedShutdownRecloseConsistent`（多次 Close
返回同一冻结计数）、`TestCloseWhileProducingConserved` /
`TestCloseConcurrentNoCrash` / `TestRequeueWhileClosingNoLoss`（边停机边生产 /
重投 / 并发 Close 下不崩溃、不丢任务、计数守恒）。

## 日志与可观测性

传入任意实现 `Printf(format, args...)` 的 Logger（如 `log.Default()`），
每次状态迁移都会打印任务标识、**业务键、生命周期来源与序号**、尝试次数、
from → to 与原因，例如：

```
task=acct-1:coupon key=acct-1 origin=ENQUEUE epoch=1 type=op attempt=0 PENDING -> PENDING cause="enqueued"
task=order-9 key=- origin=REQUEUE epoch=2 type=charge attempt=0 PENDING -> PENDING cause="requeued from dead letter, ..."
task=stuck-0 key=- origin=ENQUEUE epoch=1 type=t attempt=1 RUNNING -> PENDING_RETRY cause="shutdown deadline exceeded: ..."
```

`Stats` 提供 `Pending / Running / RetryWait / Succeeded / DeadLetter /
PendingRetry / Accepted / Enqueued / Requeued` 计数快照；`TaskInfo(id)` 提供
单任务（含业务键、来源、序号、尝试次数、最近错误）快照，`TaskHistory(id)`
提供历史生命周期。

## 验证方法

```bash
go vet ./...
go test ./... -race -count=1 -v          # 查看每次状态迁移日志
go test ./... -race -count=50            # 高重复度下的竞态与守恒
```

测试覆盖：

| 测试 | 场景 |
| --- | --- |
| `TestGroupOrderedFIFO` | 同一业务键 20 任务严格 FIFO 串行（并发即 panic） |
| `TestGroupBlocksDuringRetry` | 前一个处于退避/重试期间，同组后继不开始 |
| `TestCrossGroupConcurrent` | 6 个不同业务键可同时并发，不被串行化 |
| `TestGroupMixedWithUngrouped` | 分组任务与无分组任务可同时并发 |
| `TestGroupImmediateShutdownHandsBack` | 立即停机时组内排队任务交还 PENDING_RETRY 且守恒 |
| `TestRequeueFreshLifecycle` | 死信重投后次数清零、可成功、历史保留、统计不混算 |
| `TestRequeueRejectReasons` | 空 ID / 不存在 / 不可重投 / 满载 / 已关闭五类拒绝 |
| `TestRequeueCapacityReject` | 满载拒绝重投且任务保持死信、不计数 |
| `TestRequeueDeadLettersFiltered` | 条件批量重投、同一 ID 不重复重投 |
| `TestRequeueGroupedOrdering` | 重投的死信仍遵守分组有序 |
| `TestRequeueConcurrentNoDuplicate` | 32 并发重投同一死信恰好 1 次成功 |
| `TestBoundedShutdownForcesPendingRetry` | 超时后不可中断在途任务全部落 PENDING_RETRY、守恒 |
| `TestBoundedShutdownRecloseConsistent` | 超时后多次 Close 返回同一冻结计数 |
| `TestCloseWhileProducingConserved` | 边停机边生产，接受/拒绝数量守恒 |
| `TestCloseConcurrentNoCrash` | 并发 Close / 投递 / 查询不 panic、不卡死 |
| `TestBoundedShutdownGroupedConserved` | 限时停机下分组任务全部落账 |
| `TestRequeueWhileClosingNoLoss` | 并发重投 + 投递 + 停机不丢任务、不重复计数 |
| `TestNoGoroutineLeakAfterBoundedClose` | 限时停机并释放慢任务后无 goroutine 泄漏 |
| 原有 9 个用例 | 去重、退避、死信、panic、停机中断、重复 Close 等不回归 |

## 范围与边界

本实现是**进程内、内存态**的教学/原型实现：没有跨进程持久化，
`PENDING_RETRY` 表达“交还给外部重投”的语义，实际生产中应由调用方把这些任务
写入持久存储后再重新 `Enqueue` / `Requeue`（这也是至少一次而非恰好一次的原因）。
重投只作用于本进程内存中的死信记录，进程退出后未持久化的任务需要由外部系统恢复。
