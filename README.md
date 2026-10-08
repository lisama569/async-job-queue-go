# async-job-queue-go

进程内异步任务队列与消费调度（本地内存模拟生产、投递、失败重试与优雅停机）。
零外部依赖，仅用 Go 标准库；核心目标是让**并发消费、重复投递、异常停机**下的
任务状态始终可预期：

- 同一逻辑任务（相同 `Task.ID`）的重复提交不会导致重复执行；
- 队列关闭 / 容量不足 / 重复提交 / 重投非法的拒绝原因可明确区分；
- 退避随失败次数单调增长并受配置约束，达到上限进入可观测的死信终态；
- **按业务键分组有序消费**：同键严格 FIFO（含退避重试期间不跳过），不同键仍并发；
- **终态死信任务可按条件可控重投**，重投是全新生命周期，计数独立且不重复执行；
- **限时安全停机**：超时后非执行中的在途任务全部落到可观测终态，RUNNING 保留可见；
- 任意时刻总量守恒：`Accepted == Succeeded + DeadLetter + PendingRetry + InFlight`；
  彻底停机后 `Accepted == Succeeded + DeadLetter + PendingRetry`。

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

按业务键分组有序消费用 `EnqueueWithKey`，把达到最大尝试次数的死信任务重跑用
`Requeue`：

```go
// 同一订单 order-42 的两个任务严格按提交先后执行；不同订单仍并发。
_ = q.EnqueueWithKey("email-42-1", "order-42", "send-email", payload)
_ = q.EnqueueWithKey("stock-42-1", "order-42", "deduct-stock", payload)

// 仅 DEAD_LETTER 任务可重投，作为全新生命周期重跑。
if err := q.Requeue("email-42-1"); err != nil {
    switch {
    case errors.Is(err, jobqueue.ErrTaskNotFound):    // 任务不存在
    case errors.Is(err, jobqueue.ErrNotRequeueable): // 状态不允许（如已成功）
    case errors.Is(err, jobqueue.ErrDuplicateTask):  // 该任务已有一轮在途（并发重投去重）
    case errors.Is(err, jobqueue.ErrQueueFull):      // 容量不足
    case errors.Is(err, jobqueue.ErrQueueClosed):    // 队列已关闭
    }
}
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
任务所属业务键用 `Task.GroupKey()`、生命周期代号用 `Task.Generation()`、
累计重投次数用 `Task.Requeued()` 读取。

## 按业务键分组有序消费

- `EnqueueWithKey(id, key, type, payload)` 在普通入队语义之上附加一个业务键
  （如订单号、账户号）；`key == ""` 等价于普通 `Enqueue`，不参与分组；
- **同键严格 FIFO**：同一 `key` 下的任务严格按提交先后被领取。前一个任务
  在进入终态之前（包括 `RUNNING` 以及失败后的 `RETRY_WAIT` 退避期间），
  后一个任务一直停留在组队列里，绝不会越过前者执行；
- **跨键保持并发**：不同业务键之间、以及无键任务之间仍然由 `Workers` 个
  消费者并发执行，顺序保证不会把整个队列变成串行。头任务执行结束后，同组
  下一个任务立即解锁下发，因此在无失败的稳态下吞吐与并发队列一致；
- 容量与拒绝语义与普通入队完全一致：同键下排队等待的 PENDING 任务同样计入
  `Capacity`，满载返回 `ErrQueueFull`，关闭返回 `ErrQueueClosed`，
  相同 `ID` 仍在途返回 `ErrDuplicateTask`；
- 组在其队列排空且无在途头任务时自动删除，不会无限累积；
- 停机排空阶段仍会尽量按组顺序执行已就绪任务；实在来不及的残留任务统一
  转入 `PENDING_RETRY`（见“限时安全停机”）。

## 终态任务的可控重投（Requeue）

`Requeue(id)` 允许把**已达到最大尝试次数**的 `DEAD_LETTER` 任务重新放回队列：

- 重投是**一次全新的生命周期**：`attempts` 清零、最近失败原因清空、
  `Generation()` 加一；任务保留原来的 `ID / Type / Payload / Key`，
  并按原业务键重新参与分组有序调度；
- 新旧生命周期的计数互不混淆：旧的死信仍计入 `DeadLetter`，新生命周期另外
  计入 `Accepted`，同时 `Stats.Requeued` 加一。因此
  `Accepted = 原始接受数 + 成功重投数`，总量始终守恒；
- 同一逻辑任务不会因重投被重复执行：并发对同一 `ID` 重投至多一次成功。

拒绝原因可区分：

| 场景 | 返回错误 |
| --- | --- |
| 队列已关闭（含停机进行中） | `ErrQueueClosed` |
| 该 ID 在队列中不存在 | `ErrTaskNotFound` |
| 任务为 `SUCCEEDED` / `PENDING_RETRY` 等非死信终态 | `ErrNotRequeueable` |
| 任务正在 `PENDING/RUNNING/RETRY_WAIT`（可能并发重投已抢先） | `ErrDuplicateTask` |
| 未完成任务数已达 `Capacity` | `ErrQueueFull` |

注意：`PENDING_RETRY` 表示停机时交还给外部的任务，应直接重新 `Enqueue`，
而非 `Requeue`；`Requeue` 只面向 `DEAD_LETTER`。

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

## 优雅停机与限时安全停机

`Close(ctx)` 的顺序（由唯一的收尾 goroutine 只执行一次）：

1. 标记关闭并广播 `shutdown`：此后 `Enqueue` / `Requeue` 一律返回
   `ErrQueueClosed`；Handler 的 ctx 被取消；退避 timer 立即被打断，任务转入
   `PENDING_RETRY`；
2. worker 进入排空：在执行完在途任务的同时，非阻塞地把 `ready` 中已就绪的
   任务（含业务键下因头任务结束而解锁的任务）尽量执行完；
3. worker 全部退出后，最终兜底把任何仍非终态的任务（`ready` 残留、业务键
   队列残留）统一转入 `PENDING_RETRY`，然后关闭 ready。

**限时行为**：当传入带截止时间的 ctx 时，`Close` 最多等到 ctx 到期。超时
返回 `ctx.Err()` 的那一刻，系统会立即把当时所有【不在执行中】的未完成任务
（`PENDING` / `RETRY_WAIT`，含 `ready` 与业务键队列里的残留）强制归入
`PENDING_RETRY`——它们在等待上限内落到明确、可观测的终态，绝不静默丢弃，
也不会让计数凭空消失。唯一保留为 `RUNNING` 的是 Handler 尚未返回的在途
任务：Go 无法在进程内安全强杀 goroutine，因此它被保留为**显式可观测**状态，
Handler 返回后按结果归账（成功计 `SUCCEEDED`，失败计 `PENDING_RETRY`）。

停机守恒式分两层：

```
任意时刻（含限时超时、边停机边投递）:
  AllConserved(): Accepted == Succeeded + DeadLetter + PendingRetry + InFlight

彻底收尾后:
  Conserved():    Accepted == Succeeded + DeadLetter + PendingRetry   且 InFlight == 0
```

- `ctx` 超时/取消时 `Close` 返回 `ctx.Err()` 与当时快照（`AllConserved()`
  恒为真），但内部收尾仍会继续到完成；再次调用 `Close`（含并发、多次调用、
  传入已取消 ctx）是安全的：不会重复关闭 channel、不会 panic、不会卡死，
  所有调用最终等待同一份彻底收尾结果；
- 在途任务若因 ctx 取消返回失败，计入 `PENDING_RETRY`（可重投），而不是
  死信；只有正常尝试到 `MaxAttempts` 的任务才进入 `DEAD_LETTER`；
- 停机开始后不会再新建退避协程，避免收尾与重试互相覆盖或 WaitGroup 误用。

## 日志与可观测性

传入任意实现 `Printf(format, args...)` 的 Logger（如 `log.Default()`），
每次状态迁移都会打印任务标识、业务键、生命周期代号、类型、当前尝试次数、
from → to 与原因，例如：

```
task=o1 key=order-1 gen=0 type=t attempt=1 RUNNING -> RETRY_WAIT cause="attempt failed: ..."
task=rq key=- gen=1 type=t attempt=0 DEAD_LETTER -> PENDING cause="requeued as new lifecycle"
task=pend-3 key=- gen=0 type=t attempt=0 PENDING -> PENDING_RETRY cause="shutdown deadline exceeded: ..."
```

同键任务可从日志中的 `key=` 字段串起完整顺序；重投产生的新生命周期用
`gen=` 与旧的尝试次数区分。

`Stats` 提供 `Pending / Running / RetryWait / Succeeded / DeadLetter /
PendingRetry / Accepted / Requeued` 计数快照，以及：

- `InFlight()`：未达终态的任务数（`Pending + Running + RetryWait`）；
- `Done()`：三种终态之和；
- `Conserved()`：彻底收尾后的严格守恒式；
- `AllConserved()`：任意时刻（含限时停机超时）都必须成立的总量守恒式。

## 验证方法

```bash
go vet ./...
go test ./... -race -count=1 -v          # 查看每次状态迁移与每个任务的 key/gen
go test ./... -race -count=50            # 高重复度下的竞态、顺序与守恒
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
| `TestGroupedOrderedFIFO` | 同键任务严格 FIFO，含失败退避窗口内也不越过 |
| `TestGroupedHeadBlocking` | 头任务阻塞时后续任务绝不开始，同组并发度恒为 1 |
| `TestDifferentKeysConcurrent` | 不同业务键与无键任务之间真实并行，队列不整体串行 |
| `TestRequeueDeadLetterRunsFreshLifecycle` | 死信重投为全新生命周期：attempts 清零、gen+1、新旧计数独立守恒 |
| `TestRequeueRejectionReasons` | 不存在 / 成功态 / 关闭等重投拒绝原因可区分 |
| `TestRequeueInFlightAndConcurrentDedup` | 在途任务重投按去重拒绝；32 并发重投恰好 1 次成功、新生命周期只跑 1 次 |
| `TestRequeueRestoresGroupOrdering` | 带键死信重投后仍按原业务键排队，不抢在同组头任务之前 |
| `TestBoundedShutdownObservabilityAndConservation` | 限时超时：12 排队任务落 PENDING_RETRY、4 个 RUNNING 保留可见、快照总量守恒，收尾后 4 成功 + 12 待重试 |
| `TestConcurrentChaosConservation` | 高并发生产/消费 + 死信重投 + 限时停机同时发生，任意时刻与最终计数都守恒 |
| `TestRequeueDuringShutdown` | 停机窗口中并发重投：成功或明确 ErrQueueClosed，最终守恒无状态覆盖 |
| `TestRepeatedCloseSafe` | 12 路并发（含已取消 ctx）重复 Close，不 panic、不卡死、结果同一 |

## 与既有能力的兼容范围

- **去重**：分组有序不改变去重规则，去重仍以 `Task.ID` 为准；`Requeue`
  对在途任务同样返回 `ErrDuplicateTask`，并发重投至多一次成功；
- **退避重试**：同键任务在退避期间继续占住头位置，退避结束后才解锁同组
  下一个任务；停机开始后不再新建退避协程；
- **投递语义（至少一次）**：分组与重投都不改变 at-least-once 语义。
  停机未确认的任务（含同键队列残留）进入 `PENDING_RETRY`，重投的死信任务
  在“已执行但结果未确认”的边界上仍可能被再次执行，业务 Handler 应基于
  `Task.ID`（必要时结合 `Generation()`）做幂等。

## 范围与边界

本实现是**进程内、内存态**的教学/原型实现：没有跨进程持久化，
`PENDING_RETRY` 表达“交还给外部重投”的语义，实际生产中应由调用方把这些任务
写入持久存储后再重新 `Enqueue`（这也是至少一次而非恰好一次的原因）。
