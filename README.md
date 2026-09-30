# Concurrency Task Governor

受控的后台任务执行服务：调用方通过 REST 提交任务，服务侧在并发上限、容量、
公平性与截止时间约束下执行任务，并对并发、取消、超时、重复提交等场景给出
一致、可解释的状态结论。

## 快速开始

```bash
mvn spring-boot:run
```

```bash
# 提交任务（202 Accepted，立即返回稳定 id 与可查询状态）
curl -X POST localhost:8080/api/tasks -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"k1","source":"my-app","type":"echo","payload":"hi"}'

# 查询状态
curl localhost:8080/api/tasks/{id}

# 取消（幂等）
curl -X POST localhost:8080/api/tasks/{id}/cancel

# 查看完整生命周期记录（受理 -> 运行 -> 终态，按发生顺序）
curl localhost:8080/api/tasks/{id}/history

# 查看整个服务当前占用快照（纯只读，不触发任何执行）
curl localhost:8080/api/snapshot
```

内置任务类型：`echo`（回显 payload）、`sleep`（按 payload 毫秒数休眠）、
`fail`（总是失败，用于验证失败归一化）。新类型通过实现 `TaskHandler` 接口
并注册为 Spring Bean 接入。

## 状态机与失败原因

### 任务状态（`TaskStatus`）

| 状态 | 含义 |
|---|---|
| `QUEUED` | 已受理，等待工作线程 |
| `RUNNING` | 正在执行 |
| `SUCCEEDED` | 正常完成（终态） |
| `FAILED` | 执行失败（终态） |
| `TIMED_OUT` | 超过截止时间（终态） |
| `CANCELLED` | 被调用方取消（终态） |

迁移规则：`QUEUED → RUNNING → 终态`；排队中的任务可直接进入 `CANCELLED`
或 `TIMED_OUT`（截止时间在执行前到达）。**终态只进入一次**：超时、取消与
正常完成相互竞争时，由 CAS 保证只有一个迁移获胜，先到先生效，后续信号
不会覆盖已有终态。

### 失败原因（`FailureReason`）

| 原因 | 触发条件 |
|---|---|
| `NONE` | 未失败（含 `SUCCEEDED` 与未完成态） |
| `HANDLER_ERROR` | 任务处理器抛出异常 |
| `TIMEOUT` | 超过截止时间 |
| `CANCELLED` | 调用方取消 |
| `UNSUPPORTED_TYPE` | 无对应类型的处理器 |

底层异常**不会**透出给调用方：异常被归一化为上述稳定原因，日志中只记录
异常类名，响应体不包含异常消息或堆栈。

### 提交结果（`SubmissionOutcome`）

| 结果 | HTTP | 含义 |
|---|---|---|
| `ACCEPTED` | 202 | 已受理并排队 |
| `DUPLICATE` | 200 | 幂等键重复，返回原任务，不重复执行 |
| `REJECTED_INVALID` | 400 | 输入非法（空键/空来源/空类型/超时越界），未创建任务 |
| `REJECTED_CAPACITY` | 429 | 全局容量（运行+排队）已满，未创建任务 |
| `REJECTED_SOURCE_LIMIT` | 429 | 该来源超出在途上限，未创建任务 |

拒绝是显式的：任何提交要么受理、要么返回原任务、要么明确拒绝，绝不静默
丢弃或覆盖已有任务。

## 并发与背压策略

- **全局并发上限**：固定大小工作线程池（`maxConcurrency`）限制并行执行数。
- **全局容量上限**：信号量总量为 `maxConcurrency + maxQueueSize`，约束
  "排队 + 运行"的在途任务总数；耗尽即返回 `REJECTED_CAPACITY`（明确拒绝，
  而非无限排队）。
- **来源公平性**：每个 `source` 有独立的在途信号量
  （`perSourceMaxInFlight`），单一来源无法长期独占容量；某来源持续失败时，
  其任务进入终态即释放配额，不会拖垮其他来源。
- **资源释放**：每个任务到达终态时**恰好释放一次**全局与来源配额
  （由去重集合保证），超时/取消/完成/失败路径统一走同一释放逻辑。

## 收敛保证：看到终态即真的结束

对外提供一条线性一致的收敛语义：**调用方一旦查到某任务处于终态
（`SUCCEEDED` / `FAILED` / `TIMED_OUT` / `CANCELLED`），这次执行就已经彻底
结束，它占用的执行线程与全局/来源配额也在同一时刻对调用方生效为已释放。**

内部采用"先裁决、后发布"的两阶段协议（均在任务记录的监视器上原子完成）：

1. **裁决（decision）**：超时、取消、执行完成/失败并发竞争，由 CAS 保证
   恰好只有一个信号获胜，终态结论唯一；失败者什么都不改变。
2. **收敛（settle）**：裁决获胜者先让处理器真正停下来——排队中则保证永不
   执行，运行中则中断并**等待工作线程的 Runnable 返回**——然后释放全局与
   来源配额。
3. **发布（publication）**：只有在"已停 + 已释放"之后，获胜者才翻转状态为
   终态并写入终态事件。状态发布与占用计数的增减在同一把快照锁下完成。

由此得到的可依赖结论：

- 终态只进入一次，且与实际执行对得上：不会出现"状态说结束、其实还在跑"，
  也不会出现"已终态但配额还没还"。
- 之后再查询、再取消、再等待，状态与占用统计都不会再发生任何变化。
- 超时/取消/正常完成同时发生时只有一个结论生效（先到先生效），晚到的取消
  或超时为无操作。
- 取消接口在返回终态前会等处理器收敛，因此返回 `CANCELLED` 即代表中断已被
  处理器响应并退出（处理器仍需正确响应中断，见 `TaskHandler` 约定）。

> 兼容说明：上述加强只收紧了"终态可见"时刻的含义，不改变状态机、HTTP 状态
> 码与提交判定；重复提交仍返回原任务，容量不足仍为 `REJECTED_CAPACITY`，
> 来源超限仍为 `REJECTED_SOURCE_LIMIT`，既有配置项与请求字段保持不变。

## 生命周期记录

每个任务都有一份按发生顺序排列、只增不改的事件流，可通过
`GET /api/tasks/{id}/history` 获取（未知 id 返回 404）。每个事件包含：

| 字段 | 含义 |
|---|---|
| `sequence` | 从 0 开始的序号，严格按发生顺序 |
| `status` | 该步到达的状态：`QUEUED`（受理）、`RUNNING`（真正开跑）或某个终态 |
| `failureReason` | 终态事件的归一化原因，非终态为 `NONE` |
| `at` | 该迁移被观测到的时间戳 |

一致性保证：

- 记录里的**最后一个事件必为终态，且与任务当前状态完全一致**；不会出现
  记录里没写过的终态。
- 终态事件写入后记录即冻结，之后再取消/再等超时都不会追加新事件。
- 排队中被取消/超时的任务只有 `QUEUED -> 终态` 两个事件，`RUNNING` 不会
  出现，可据以判断"先进队还是先超时、何时真正跑起来、最后谁赢了"。
- 同一个幂等键重复提交拿到的是同一个任务、同一份记录。

## 运行快照

`GET /api/snapshot` 一次返回整个服务的当前占用，便于核对背压与释放：

```json
{
  "running": 2,
  "queued": 1,
  "totalInFlight": 3,
  "sources": [
    { "source": "app-a", "running": 2, "queued": 0, "totalInFlight": 2 },
    { "source": "app-b", "running": 0, "queued": 1, "totalInFlight": 1 }
  ]
}
```

- `running + queued == totalInFlight` 恒成立；按 `source` 给出同样的拆分。
- **没有任务在途时所有数字精确为 0**，来源列表为空。
- 计数与状态发布在同一把锁下联动，因此快照内部自洽，与已发放/已归还的
  信号量许可严格对账：提交、取消、超时并发交错时数字不会凭空多或凭空少。
- 快照是**纯只读**：不触发任何执行、不改变任何任务状态。

## 幂等与截止时间约束

- **幂等键**：`idempotencyKey` 必填。相同键的重复提交（含并发提交）返回
  首次创建的任务 id，任务只执行一次；键到任务的映射在任务创建前以
  `putIfAbsent` 原子登记，杜绝并发下的重复执行。
- **截止时间**：`deadline = 提交时间 + timeoutMs`（缺省用
  `defaultTimeoutMs`，上限 `maxTimeoutMs`）。到期后未完成的任务迁移为
  `TIMED_OUT`：排队中的不再执行，运行中的被中断（`Future.cancel(true)`，
  处理器需响应中断）。
- **取消**：`POST /api/tasks/{id}/cancel` 幂等。排队任务直接标记
  `CANCELLED` 且永不被执行；运行中任务被中断；终态任务取消为无操作，
  返回当前状态。

## 配置项（`application.properties`）

| 键 | 默认值 | 含义 |
|---|---|---|
| `task-governor.max-concurrency` | 4 | 并行执行上限 |
| `task-governor.max-queue-size` | 0 | 排队上限 |
| `task-governor.per-source-max-in-flight` | 并发+队列 | 单来源在途上限 |
| `task-governor.default-timeout-ms` | 30000 | 默认超时 |
| `task-governor.max-timeout-ms` | 300000 | 超时上限 |

## 本地验证

```bash
# 全部测试（单元 + HTTP 集成）
mvn test

# 仅核心并发语义测试
mvn test -Dtest=TaskGovernorTests

# 仅 REST 层测试
mvn test -Dtest=TaskControllerTests

# 收敛保证 + 快照对账（含"终态可见时处理器已真正退出"、归零断言）
mvn test -Dtest=ConvergenceAndSnapshotTests

# 超时/取消同时竞争（40 轮随机碰撞，两种获胜顺序都必须出现）+ 生命周期记录
mvn test -Dtest=TimeoutCancelRaceAndHistoryTests

# 新观测端点的 HTTP 层测试（history / snapshot）
mvn test -Dtest=ObservabilityControllerTests
```

测试覆盖：容量打满后的精确执行次数与确定性拒绝、重复/并发重复提交只执行
一次、超时后容量释放、排队与运行中任务的取消、超时与取消叠加时终态不被
覆盖、异常归一化且不透出、单来源限流与失败隔离、混合负载下所有任务收敛
到终态。测试日志会打印任务 id、关键状态迁移与判定依据（`[judgment]` 行）。

新增行为的重点测试：

- **真同时竞争**：`timeoutAndCancelRace_atExactlyTheSameMoment` 让取消时刻在
  截止时间前后对称抖动（不是固定先超时或先取消），`@RepeatedTest(40)` 反复
  跑，并用跨迭代计数断言两种获胜顺序都真实发生过；每轮校验任务只执行一次、
  只收敛到一个终态、终态可见瞬间处理器已停住（`active == 0`）、晚到信号不再
  改变任何东西。
- **终态即收敛**：观察到 `TIMED_OUT` 的同一时刻断言处理器线程已退出、停止
  计数到位，且后续提交立即被受理（配额同时刻已释放）。
- **快照对账**：忙时断言 running/queued/total 与按来源拆分自洽；全部收敛后
  断言精确归零、来源消失；提交/取消/超时混合并发多轮后同样归零且无许可泄漏。
- **生命周期一致性**：终态事件与最终状态一致、时间戳有序、终态后冻结；排队
- 取消不出现 `RUNNING`；成功/失败/超时/取消四种终态事件可区分；重复提交共享
  同一份记录。
