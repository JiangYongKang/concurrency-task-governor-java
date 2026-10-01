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

# 取消（幂等；返回时任务已彻底收敛）
curl -X POST localhost:8080/api/tasks/{id}/cancel

# 运行快照：在途/运行/排队总数与按来源拆分（只读，不触发执行）
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
或 `TIMED_OUT`（截止时间在执行前到达，此时处理器从不执行）。**终态只进入
一次**：超时、取消与正常完成相互竞争时，由"终态认领"保证只有一个信号获胜，
先到先生效，后续信号不会覆盖已有终态；终态在执行真正停下、资源全部释放后
才对外发布（见下文"收敛保证"）。

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
- **资源释放**：每个任务到达终态时**恰好释放一次**全局与来源配额。
  释放发生在终态对外可见**之前**（见下一节"收敛保证"），超时/取消/完成/
  失败路径统一走同一套"先认领、再收敛、最后发布终态"的逻辑。

## 收敛保证：看到终态即已真正结束

对外提供一条强保证：**只要调用方在任何接口上观察到某个任务处于终态，这次
执行就已经彻底结束**——处理器线程已经停下，全局配额、来源配额和占用计数都
已在同一时刻归还并对后续调用生效。之后再查询、再取消、再等待，状态、失败
原因、结果、生命周期记录和占用统计都不会再发生任何变化。

实现上把"终态信号"与"终态发布"分开（`TaskRecord`）：

1. **认领（claim）**：超时、取消是对终态的一次竞争认领，只有第一个信号能
   赢（`CLAIMED_QUEUED` / `CLAIMED_RUNNING`），后到的信号得到 `TOO_LATE`
   并被忽略，因此超时、取消、正常完成同时发生时**只有一个结论生效**。
2. **收敛（finalize）**：
   - 在排队阶段被认领：处理器**从不执行**，由认领线程完成收敛；
   - 在运行阶段被认领：中断实际执行线程，由该 worker 在处理器真正停下后
     完成收敛——因此终态结论与"是否真的跑过、是否被中断"对得上，不会
     出现"状态说结束了、其实还在跑"；
   - 没有认领：处理器自然返回，由 worker 以其自然结论（成功/失败）收敛。
3. **发布（publish）**：收敛动作在任务锁内先执行清理（占用计数 -1、释放
   全局与来源信号量），**然后**才把状态切换到终态并追加终态生命周期事件、
   唤醒等待者。`POST /cancel` 会等到终态发布后才返回，所以取消返回后
   立刻查询也不可能看到陈旧的非终态。

重复提交（`DUPLICATE`）、容量不足（`REJECTED_CAPACITY`）、来源超限
（`REJECTED_SOURCE_LIMIT`）的判定标准与以前完全一致，这些路径既不创建
任务也不改变任何占用。

## 生命周期记录（`LifecycleEvent`）

每个任务的查询响应（`GET /api/tasks/{id}`、提交与取消响应）都带一个按
发生先后排序的 `events` 数组，每个元素包含：

| 字段 | 含义 |
|---|---|
| `at` | 事件时间戳 |
| `from` / `to` | 事件前后状态（受理事件 `from` 为 `null`） |
| `reason` | 与该事件关联的归一化失败原因 |
| `detail` | 可选的、对调用方安全的说明（不含异常堆栈/消息） |

记录形状：

- 正常完成：`QUEUED → RUNNING → SUCCEEDED`
- 执行失败：`QUEUED → RUNNING → FAILED`（`reason=HANDLER_ERROR`）
- 排队即取消/超时：`QUEUED → CANCELLED | TIMED_OUT`（不会出现没跑过却有
  `RUNNING` 事件的情况）
- 运行中取消/超时：`QUEUED → RUNNING → CANCELLED | TIMED_OUT`

保证：终态事件的 `to` 与任务最终 `status` 永远一致；记录里绝不会出现没
写过的终态；**到达终态后不会再追加任何事件**。同一个幂等键重复提交拿到
的是同一个任务、同一份记录（事件列表也相同）。

## 运行快照（`GET /api/snapshot`）

一次只读查询返回整个服务当前的占用，用于核对背压与释放：

| 字段 | 含义 |
|---|---|
| `capturedAt` | 快照时间 |
| `running` | 处理器正在执行的任务数 |
| `queued` | 已受理但尚未开始执行的任务数 |
| `inFlight` | `running + queued`（占用全局容量的总数） |
| `globalCapacity` / `globalAvailable` | 全局容量与当前可用量 |
| `sources[]` | 按来源拆分：`source / running / queued / inFlight / limit / available` |

语义保证：

- 恒有 `inFlight = running + queued`，且各来源 `inFlight` 之和等于总数；
- 没有任务在途时 `running/queued/inFlight` **精确归零**，`sources` 为空，
  `globalAvailable == globalCapacity`；
- 提交、取消、超时并发进行时总数守恒：占用随受理 +1、随终态收敛 -1，
  不会凭空多出或丢失；
- 快照查询**只读**：不触发任何执行、不改变任何任务状态，也不消耗配额。

**一致性是"每次查询"而非"最终一致"**：上述恒等式对**任意一次**快照查询
都成立——包括提交、取消、超时正在并发交错的中间时刻，不需要"多查几次才
对得上"，也不会出现负的排队数或超过总容量的在途数。实现上用一把单一的
占用锁（`occupancyLock`）把"全部占用状态"保护为一个原子整体：

- 全局 `running/inFlight` 计数、各来源 `[running, inFlight]` 计数、全局与
  来源信号量、来源计数表都只在该锁内变更或读取；
- **受理**：全局/来源许可的获取与计数 +1 在同一临界区内一起翻转（且保持
  "先抢全局、再抢来源"的判定顺序）；
- **开始执行**：全局与来源的 running 计数在同一临界区内一起 +1；
- **终态收敛**：全局与来源计数 −1、两副信号量释放都在同一临界区内原子
  完成，因此不可能再出现"在途已减、运行未减"造成的瞬时负排队；
- **快照**：在同一临界区内一次性读取全部计数与许可余量后才构造返回值，
  所以一张快照是一个时刻的自洽视图，而不是多个时刻状态的拼凑；
  `globalAvailable = globalCapacity - inFlight`、
  来源 `available = limit - inFlight` 也在该同一视图内成立。

锁的获取顺序是单向的 `任务记录锁 → 占用锁`：占用锁内绝不访问任务记录、
不睡眠、不阻塞（`Semaphore.tryAcquire`/`release` 均不阻塞），临界区只做
整数与信号量操作，因此既不会与任务记录锁形成死锁，对吞吐也没有实质影响。

示例：

```json
{
  "capturedAt": "2026-09-30T05:00:00Z",
  "running": 1,
  "queued": 1,
  "inFlight": 2,
  "globalCapacity": 12,
  "globalAvailable": 10,
  "sources": [
    { "source": "my-app", "running": 1, "queued": 1, "inFlight": 2,
      "limit": 6, "available": 4 }
  ]
}
```

## 兼容性

- **接口**：原有 `POST /api/tasks`、`GET /api/tasks/{id}`、
  `POST /api/tasks/{id}/cancel` 的路径、方法、状态码与请求体均未改变；
  任务视图仅**新增** `events` 字段（原字段保持不变）；新增只读端点
  `GET /api/snapshot`。
- **行为**：提交受理/重复/拒绝判定、限流阈值、终态优先级（先到先生效）、
  失败原因归一化均与既有版本一致；取消语义从"标记后异步收尾"增强为
  "返回时已收敛"，对调用方只观察到更强的保证。本次快照修复**不改变任何
  既有判定与阈值**：幂等键仍在取许可之前以 `putIfAbsent` 登记，重复提交
  （含并发重复）既不创建任务也不占用任何容量，受理失败时撤销键登记以便
  同键可重试；返回体字段形状与状态码保持不变，仅把"占用数字之间偶尔对
  不上"变为"每次查询都严格自洽"。
- **配置**：`application.properties` 的所有键与默认值不变，无需为新能力
  增加任何配置。

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
  返回当前状态。取消调用会**阻塞到终态发布后才返回**——执行已停下、
  占用已释放，返回即满足"看到终态就是真的收敛"。

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

# 收敛保证 / 生命周期 / 快照测试（含超时与取消同时竞争的多轮压测）
mvn test -Dtest=GovernorConvergenceTests

# 快照每次查询自洽 / 精确归零 / 只读 的确定性并发压测
mvn test -Dtest=SnapshotConsistencyTests

# 快照与生命周期的 REST 层测试
mvn test -Dtest=ObservabilityControllerTests
```

测试覆盖：容量打满后的精确执行次数与确定性拒绝、重复/并发重复提交只执行
一次、超时后容量释放、排队与运行中任务的取消、超时与取消叠加时终态不被
覆盖、异常归一化且不透出、单来源限流与失败隔离、混合负载下所有任务收敛
到终态。新增测试重点验证：

- **看到终态即收敛**：取消返回 / 超时可见时，处理器线程确已退出，释放的
  槽位立即可复用，之后重复取消与查询不再改变任何状态；
- **同一时刻的超时-取消竞争**：`GovernorConvergenceTests` 中以超时调度
  与外部取消同时发起、**不固定先后顺序**，连续 40 轮验证每个任务只执行
  一次、只产生一个终态事件、只收敛到一个终态（`TIMED_OUT` 或
  `CANCELLED` 均可能出现），且终态可见时执行已停止、随后快照精确归零；
- **生命周期一致**：记录顺序为受理→运行→终态，终态事件与最终状态、失败
  原因一致，终态后不再有新事件，重复提交共享同一份记录；
- **快照守恒与归零**：稳态总数与按来源拆分一致；24 个任务在提交/取消/
  超时混合并发下，观察者持续校验 `inFlight = running + queued` 且来源
  之和等于总数，全部收敛后所有计数精确归零；快照本身不触发执行。
- **快照每次自洽（`SnapshotConsistencyTests`，本次新增）**：在极小容量
  （3 运行 + 3 排队、5 个来源）下连续 6 轮、每轮 240 个 1–10ms 的短
  任务，6 个提交线程以约 1/3 比例塞入极短截止时间、并对部分任务外部
  取消；4 个观察者线程在整个 churn 期间持续轮询快照（每轮 10 万次
  量级），对**每一张**快照逐次校验：无负数、`running + queued =
  inFlight`、各来源 running/inFlight 之和等于总数、`inFlight ≤
  globalCapacity`、全局与来源 `available = 容量 - 在途`、来源不超限；
  违例在线程内收集、用例结束后必然失败（而非靠断言线程偶尔崩掉）。
  churn 结束后还校验每个受理任务恰好一个终态事件且最多执行一次、占用
  精确归零。该用例在修复前可稳定复现上述撕裂（含负排队、来源之和 5≠6、
  `inFlight=0` 却 `globalAvailable=0` 等），修复后连续多轮零违例。
- **快照严格只读**：占满工作线程并挂住全部任务后连续查询快照，任务状态
  与各项计数、余量必须纹丝不动，新提交仍按原阈值被容量拒绝；取消收敛
  后快照精确归零。

手动验证示例：

```bash
mvn spring-boot:run
# 提交一个长任务后立即取消；返回时 status 已为 CANCELLED，随后查快照应为 0
curl -X POST localhost:8080/api/tasks -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"k1","source":"my-app","type":"sleep","payload":"10000","timeoutMs":60000}'
curl -X POST localhost:8080/api/tasks/{id}/cancel
curl localhost:8080/api/snapshot
```

测试日志会打印任务 id、关键状态迁移与判定依据（`[judgment]` /
`[race-round]` / `[snapshot]` 行）。
