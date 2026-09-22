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
```

测试覆盖：容量打满后的精确执行次数与确定性拒绝、重复/并发重复提交只执行
一次、超时后容量释放、排队与运行中任务的取消、超时与取消叠加时终态不被
覆盖、异常归一化且不透出、单来源限流与失败隔离、混合负载下所有任务收敛
到终态。测试日志会打印任务 id、关键状态迁移与判定依据（`[judgment]` 行）。
