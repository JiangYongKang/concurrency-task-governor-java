# Concurrency Task Governor（受控后台任务执行器）

在 Spring Boot 之上提供一套**服务侧受控执行**的后台任务能力：调用方提交任务后立即拿到稳定标识与
可查询状态；执行并发受硬上限约束；支持幂等提交、截止时间（超时）与协作式取消；多来源共享容量时
做轮转调度与失败隔离；所有异常归一化为稳定、可区分的失败原因，底层异常不透出。

- Java 21 / Spring Boot 3.4 / 无外部中间件依赖（纯 JDK 并发原语实现）

---

## 1. HTTP 接口

| 方法 | 路径 | 说明 | 成功状态 |
|---|---|---|---|
| POST | `/api/tasks` | 提交任务 | `201 Created` |
| POST | `/api/tasks` | 幂等键重复 | `200 OK`（返回既有任务） |
| GET  | `/api/tasks/{taskId 或 idempotencyKey}` | 查询状态 | `200 OK` |
| POST | `/api/tasks/{key}/cancel` | 请求取消 | `200 OK` |

### 提交请求体

```json
{
  "source": "order-service",
  "idempotencyKey": "biz-unique-key-001",
  "taskType": "delay",
  "payload": "300",
  "timeoutMillis": 2000
}
```

- `source`：调用来源标识，用于按来源限流与公平调度；空值归一化为 `default`。
- `idempotencyKey`：**必填**，幂等键。
- `taskType`：**必填**，内置 `delay` / `echo` / `failing`（见第 5 节）。
- `payload`：传给任务处理器的字符串参数。
- `timeoutMillis`：可选；省略时取服务默认值；必须满足 `0 < timeoutMillis <= governor.max-timeout-millis`。

### 受理响应（201/200）

```json
{
  "outcome": "ACCEPTED",
  "message": "task accepted and queued",
  "task": { "taskId": "task-xxxx-uuid", "status": "QUEUED", "executionCount": 0, "...": "..." }
}
```

`outcome` 取值：`ACCEPTED`（已受理）、`DUPLICATE`（幂等键已存在，`task` 为既有任务当前快照）。

### 错误响应（所有 4xx/5xx 统一形态）

```json
{ "reason": "CAPACITY_EXHAUSTED", "message": "global queue is full (64); retry later", "timestampMillis": 1790000000000 }
```

错误响应只包含归一化原因码与简短说明，**不包含底层异常类型/堆栈**。

| HTTP 状态 | reason | 触发条件 |
|---|---|---|
| 400 | `INVALID_INPUT` | 幂等键/任务类型为空、action 为空、超时越界、未知 taskType、JSON 非法 |
| 404 | `NOT_FOUND` | 按 taskId/幂等键查询或取消一个不存在的标识 |
| 429 | `CAPACITY_EXHAUSTED` | 全局队列或该来源队列打满（可重试的暂时状态） |
| 503 | `SERVICE_SHUTDOWN` | 服务关闭过程中提交 |
| 500 | `INTERNAL_ERROR` | 兜底，任何未预期异常（细节仅在服务端日志） |

---

## 2. 任务状态定义（state machine）

```
提交受理 ──► QUEUED ──► RUNNING ──► SUCCEEDED   正常完成
               │           ├──────► FAILED      执行异常（归一化）
               │           ├──────► CANCELLED   运行中被取消且任务响应中断
               │           └──────► TIMED_OUT   到达截止时间
               ├──────────────► CANCELLED      排队中被取消（立即出队）
               └──────────────► TIMED_OUT      排队期间截止时间到达（从未执行）
```

- 提交被**拒绝**（非法/容量不足）不会产生任务实体，调用方拿到的是 `REJECTED` 结论与原因码，
  因此不存在“被静默丢弃”的任务。
- 所有状态迁移只通过 **CAS（compare-and-set）** 完成。超时守护、取消请求与正常完成并发发生时，
  只有第一个 CAS 成功方决定终态，因此结论**唯一且收敛**，重复取消不会改变已确定的结论。
- `executionCount` 为任务实际开始执行的次数，实现保证它**只可能为 0 或 1**：
  排队中超时/取消为 0；开始执行后为 1，失败/超时都不会触发重试（任何任务不执行两次）。

## 3. 失败原因定义（FailureReason）

| 原因 | 含义 | 是否可重试 |
|---|---|---|
| `NONE` | 非失败（正常态） | — |
| `INVALID_INPUT` | 请求字段非法（空键、空类型、超时越界、未知类型等） | 修正请求后重试 |
| `NOT_FOUND` | 标识未知（已过保留窗口或从未提交） | 视业务而定 |
| `DUPLICATE_SUBMISSION` | 幂等键冲突（提交响应以 `outcome=DUPLICATE` 表达，HTTP 200） | 无需重试 |
| `CAPACITY_EXHAUSTED` | 全局/分来源队列已满 | 稍后重试 |
| `EXECUTION_ERROR` | 任务执行体抛出的业务异常（归一化，detail 只保留异常类简名+截断消息） | 由调用方决定 |
| `TIMEOUT` | 到达任务截止时间（排队中或运行中，detail 说明发生阶段） | 由调用方决定 |
| `CANCELLED` | 调用方取消（排队中立即生效；运行中为协作式） | 否 |
| `INTERNAL_ERROR` | JVM Error、意外中断等内部问题 | 可重试 |
| `SERVICE_SHUTDOWN` | 服务关闭：新提交拒绝；未执行的排队任务收敛为该原因 | 重启后重新提交 |

`failureDetail` 是单行、截断（≤200 字符）的人类可读说明，便于区分失败子类；完整异常堆栈仅写入服务端日志。

---

## 4. 并发、背压与公平策略

配置项（`application.properties`，前缀 `governor.`）：

| 配置 | 默认值 | 含义 |
|---|---|---|
| `max-concurrency` | 4 | **全局执行并发硬上限**：固定数量的工作线程，任何时刻在执行的任务数不会超过它 |
| `max-queued-per-source` | 8 | 单个来源的排队容量；打满后该来源新提交立即 429 |
| `max-queued-global` | 64 | 全局排队容量；打满后任何提交立即 429 |
| `default-timeout-millis` | 5000 | 未指定超时时的默认预算 |
| `max-timeout-millis` | 60000 | 允许申请的超时上限 |
| `retained-terminal-tasks` | 1000 | 终态任务保留条数（幂等去重窗口） |

**背压（back-pressure）行为**：执行槽满 → 进入有界分桶队列；队列满 → **立即拒绝（429）**，
调用方不会被阻塞，任务不会被静默丢弃。拒绝消息明确指出饱和的是全局队列还是某来源队列。

**公平调度**：每个来源一个 FIFO 桶；工作线程按“来源轮转（round-robin）”取任务——取过一个来源后
该来源转到轮次末尾。这样即使来源 A 的队列长期积压，来源 B 的任务也会在 A 的任务之间被穿插执行，
单一来源不能长期独占容量；某来源持续失败时（异常在任务边界被捕获并归一化），工作线程立即继续服务
其他来源，失败不会传播、也不会拖垮别人。

## 5. 幂等与截止时间约束

- **幂等**：`idempotencyKey` 相同的重复提交（无论任务在 QUEUED/RUNNING 还是终态）一律返回
  `outcome=DUPLICATE` 与**同一个 taskId** 的当前快照，绝不覆盖、绝不重复执行。
- **有界去重窗口**：终态任务保留最近 `retained-terminal-tasks` 条，超出后最老的终态标识被回收
  （FIFO）。**活动任务永不被回收**。标识回收后，同一幂等键可以再次提交——这是内存有界性的明确
  取舍；需要永久幂等保证时应将该保留值调大或换用外部存储（当前实现为纯内存）。
- **截止时间**：任务受理时即确定绝对截止时刻（`createdAt + timeout`），由独立的单线程守护调度器
  在截止点检查：排队中则直接判 `TIMED_OUT` 并释放队列槽位；运行中则 CAS 判超时并中断工作线程。
  不响应中断的任务无法被强制杀死，但其状态仍会在截止点收敛为 `TIMED_OUT`，工作线程会在任务返回
  后立即被释放（不会执行第二次、结果被丢弃）。
- **取消语义**：排队中取消是**立即**的（状态变 `CANCELLED`、从队列移除、容量释放）；运行中取消是
  **协作式**的——置取消标志并中断线程，行为良好的任务（如内置 `delay`）在下次阻塞/标志检查时结束。
  取消与超时同时发生时由 CAS 决胜，终态恰为其中之一。
- **资源释放**：终态后会取消截止守护任务；超时/取消通过中断唤醒阻塞任务；工作线程为守护线程，
  `@PreDestroy` 时排空队列（未执行任务收敛为 `FAILED/SERVICE_SHUTDOWN`）并中断全部工作线程。

### 内置任务类型

| taskType | payload 语义 | 用途 |
|---|---|---|
| `echo` | 任意文本，立即返回 `echo:<payload>` | 快速成功路径 |
| `delay` | 休眠毫秒数（缺省 50），响应中断/取消 | 占用执行槽、验证超时与取消 |
| `failing` | `延迟毫秒:异常消息`（缺省 `10:forced failure`），延迟后抛 `IllegalStateException` | 验证失败归一化与失败隔离 |

---

## 6. 本地验证方法

前置：JDK 21+、Maven 3.9+。

```bash
# 1) 编译 + 全部测试（17 个用例）
mvn test

# 2) 只跑某一类场景
mvn test -Dtest=TaskGovernorCapacityTest          # 容量打满 / 精确执行次数
mvn test -Dtest=TaskGovernorIdempotencyTest       # 重复提交 / 非法输入
mvn test -Dtest=TaskGovernorTimeoutCancelTest     # 超时、取消及二者竞争叠加
mvn test -Dtest=TaskGovernorDeadlineEdgeTest      # 出队/截止竞争边界压力（300 任务 0 误执行）
mvn test -Dtest=TaskGovernorFairnessFailureTest   # 轮转公平 / 失败隔离 / 保留回收
mvn test -Dtest=TaskApiIntegrationTest            # 端到端 HTTP 契约

# 3) 启动服务后手工验证
mvn spring-boot:run
curl -i -X POST localhost:8080/api/tasks -H 'Content-Type: application/json' \
  -d '{"source":"demo","idempotencyKey":"k1","taskType":"delay","payload":"300","timeoutMillis":1000}'
curl localhost:8080/api/tasks/k1
curl -X POST localhost:8080/api/tasks/k1/cancel
```

### 测试覆盖与可观测性

- `TaskGovernorCapacityTest`：打满执行槽 + 分来源队列 + 全局队列；拒绝原因可区分；6 个受理任务
  全部成功且 `executionCount` 精确为 1、动作总执行次数精确为 6。
- `TaskGovernorIdempotencyTest`：运行中/终态后重复提交返回同一任务且只执行一次；8 类非法输入
  全部归类 `INVALID_INPUT` 且不占用标识/容量；未知标识查询与取消为空。
- `TaskGovernorTimeoutCancelTest`：运行超时、排队超时（执行 0 次）、运行中取消、排队中取消
  （槽位立即释放）、**超时与取消在 180/205/230ms 三个时点竞争叠加**——终态稳定唯一、重复取消
  不改结论、执行次数恰为 1；超时/取消后工作线程可继续服务后续任务（资源释放验证）。
- `TaskGovernorDeadlineEdgeTest`：并发 1 + 占槽 400ms + 300 个 1ms 预算任务的边界压力，
  全部稳定收敛 TIMED_OUT、`executionCount=0`、动作调用次数精确为 0，验证“worker 出队”与
  “截止守护”在同一时刻竞争时不会误执行、不重复簿记。
- `TaskGovernorFairnessFailureTest`：单来源积压无法饿死其他来源（观测到跨来源穿插执行）；
  BAD 来源 5 连失败归一化为 `FAILED/EXECUTION_ERROR` 而 GOOD 来源 5 个任务全部成功；
  终态保留窗口回收最老条目且活动任务不受影响。
- `TaskApiIntegrationTest`：小容量配置（并发 2/单源队列 1/全局 2）下的完整 HTTP 契约。

测试日志会打印每个任务的 **taskId、关键状态迁移（QUEUED→RUNNING→终态）、失败原因、执行次数与
判定依据**（如 `[case] ... REJECTED reason=CAPACITY_EXHAUSTED`、`[await] key=... converged ...`、
`[fair] interleaving observed ...`），便于复盘并发时序。
