package com.github.highcumontoa.concurrencytaskgovernorjava.core;

import com.github.highcumontoa.concurrencytaskgovernorjava.config.GovernorProperties;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.FailureReason;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.SubmitOutcome;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.SubmitResult;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.TaskAction;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.TaskSnapshot;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.TaskStatus;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 受控后台任务执行器。
 *
 * <h3>并发与背压</h3>
 * <ul>
 *   <li>{@code maxConcurrency} 个工作线程构成全局执行硬上限，任何时刻在执行的任务数不会超过它。</li>
 *   <li>超出上限的任务进入按来源分桶的有界队列：受全局与单来源双重容量约束；
 *       任一容量打满时，提交立即被拒绝，返回 {@code REJECTED + CAPACITY_EXHAUSTED}，
 *       绝不静默丢弃或阻塞调用方。</li>
 *   <li>出队采用“来源轮转（round-robin）”策略：每次取任务后把该来源转到轮次末尾，
 *       单一来源即使持续占满其自身队列，也无法长期独占执行容量。</li>
 * </ul>
 *
 * <h3>幂等</h3>
 * <p>以 {@code idempotencyKey} 去重：重复提交直接返回该键既有任务的当前快照（{@code DUPLICATE}），
 * 绝不覆盖、绝不重复执行。去重窗口为终态任务保留条数（{@code retainedTerminalTasks}）之内。</p>
 *
 * <h3>截止时间与取消</h3>
 * <p>每个任务在受理时即确定绝对截止时间，由独立的守护调度器检查。终态通过 CAS 迁移保证唯一：
 * 超时、取消与正常完成同时发生时，只有一方胜出，结果确定且可解释。</p>
 */
@Service
public class TaskGovernor {

    private static final Logger log = LoggerFactory.getLogger(TaskGovernor.class);

    private final GovernorProperties props;

    /** 调度锁：保护分桶队列、来源轮转顺序、容量计数与注册表结构。 */
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition workAvailable = lock.newCondition();

    /** source -> 排队任务（仅 QUEUED 态任务停留其中）。 */
    private final Map<String, Deque<TaskEntry>> queuesBySource = new HashMap<>();
    /** 来源轮转顺序。 */
    private final Deque<String> sourceOrder = new ArrayDeque<>();

    private int globalQueued;
    private long enqueueSeq;

    /** 幂等键 -> 任务（活动任务与最近的终态任务）。 */
    private final Map<String, TaskEntry> byIdempotencyKey = new HashMap<>();
    /** 任务 id -> 任务。 */
    private final Map<String, TaskEntry> byTaskId = new HashMap<>();
    /** 终态任务按完成先后排队，用于有界回收。 */
    private final Deque<TaskEntry> finishedOrder = new ArrayDeque<>();

    private final ScheduledExecutorService deadlineScheduler;
    private final Thread[] workers;
    private volatile boolean shutdown;

    private final AtomicLong rejectedSeq = new AtomicLong();

    public TaskGovernor(GovernorProperties props) {
        this.props = props;
        int n = Math.max(1, props.getMaxConcurrency());
        ThreadFactory daemonFactory = r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("governor-deadline-guard");
            return t;
        };
        this.deadlineScheduler = Executors.newSingleThreadScheduledExecutor(daemonFactory);
        this.workers = new Thread[n];
        for (int i = 0; i < n; i++) {
            Thread w = new Thread(this::workerLoop, "governor-worker-" + (i + 1));
            w.setDaemon(true);
            workers[i] = w;
            w.start();
        }
        log.info("TaskGovernor started: maxConcurrency={}, maxQueuedGlobal={}, maxQueuedPerSource={}, "
                        + "defaultTimeoutMillis={}, retainedTerminalTasks={}",
                n, props.getMaxQueuedGlobal(), props.getMaxQueuedPerSource(),
                props.getDefaultTimeoutMillis(), props.getRetainedTerminalTasks());
    }

    // ============================================================
    // 提交
    // ============================================================

    /**
     * 提交任务。
     *
     * @param source         调用来源标识（null/空白归一化为 "default"）
     * @param idempotencyKey 幂等键，非空
     * @param taskType       任务类型，非空
     * @param timeoutMillis  超时毫秒；null 使用默认值；必须为正且不超过配置上限
     * @param action         任务执行体
     * @return 受理结果（ACCEPTED / DUPLICATE / REJECTED）
     */
    public SubmitResult submit(String source, String idempotencyKey, String taskType,
                               Long timeoutMillis, TaskAction action) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            return invalid("idempotencyKey must not be blank");
        }
        if (taskType == null || taskType.isBlank()) {
            return invalid("taskType must not be blank");
        }
        if (action == null) {
            return invalid("action must not be null");
        }
        long timeout = props.getDefaultTimeoutMillis();
        if (timeoutMillis != null) {
            if (timeoutMillis <= 0L || timeoutMillis > props.getMaxTimeoutMillis()) {
                return invalid("timeoutMillis must be within (0, " + props.getMaxTimeoutMillis() + "]");
            }
            timeout = timeoutMillis;
        }
        String src = (source == null || source.isBlank()) ? "default" : source.trim();

        lock.lock();
        try {
            if (shutdown) {
                return rejected(FailureReason.SERVICE_SHUTDOWN,
                        "task governor is shutting down; submit is not accepted");
            }
            TaskEntry existing = byIdempotencyKey.get(idempotencyKey);
            if (existing != null) {
                log.info("submit duplicate idempotencyKey={} -> taskId={} status={} (not re-executed)",
                        idempotencyKey, existing.taskId(), existing.status());
                return new SubmitResult(SubmitOutcome.DUPLICATE, existing.snapshot(),
                        "idempotency key already exists; returning existing task");
            }
            if (globalQueued >= props.getMaxQueuedGlobal()) {
                return rejected(FailureReason.CAPACITY_EXHAUSTED,
                        "global queue is full (" + props.getMaxQueuedGlobal() + "); retry later");
            }
            Deque<TaskEntry> perSource = queuesBySource.get(src);
            int perSourceSize = perSource == null ? 0 : perSource.size();
            if (perSourceSize >= props.getMaxQueuedPerSource()) {
                return rejected(FailureReason.CAPACITY_EXHAUSTED,
                        "queue for source '" + src + "' is full ("
                                + props.getMaxQueuedPerSource() + "); retry later");
            }

            long now = System.currentTimeMillis();
            String taskId = newTaskId();
            TaskEntry entry = new TaskEntry(taskId, idempotencyKey, src, taskType, action,
                    now, now + timeout, ++enqueueSeq);

            byIdempotencyKey.put(idempotencyKey, entry);
            byTaskId.put(taskId, entry);
            queuesBySource.computeIfAbsent(src, k -> new ArrayDeque<>()).addLast(entry);
            if (!sourceOrder.contains(src)) {
                sourceOrder.addLast(src);
            }
            globalQueued++;
            workAvailable.signal();

            // 截止时间守护：终态 CAS 失败即说明任务已以其他方式结束。
            long delay = Math.max(0L, entry.deadlineAtMillis() - now);
            entry.bindDeadlineGuard(deadlineScheduler.schedule(
                    () -> enforceDeadline(entry), delay, TimeUnit.MILLISECONDS));

            log.info("submit accepted taskId={} key={} source={} type={} queuePos(global={},source={}) deadlineIn={}ms",
                    taskId, idempotencyKey, src, taskType, globalQueued, perSourceSize + 1, timeout);
            return new SubmitResult(SubmitOutcome.ACCEPTED, entry.snapshot(), "task accepted and queued");
        } finally {
            lock.unlock();
        }
    }

    // ============================================================
    // 查询 / 取消
    // ============================================================

    /** 按任务 id 或幂等键查询当前快照。 */
    public Optional<TaskSnapshot> find(String taskIdOrIdempotencyKey) {
        if (taskIdOrIdempotencyKey == null || taskIdOrIdempotencyKey.isBlank()) {
            return Optional.empty();
        }
        lock.lock();
        try {
            TaskEntry e = byTaskId.get(taskIdOrIdempotencyKey);
            if (e == null) {
                e = byIdempotencyKey.get(taskIdOrIdempotencyKey);
            }
            return e == null ? Optional.empty() : Optional.of(e.snapshot());
        } finally {
            lock.unlock();
        }
    }

    /**
     * 请求取消。
     * <ul>
     *   <li>排队中：立即迁移为 CANCELLED 并从队列移除（容量同时释放）。</li>
     *   <li>运行中：置取消标志并中断工作线程；协作式任务会在下次阻塞/中断检查时结束，
     *       最终是否为 CANCELLED 取决于与超时/正常完成的 CAS 竞争。</li>
     *   <li>已终态：取消不改变任何状态，原样返回当前快照。</li>
     * </ul>
     */
    public Optional<TaskSnapshot> cancel(String taskIdOrIdempotencyKey) {
        TaskEntry entry;
        lock.lock();
        try {
            TaskEntry e = byTaskId.get(taskIdOrIdempotencyKey);
            if (e == null) {
                e = byIdempotencyKey.get(taskIdOrIdempotencyKey);
            }
            if (e == null) {
                return Optional.empty();
            }
            entry = e;

            if (entry.status() == TaskStatus.QUEUED) {
                if (entry.casStatus(TaskStatus.QUEUED, TaskStatus.CANCELLED)) {
                    Deque<TaskEntry> q = queuesBySource.get(entry.source());
                    if (q != null) {
                        q.remove(entry);
                        if (q.isEmpty()) {
                            queuesBySource.remove(entry.source());
                            sourceOrder.remove(entry.source());
                        }
                    }
                    globalQueued--;
                    entry.markFailed(FailureReason.CANCELLED, "cancelled while queued");
                    entry.cancelDeadlineGuard();
                    onTerminalLocked(entry);
                    log.info("cancel taskId={} source={}: removed from queue, status=CANCELLED",
                            entry.taskId(), entry.source());
                }
            } else if (!entry.isTerminal()) {
                // RUNNING：交由工作线程按中断/标志收敛状态。
                entry.requestCancel();
                log.info("cancel taskId={} status={}: interrupt requested", entry.taskId(), entry.status());
            } else {
                log.info("cancel taskId={}: already terminal status={}, state unchanged",
                        entry.taskId(), entry.status());
            }
            return Optional.of(entry.snapshot());
        } finally {
            lock.unlock();
        }
    }

    // ============================================================
    // 工作线程与公平调度
    // ============================================================

    private void workerLoop() {
        while (true) {
            TaskEntry entry;
            lock.lock();
            try {
                while ((entry = pollRunnableLocked()) == null) {
                    if (shutdown) {
                        return;
                    }
                    workAvailable.awaitUninterruptibly();
                }
            } finally {
                lock.unlock();
            }
            execute(entry);
        }
    }

    /**
     * 来源轮转地取出一个可执行任务。
     * 队首若已在排队期间被取消/超时（终态），顺手清理并释放容量；
     * 一个来源本轮取不到就让出，转到队尾继续检查下一个来源，避免单来源队头阻塞他人。
     */
    private TaskEntry pollRunnableLocked() {
        int sourcesToCheck = sourceOrder.size();
        for (int i = 0; i < sourcesToCheck; i++) {
            String src = sourceOrder.pollFirst();
            if (src == null) {
                return null;
            }
            Deque<TaskEntry> q = queuesBySource.get(src);
            try {
                if (q == null || q.isEmpty()) {
                    continue;
                }
                TaskEntry head = q.peekFirst();
                if (head.status() != TaskStatus.QUEUED) {
                    // 排队期间已取消/超时（终态登记由取消/超时路径在同一把锁内完成）：仅回收出队。
                    q.pollFirst();
                    globalQueued--;
                    i--;
                    continue;
                }
                // 注意：即使队首已过截止时间也不在此判定——超时统一由截止守护线程在本锁内
                // CAS + 出队 + 登记，避免两条路径重复扣减容量或重复登记终态。
                q.pollFirst();
                globalQueued--;
                return head;
            } finally {
                // 仍有排队：转回轮次末尾（轮转公平）；已取空：移除空桶，避免残留无效轮次。
                if (q != null && !q.isEmpty()) {
                    sourceOrder.addLast(src);
                } else if (q != null) {
                    queuesBySource.remove(src);
                }
            }
        }
        return null;
    }

    private void execute(TaskEntry entry) {
        // 出队后、执行前的最后一次状态确认（可能已被取消或超时守护命中）。
        if (!entry.casStatus(TaskStatus.QUEUED, TaskStatus.RUNNING)) {
            log.info("skip execution taskId={}: no longer QUEUED (status={})",
                    entry.taskId(), entry.status());
            return;
        }
        // 与超时守护的边界竞态：worker 可能恰好在截止时间点把任务从 QUEUED 转出，
        // 此时绝不能开始执行动作——直接收敛为 TIMED_OUT，保证“过期任务执行 0 次动作”。
        if (entry.deadlineAtMillis() <= System.currentTimeMillis()) {
            if (entry.casStatus(TaskStatus.RUNNING, TaskStatus.TIMED_OUT)) {
                entry.markFailed(FailureReason.TIMEOUT,
                        "deadline reached before execution started");
                entry.cancelDeadlineGuard();
                lock.lock();
                try {
                    onTerminalLocked(entry);
                    workAvailable.signalAll();
                } finally {
                    lock.unlock();
                }
                log.info("deadline taskId={}: expired at dequeue time, action never invoked",
                        entry.taskId());
                logTransition(entry, TaskStatus.TIMED_OUT);
            }
            return;
        }
        entry.markRunning(Thread.currentThread());
        logTransition(entry, TaskStatus.RUNNING);

        TaskStatus finalStatus;
        FailureReason reason = FailureReason.NONE;
        String detail = null;
        String output = null;
        try {
            output = entry.action().execute(entry);
            finalStatus = TaskStatus.SUCCEEDED;
        } catch (Throwable t) {
            boolean interrupted = t instanceof InterruptedException;
            boolean deadlinePassed = entry.deadlineAtMillis() <= System.currentTimeMillis();
            if (interrupted && entry.cancelRequested() && !deadlinePassed) {
                finalStatus = TaskStatus.CANCELLED;
                reason = FailureReason.CANCELLED;
                detail = "cancelled while running";
            } else if (interrupted && deadlinePassed) {
                finalStatus = TaskStatus.TIMED_OUT;
                reason = FailureReason.TIMEOUT;
                detail = "interrupted after deadline";
            } else if (t instanceof InterruptedException) {
                // 关闭或其他中断：按内部错误归一化，不透出中断本身。
                finalStatus = TaskStatus.FAILED;
                reason = FailureReason.INTERNAL_ERROR;
                detail = "execution interrupted";
            } else if (t instanceof Error) {
                finalStatus = TaskStatus.FAILED;
                reason = FailureReason.INTERNAL_ERROR;
                detail = "internal error: " + t.getClass().getSimpleName();
                log.error("task {} raised an Error", entry.taskId(), t);
            } else {
                finalStatus = TaskStatus.FAILED;
                reason = FailureReason.EXECUTION_ERROR;
                detail = "execution error: " + t.getClass().getSimpleName();
                if (t.getMessage() != null) {
                    detail += ": " + safeMessage(t.getMessage());
                }
                log.warn("task {} failed: {}: {}", entry.taskId(),
                        t.getClass().getName(), t.getMessage());
            }
        } finally {
            if (Thread.currentThread() == entry.runner()) {
                Thread.interrupted(); // 清掉中断标志，避免污染工作线程
            }
        }

        boolean won = entry.casStatus(TaskStatus.RUNNING, finalStatus);
        if (!won) {
            // 超时守护或取消已经先行决定了终态；保持其结论，丢弃执行结果。
            log.info("taskId={} finished as {} but terminal state already {}; keeping existing conclusion",
                    entry.taskId(), finalStatus, entry.status());
        } else if (finalStatus == TaskStatus.SUCCEEDED) {
            entry.markSucceeded(output);
        } else {
            entry.markFailed(reason, detail);
        }

        entry.cancelDeadlineGuard();
        lock.lock();
        try {
            onTerminalLocked(entry);
            workAvailable.signalAll();
        } finally {
            lock.unlock();
        }
        logTransition(entry, entry.status());
    }

    /** 截止时间守护：排队中在锁内判定超时并出队；运行中尝试 CAS 超时并中断工作线程。 */
    private void enforceDeadline(TaskEntry entry) {
        TaskStatus s = entry.status();
        if (s.isTerminal()) {
            return;
        }
        if (s == TaskStatus.QUEUED || s == TaskStatus.RUNNING) {
            enforceDeadlineUnderLock(entry, s);
        }
    }

    /**
     * 在调度锁内完成超时判定。
     *
     * <p>边界竞态由两侧分别负责：若守护线程观测到 QUEUED 但 worker 恰好在此刻把任务转为
     * RUNNING（CAS 失败），守护线程<strong>不会</strong>越权判 RUNNING 超时——因为此刻是否真的
     * 已过截止时间必须以 worker 执行前的同一时刻判断为准（见 {@code execute()} 开头的截止检查）。
     * 若任务确实已过期，worker 不会调用动作即收敛 TIMED_OUT；若仍在预算内，则正常执行，
     * 由后续到期的下一次守护判定。</p>
     */
    private void enforceDeadlineUnderLock(TaskEntry entry, TaskStatus observed) {
        boolean removedFromQueue = false;
        boolean won = false;
        lock.lock();
        try {
            if (observed == TaskStatus.QUEUED
                    && entry.casStatus(TaskStatus.QUEUED, TaskStatus.TIMED_OUT)) {
                won = true;
                entry.markFailed(FailureReason.TIMEOUT,
                        "deadline reached before execution started");
                Deque<TaskEntry> q = queuesBySource.get(entry.source());
                if (q != null && q.remove(entry)) {
                    globalQueued--;
                    removedFromQueue = true;
                    if (q.isEmpty()) {
                        queuesBySource.remove(entry.source());
                        sourceOrder.remove(entry.source());
                    }
                }
            } else if (observed == TaskStatus.RUNNING
                    && entry.casStatus(TaskStatus.RUNNING, TaskStatus.TIMED_OUT)) {
                won = true;
                entry.markFailed(FailureReason.TIMEOUT,
                        "deadline reached after " + entry.effectiveTimeoutMillis() + "ms budget");
            }
            if (won) {
                onTerminalLocked(entry);
                workAvailable.signalAll();
            }
        } finally {
            lock.unlock();
        }
        if (won && !removedFromQueue) {
            log.info("deadline taskId={} source={}: TIMED_OUT while running, interrupting worker",
                    entry.taskId(), entry.source());
            Thread r = entry.runner();
            if (r != null) {
                r.interrupt();
            }
        } else if (won) {
            log.info("deadline taskId={} source={}: TIMED_OUT while queued (removed=true)",
                    entry.taskId(), entry.source());
        }
    }

    /** 终态登记：进入完成队列并按有界保留策略回收最老终态任务（调用方须持有调度锁）。 */
    private void onTerminalLocked(TaskEntry entry) {
        finishedOrder.addLast(entry);
        while (finishedOrder.size() > props.getRetainedTerminalTasks()) {
            TaskEntry old = finishedOrder.pollFirst();
            if (old == null) {
                break;
            }
            byTaskId.remove(old.taskId());
            byIdempotencyKey.remove(old.idempotencyKey());
            log.debug("evicted terminal taskId={} key={} (retention limit reached)",
                    old.taskId(), old.idempotencyKey());
        }
    }

    // ============================================================
    // 辅助
    // ============================================================

    private SubmitResult invalid(String message) {
        return rejected(FailureReason.INVALID_INPUT, message);
    }

    private SubmitResult rejected(FailureReason reason, String message) {
        TaskSnapshot snapshot = new TaskSnapshot(
                "rejected-" + rejectedSeq.incrementAndGet(),
                null, null, null, TaskStatus.REJECTED, reason, message, null,
                System.currentTimeMillis(), 0L, 0L, 0L, 0);
        return new SubmitResult(SubmitOutcome.REJECTED, snapshot, message);
    }

    private String newTaskId() {
        return "task-" + UUID.randomUUID();
    }

    /** 截断可能包含底层实现细节的异常消息，避免底层异常原样透出。 */
    private String safeMessage(String message) {
        String trimmed = message.replaceAll("\\s+", " ").trim();
        return trimmed.length() > 200 ? trimmed.substring(0, 200) + "..." : trimmed;
    }

    private void logTransition(TaskEntry entry, TaskStatus status) {
        log.info("transition taskId={} key={} source={} -> {} executionCount={} reason={}",
                entry.taskId(), entry.idempotencyKey(), entry.source(),
                status, entry.snapshot().executionCount(),
                entry.snapshot().failureReason());
    }

    @PreDestroy
    public void shutdown() {
        lock.lock();
        try {
            shutdown = true;
            // 尚未出队的任务永远不会再被执行：收敛为确定终态并释放全部排队容量。
            for (Map.Entry<String, Deque<TaskEntry>> bucket : queuesBySource.entrySet()) {
                for (TaskEntry e : bucket.getValue()) {
                    if (e.casStatus(TaskStatus.QUEUED, TaskStatus.FAILED)) {
                        e.markFailed(FailureReason.SERVICE_SHUTDOWN,
                                "service shutting down; task never executed");
                        onTerminalLocked(e);
                        log.info("shutdown taskId={}: marked FAILED/SERVICE_SHUTDOWN while queued",
                                e.taskId());
                    }
                }
            }
            queuesBySource.clear();
            sourceOrder.clear();
            globalQueued = 0;
            workAvailable.signalAll();
        } finally {
            lock.unlock();
        }
        deadlineScheduler.shutdownNow();
        for (Thread w : workers) {
            w.interrupt();
        }
        log.info("TaskGovernor shutdown signalled");
    }

    /** 测试用：等待系统中不再有活动任务（队列空且没有运行中的任务）。 */
    public boolean awaitQuiet(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (System.nanoTime() < deadline) {
            lock.lock();
            try {
                if (globalQueued == 0 && isNothingRunning()) {
                    return true;
                }
            } finally {
                lock.unlock();
            }
            Thread.sleep(5L);
        }
        return false;
    }

    private boolean isNothingRunning() {
        for (TaskEntry e : byTaskId.values()) {
            if (e.status() == TaskStatus.RUNNING || e.status() == TaskStatus.QUEUED) {
                return false;
            }
        }
        return true;
    }

    /** 测试用：返回当前全局排队数。 */
    public int currentGlobalQueuedForTest() {
        lock.lock();
        try {
            return globalQueued;
        } finally {
            lock.unlock();
        }
    }

    /** 测试用：返回某来源当前排队数。 */
    public int currentSourceQueuedForTest(String source) {
        lock.lock();
        try {
            Deque<TaskEntry> q = queuesBySource.get(source);
            return q == null ? 0 : q.size();
        } finally {
            lock.unlock();
        }
    }

}

