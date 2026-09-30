package com.github.highcumontoa.concurrencytaskgovernorjava.task;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Default {@link TaskGovernor}.
 *
 * <p>Capacity model: a fair global semaphore of
 * {@code maxConcurrency + maxQueueSize} permits bounds total in-flight tasks; a
 * fixed worker pool of {@code maxConcurrency} threads bounds actual parallelism;
 * a per-source fair semaphore of {@code perSourceMaxInFlight} permits guarantees
 * fair sharing.
 *
 * <p><b>Convergence guarantee.</b> Terminal status is published only after the
 * handler has stopped and both permits plus the occupancy counters are released
 * (see {@link TaskRecord}). Consequently a task observed as terminal is fully
 * stopped and its capacity is already effective; timeout/cancel/completion
 * resolve to exactly one conclusion. Permits are released exactly once because
 * finalization itself happens exactly once.
 *
 * <p>Occupancy counters are authoritative for {@link #snapshot()} and are kept
 * consistent with permit holdings: +1 on acceptance, -1 at finalization while
 * holding the record monitor.
 */
@Component
@EnableConfigurationProperties(TaskGovernorProperties.class)
public class DefaultTaskGovernor implements TaskGovernor {

    private static final Logger log = LoggerFactory.getLogger(DefaultTaskGovernor.class);
    private static final long CONVERGE_WAIT_MS = 10_000L;

    private final TaskGovernorProperties properties;
    private final Map<String, TaskHandler> handlersByType;

    private final Map<String, TaskRecord> tasksById = new ConcurrentHashMap<>();
    private final Map<String, String> idByIdempotencyKey = new ConcurrentHashMap<>();
    private final Map<String, Semaphore> sourcePermits = new ConcurrentHashMap<>();
    private final Map<String, int[]> sourceCounters = new ConcurrentHashMap<>();

    private final Semaphore globalPermits;
    private final AtomicInteger runningTotal = new AtomicInteger();
    private final AtomicInteger inFlightTotal = new AtomicInteger();

    private final ExecutorService workers;
    private final ScheduledExecutorService scheduler;

    public DefaultTaskGovernor(TaskGovernorProperties properties, List<TaskHandler> handlers) {
        this.properties = properties;
        Map<String, TaskHandler> byType = new HashMap<>();
        for (TaskHandler handler : handlers) {
            TaskHandler previous = byType.putIfAbsent(handler.type(), handler);
            if (previous != null) {
                throw new IllegalStateException("duplicate task handler for type: " + handler.type());
            }
        }
        this.handlersByType = Map.copyOf(byType);
        this.globalPermits = new Semaphore(properties.maxConcurrency() + properties.maxQueueSize(), true);
        this.workers = Executors.newFixedThreadPool(properties.maxConcurrency(),
                runnable -> {
                    Thread t = new Thread(runnable, "task-worker");
                    t.setDaemon(true);
                    return t;
                });
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread t = new Thread(runnable, "task-deadline-scheduler");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public SubmissionResult submit(TaskRequest request) {
        String validationError = validate(request);
        if (validationError != null) {
            return SubmissionResult.rejected(SubmissionOutcome.REJECTED_INVALID, validationError);
        }

        String existingId = idByIdempotencyKey.get(request.idempotencyKey());
        if (existingId != null) {
            TaskRecord existing = resolveExisting(existingId);
            if (existing != null) {
                return SubmissionResult.duplicate(existing);
            }
        }

        if (!globalPermits.tryAcquire()) {
            return SubmissionResult.rejected(SubmissionOutcome.REJECTED_CAPACITY,
                    "global capacity exhausted (running + queued); retry later");
        }
        Semaphore sourceSemaphore = sourcePermits.computeIfAbsent(request.source(),
                key -> new Semaphore(properties.perSourceMaxInFlight(), true));
        if (!sourceSemaphore.tryAcquire()) {
            globalPermits.release();
            return SubmissionResult.rejected(SubmissionOutcome.REJECTED_SOURCE_LIMIT,
                    "source '" + request.source() + "' exceeded its in-flight limit; retry later");
        }

        long timeoutMs = request.timeoutMs() != null ? request.timeoutMs() : properties.defaultTimeoutMs();
        Instant now = Instant.now();
        TaskRecord record = new TaskRecord(UUID.randomUUID().toString(), request.idempotencyKey(),
                request.source(), request.type(), request.payload(), now, now.plusMillis(timeoutMs));

        // Account for the permits while they are held and before the task is
        // published, so a snapshot can never show an accepted task with no
        // occupancy; roll it back if the idempotency race is lost.
        inFlightTotal.incrementAndGet();
        sourceCounters.computeIfAbsent(request.source(), key -> new int[2]);
        bumpSource(request.source(), 0, 1);

        String racedId = idByIdempotencyKey.putIfAbsent(request.idempotencyKey(), record.getId());
        if (racedId != null) {
            bumpSource(request.source(), 0, -1);
            inFlightTotal.decrementAndGet();
            sourceSemaphore.release();
            globalPermits.release();
            TaskRecord raced = resolveExisting(racedId);
            return SubmissionResult.duplicate(raced != null ? raced : record);
        }
        tasksById.put(record.getId(), record);

        log.info("task {} accepted: source={} type={} deadline={}",
                record.getId(), record.getSource(), record.getType(), record.getDeadline());

        scheduler.schedule(() -> onDeadline(record.getId()), timeoutMs, TimeUnit.MILLISECONDS);
        workers.execute(() -> execute(record));
        return SubmissionResult.accepted(record);
    }

    /** Resolve a task by id, briefly covering the publish window of a racing winner. */
    private TaskRecord resolveExisting(String id) {
        TaskRecord existing = tasksById.get(id);
        for (int i = 0; existing == null && i < 100; i++) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                break;
            }
            existing = tasksById.get(id);
        }
        return existing;
    }

    @Override
    public Optional<TaskRecord> findById(String id) {
        return Optional.ofNullable(tasksById.get(id));
    }

    @Override
    public Optional<TaskRecord> cancel(String id) {
        TaskRecord record = tasksById.get(id);
        if (record == null) {
            return Optional.empty();
        }
        applyTerminalSignal(record, TaskStatus.CANCELLED, FailureReason.CANCELLED, "cancelled by caller");
        // Return only once convergence is published, so a post-cancel query can
        // never observe a stale (non-terminal, still-running) conclusion.
        awaitConvergedQuietly(record);
        return Optional.of(record);
    }

    @Override
    public GovernorSnapshot snapshot() {
        int running = runningTotal.get();
        int inFlight = inFlightTotal.get();
        int capacity = properties.maxConcurrency() + properties.maxQueueSize();
        Instant now = Instant.now();

        List<GovernorSnapshot.SourceSnapshot> sourceViews = new ArrayList<>();
        for (Map.Entry<String, int[]> entry : sourceCounters.entrySet()) {
            int[] counters = entry.getValue();
            int srcRunning;
            int srcInFlight;
            synchronized (counters) {
                srcRunning = counters[0];
                srcInFlight = counters[1];
            }
            if (srcInFlight == 0) {
                continue;
            }
            int limit = properties.perSourceMaxInFlight();
            Semaphore semaphore = sourcePermits.get(entry.getKey());
            int available = semaphore != null ? semaphore.availablePermits() : limit;
            sourceViews.add(new GovernorSnapshot.SourceSnapshot(
                    entry.getKey(), srcRunning, srcInFlight - srcRunning, srcInFlight, limit, available));
        }
        sourceViews.sort(java.util.Comparator.comparing(GovernorSnapshot.SourceSnapshot::source));
        return new GovernorSnapshot(now, running, inFlight - running, inFlight,
                capacity, globalPermits.availablePermits(), List.copyOf(sourceViews));
    }

    private void onDeadline(String taskId) {
        TaskRecord record = tasksById.get(taskId);
        if (record == null) {
            return;
        }
        // The scheduler is single-threaded: claim + interrupt only, never block
        // waiting for a worker (the worker performs finalization).
        applyTerminalSignal(record, TaskStatus.TIMED_OUT, FailureReason.TIMEOUT, "deadline reached");
    }

    /** Claim a terminal signal; only the first claim wins and gets to interrupt. */
    private void applyTerminalSignal(TaskRecord record, TaskStatus status,
                                     FailureReason reason, String detail) {
        TaskRecord.ClaimOutcome outcome = record.claimTerminal(status, reason);
        switch (outcome) {
            case CLAIMED_QUEUED -> {
                log.info("task {} claimed {} while queued: handler will never run",
                        record.getId(), status);
                record.finalizeFromClaim(releaseResources(record));
            }
            case CLAIMED_RUNNING -> {
                log.info("task {} claimed {} while running: interrupting handler",
                        record.getId(), status);
                Thread runner = record.runnerThread();
                if (runner != null) {
                    runner.interrupt();
                }
            }
            case TOO_LATE -> log.debug("task {} terminal signal {} lost the race, current={}",
                    record.getId(), status, record.getStatus());
        }
    }

    private void execute(TaskRecord record) {
        String taskId = record.getId();
        if (!record.beginRun(() -> {
            runningTotal.incrementAndGet();
            bumpSource(record.getSource(), 1, 0);
        })) {
            // Claimed (and finalized) while queued; handler must never run.
            Thread.interrupted();
            log.info("task {} skipped execution, already terminal: {}", taskId, record.getStatus());
            return;
        }
        log.info("task {} running", taskId);
        try {
            TaskHandler handler = handlersByType.get(record.getType());
            if (handler == null) {
                record.finalizeFromWorker(TaskStatus.FAILED, FailureReason.UNSUPPORTED_TYPE, null,
                        "unsupported task type: " + record.getType(), releaseResources(record));
                return;
            }
            String handlerResult;
            try {
                handlerResult = handler.execute(record.getPayload());
            } catch (InterruptedException interrupted) {
                // Interrupt is only delivered by a winning claim; publish that
                // claim's conclusion (fallback covers a bare/stray interrupt).
                Thread.interrupted();
                boolean finalized = record.finalizeFromWorker(TaskStatus.CANCELLED, FailureReason.CANCELLED,
                        null, "interrupted", releaseResources(record));
                log.info("task {} interrupted, final state {} (finalizedHere={})",
                        taskId, record.getStatus(), finalized);
                return;
            } catch (Throwable throwable) {
                boolean finalized = record.finalizeFromWorker(TaskStatus.FAILED,
                        FailureReason.HANDLER_ERROR, null,
                        "handler error: " + throwable.getClass().getSimpleName(),
                        releaseResources(record));
                if (finalized) {
                    log.warn("task {} failed: {}", taskId, throwable.getClass().getSimpleName());
                } else {
                    log.info("task {} threw after terminal state {}: {}",
                            taskId, record.getStatus(), throwable.getClass().getSimpleName());
                }
                return;
            }
            boolean finalized = record.finalizeFromWorker(TaskStatus.SUCCEEDED,
                    FailureReason.NONE, handlerResult, null, releaseResources(record));
            if (finalized) {
                log.info("task {} succeeded", taskId);
            } else {
                log.info("task {} finished but converged to {}", taskId, record.getStatus());
            }
        } finally {
            Thread.interrupted();
        }
    }

    /**
     * Cleanup action executed inside the record monitor, immediately before the
     * terminal status is published. Decrements counters then releases permits,
     * so observed termination always matches released occupancy.
     */
    private Runnable releaseResources(TaskRecord record) {
        return () -> {
            String source = record.getSource();
            decrementSource(source, record.getStartedAt() != null);
            inFlightTotal.decrementAndGet();
            if (record.getStartedAt() != null) {
                runningTotal.decrementAndGet();
            }
            Semaphore sourceSemaphore = sourcePermits.get(source);
            if (sourceSemaphore != null) {
                sourceSemaphore.release();
            }
            globalPermits.release();
            log.debug("task {} resources released", record.getId());
        };
    }

    private void bumpSource(String source, int runningDelta, int inFlightDelta) {
        int[] counters = sourceCounters.get(source);
        synchronized (counters) {
            counters[0] += runningDelta;
            counters[1] += inFlightDelta;
        }
    }

    private void decrementSource(String source, boolean wasRunning) {
        int[] counters = sourceCounters.get(source);
        if (counters == null) {
            return;
        }
        synchronized (counters) {
            if (wasRunning) {
                counters[0]--;
            }
            counters[1]--;
        }
    }

    private static void awaitConvergedQuietly(TaskRecord record) {
        try {
            if (!record.awaitConverged(CONVERGE_WAIT_MS, TimeUnit.MILLISECONDS)) {
                log.warn("task {} did not converge within {}ms after terminal signal",
                        record.getId(), CONVERGE_WAIT_MS);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private String validate(TaskRequest request) {
        if (request == null) {
            return "request body is required";
        }
        if (isBlank(request.idempotencyKey())) {
            return "idempotencyKey must not be blank";
        }
        if (isBlank(request.source())) {
            return "source must not be blank";
        }
        if (isBlank(request.type())) {
            return "type must not be blank";
        }
        if (request.timeoutMs() != null
                && (request.timeoutMs() <= 0 || request.timeoutMs() > properties.maxTimeoutMs())) {
            return "timeoutMs must be in (0, " + properties.maxTimeoutMs() + "]";
        }
        return null;
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    @Override
    @PreDestroy
    public void shutdown() {
        scheduler.shutdownNow();
        workers.shutdownNow();
        try {
            workers.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
