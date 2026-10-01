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
 * <p><b>Snapshot consistency.</b> All occupancy counters live in a single
 * {@link OccupancyLedger} whose every mutation and every read takes one monitor.
 * {@link #snapshot()} is therefore one linearizable point: running + queued,
 * the in-flight total and the per-source breakdown are always drawn from the
 * same state and can never tear (no negative queued counts, no source/total
 * mismatch). {@code snapshot()} only reads: it never runs a task, never changes
 * task state and never acquires an admission permit.
 *
 * <p><b>Atomic admission.</b> The duplicate-check &rarr; capacity/source check
 * &rarr; register sequence for one idempotency key runs under a per-key lock, so
 * concurrent same-key submits are a strict total order: exactly one wins as
 * ACCEPTED and every loser observes it as DUPLICATE instead of racing past the
 * lookup and being misreported as a capacity rejection. Counters move +1 once
 * on acceptance and -1 once at finalization, so occupancy is conserved and
 * every accepted task converges the counters back to exactly zero.
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
    /** One admission lock per idempotency key, serializing duplicate-check -&gt; accept -&gt; register. */
    private final Map<String, Object> keyLocks = new ConcurrentHashMap<>();
    private final OccupancyLedger ledger = new OccupancyLedger();

    private final Semaphore globalPermits;

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

        TaskRecord acceptedRecord;
        long timeoutMs;
        // Serialize the whole duplicate-check -> capacity check -> register
        // sequence per idempotency key. Without this, concurrent same-key
        // submits could all pass the initial lookup, each grab capacity, and
        // only discover the collision at putIfAbsent — which misclassified the
        // losers as capacity rejections and churned occupancy +1/-1 under the
        // snapshot observer. A loser now always observes the winner (or a
        // capacity/source rejection, with the same precedence as before).
        Object keyLock = keyLocks.computeIfAbsent(request.idempotencyKey(), key -> new Object());
        synchronized (keyLock) {
            String existingId = idByIdempotencyKey.get(request.idempotencyKey());
            if (existingId != null) {
                // The winner registers in tasksById before the key map, so the
                // record is already published when a racing loser reaches here.
                TaskRecord existing = tasksById.get(existingId);
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

            timeoutMs = request.timeoutMs() != null ? request.timeoutMs() : properties.defaultTimeoutMs();
            Instant now = Instant.now();
            TaskRecord record = new TaskRecord(UUID.randomUUID().toString(), request.idempotencyKey(),
                    request.source(), request.type(), request.payload(), now, now.plusMillis(timeoutMs));

            // Publish the record and the idempotency mapping while still holding
            // the admission lock; account first so an accepted task is never
            // visible without its occupancy.
            ledger.acceptQueued(request.source());
            tasksById.put(record.getId(), record);
            idByIdempotencyKey.put(request.idempotencyKey(), record.getId());
            acceptedRecord = record;
        }

        log.info("task {} accepted: source={} type={} deadline={}",
                acceptedRecord.getId(), acceptedRecord.getSource(),
                acceptedRecord.getType(), acceptedRecord.getDeadline());

        scheduler.schedule(() -> onDeadline(acceptedRecord.getId()), timeoutMs, TimeUnit.MILLISECONDS);
        workers.execute(() -> execute(acceptedRecord));
        return SubmissionResult.accepted(acceptedRecord);
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
        int capacity = properties.maxConcurrency() + properties.maxQueueSize();
        Instant now = Instant.now();

        // One linearizable read: totals and the per-source breakdown can never
        // disagree, because the ledger captures them in a single locked step.
        OccupancyLedger.View view = ledger.snapshot();
        int limit = properties.perSourceMaxInFlight();
        List<GovernorSnapshot.SourceSnapshot> sourceViews = new ArrayList<>(view.sources().size());
        for (OccupancyLedger.View.SourceView src : view.sources()) {
            // availablePermits() is an independent, read-only gauge; it is not
            // part of the occupancy invariants (a released permit may lag the
            // counter decrement by the few instructions between them).
            Semaphore semaphore = sourcePermits.get(src.source());
            int available = semaphore != null ? semaphore.availablePermits() : limit;
            sourceViews.add(new GovernorSnapshot.SourceSnapshot(
                    src.source(), src.running(), src.queued(), src.inFlight(), limit, available));
        }
        return new GovernorSnapshot(now, view.running(), view.queued(), view.inFlight(),
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
        if (!record.beginRun(() -> ledger.beginRunning(record.getSource()))) {
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
     * terminal status is published. Decrements the ledger then releases permits,
     * so observed termination always matches released occupancy.
     */
    private Runnable releaseResources(TaskRecord record) {
        return () -> {
            String source = record.getSource();
            boolean wasRunning = record.getStartedAt() != null;
            ledger.release(source, wasRunning);
            Semaphore sourceSemaphore = sourcePermits.get(source);
            if (sourceSemaphore != null) {
                sourceSemaphore.release();
            }
            globalPermits.release();
            log.debug("task {} resources released", record.getId());
        };
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
