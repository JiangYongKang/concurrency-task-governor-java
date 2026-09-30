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
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Default {@link TaskGovernor}.
 *
 * <p>Capacity model: a global semaphore of {@code maxConcurrency + maxQueueSize}
 * permits bounds total in-flight (queued + running) tasks; a fixed worker pool of
 * {@code maxConcurrency} threads bounds actual parallelism; a per-source semaphore
 * of {@code perSourceMaxInFlight} permits guarantees fair sharing between callers.
 *
 * <p>Convergence model: timeout, cancel and completion race through
 * {@link TaskRecord#decideTerminal}; exactly one winner exists. The winner first
 * makes the handler stop (it never starts, returns, or is interrupted and
 * returns), then releases the global and per-source permits, and only then
 * publishes the terminal status. Consequently a task observed in a terminal
 * state has already fully ended and given every resource back.
 *
 * <p>Occupancy counters are moved under {@link #snapshotLock} together with the
 * RUNNING/terminal status publication, so {@link #snapshot()} is internally
 * consistent and always reconciles exactly with the outstanding semaphore
 * permits (in-flight = acquired, not yet released).
 */
@Component
@EnableConfigurationProperties(TaskGovernorProperties.class)
public class DefaultTaskGovernor implements TaskGovernor {

    private static final Logger log = LoggerFactory.getLogger(DefaultTaskGovernor.class);

    private final TaskGovernorProperties properties;
    private final Map<String, TaskHandler> handlersByType;

    private final Map<String, TaskRecord> tasksById = new ConcurrentHashMap<>();
    private final Map<String, String> idByIdempotencyKey = new ConcurrentHashMap<>();
    private final Map<String, Semaphore> sourcePermits = new ConcurrentHashMap<>();

    private final Semaphore globalPermits;
    private final ExecutorService workers;
    private final ScheduledExecutorService scheduler;

    // Occupancy counters; every mutation happens while holding snapshotLock and,
    // when a task status is involved, the task record's monitor (lock order:
    // snapshotLock -> TaskRecord).
    private final Lock snapshotLock = new ReentrantLock();
    private int runningCount;
    private int queuedCount;
    private final Map<String, SourceCounters> sourceCounters = new HashMap<>();

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

        long timeoutMs = request.timeoutMs() != null ? request.timeoutMs() : properties.defaultTimeoutMs();
        Instant now = Instant.now();
        TaskRecord record = new TaskRecord(UUID.randomUUID().toString(), request.idempotencyKey(),
                request.source(), request.type(), request.payload(), now, now.plusMillis(timeoutMs));

        String racedId = idByIdempotencyKey.putIfAbsent(request.idempotencyKey(), record.getId());
        if (racedId != null) {
            sourceSemaphore.release();
            globalPermits.release();
            TaskRecord raced = tasksById.get(racedId);
            return SubmissionResult.duplicate(raced != null ? raced : record);
        }
        tasksById.put(record.getId(), record);

        snapshotLock.lock();
        try {
            queuedCount++;
            sourceCounters.computeIfAbsent(request.source(), key -> new SourceCounters()).queued++;
        } finally {
            snapshotLock.unlock();
        }
        log.info("task {} accepted: source={} type={} deadline={}",
                record.getId(), record.getSource(), record.getType(), record.getDeadline());

        scheduler.schedule(() -> onDeadline(record.getId()),
                timeoutMs, TimeUnit.MILLISECONDS);
        Future<?> future = workers.submit(() -> runTask(record));
        record.bindFuture(future);
        return SubmissionResult.accepted(record);
    }

    @Override
    public Optional<TaskRecord> findById(String id) {
        return Optional.ofNullable(tasksById.get(id));
    }

    @Override
    public List<TaskEvent> history(String id) {
        TaskRecord record = tasksById.get(id);
        return record == null ? List.of() : record.historySnapshot();
    }

    @Override
    public GovernorSnapshot snapshot() {
        snapshotLock.lock();
        try {
            List<SourceOccupancy> perSource = new ArrayList<>();
            for (Map.Entry<String, SourceCounters> entry : sourceCounters.entrySet()) {
                SourceCounters counters = entry.getValue();
                int total = counters.running + counters.queued;
                if (total > 0) {
                    perSource.add(new SourceOccupancy(entry.getKey(), counters.running, counters.queued, total));
                }
            }
            perSource.sort((a, b) -> a.source().compareTo(b.source()));
            return new GovernorSnapshot(runningCount, queuedCount,
                    runningCount + queuedCount, List.copyOf(perSource));
        } finally {
            snapshotLock.unlock();
        }
    }

    @Override
    public Optional<TaskRecord> cancel(String id) {
        TaskRecord record = tasksById.get(id);
        if (record == null) {
            return Optional.empty();
        }
        if (record.decideTerminal(TaskStatus.CANCELLED, FailureReason.CANCELLED)) {
            log.info("task {} cancel won terminal decision; interrupting/dismissing worker", id);
            record.interruptIfBound();
            settle(record, TaskStatus.CANCELLED, FailureReason.CANCELLED, null, record.isStarted());
        }
        return Optional.of(record);
    }

    private void onDeadline(String taskId) {
        TaskRecord record = tasksById.get(taskId);
        if (record == null) {
            return;
        }
        if (record.decideTerminal(TaskStatus.TIMED_OUT, FailureReason.TIMEOUT)) {
            log.info("task {} deadline won terminal decision (deadline {}); interrupting/dismissing worker",
                    taskId, record.getDeadline());
            record.interruptIfBound();
            settle(record, TaskStatus.TIMED_OUT, FailureReason.TIMEOUT, null, record.isStarted());
        }
    }

    /** Worker-pool runnable: claims execution, invokes the handler, converges on finish. */
    private void runTask(TaskRecord record) {
        String taskId = record.getId();
        boolean claimed;
        snapshotLock.lock();
        try {
            synchronized (record) {
                if (record.isDecided() || record.isStarted()) {
                    claimed = false;
                } else {
                    record.markRunning();
                    claimed = true;
                    queuedCount--;
                    runningCount++;
                    SourceCounters counters = sourceCounters.get(record.getSource());
                    counters.queued--;
                    counters.running++;
                }
            }
        } finally {
            snapshotLock.unlock();
        }
        if (!claimed) {
            // A terminal decision (cancel/timeout while queued) won first; the
            // handler must never execute.
            log.info("task {} skipped execution, terminal decision already made: {}",
                    taskId, record.getStatus());
            return;
        }

        log.info("task {} running", taskId);
        try {
            TaskHandler handler = handlersByType.get(record.getType());
            if (handler == null) {
                workerConverge(record, TaskStatus.FAILED, FailureReason.UNSUPPORTED_TYPE, null);
                return;
            }
            String handlerResult;
            try {
                handlerResult = handler.execute(record.getPayload());
            } catch (InterruptedException interrupted) {
                // Interrupt normally follows an external timeout/cancel decision;
                // the fallback only covers an interrupt with no decision present.
                Thread.currentThread().interrupt();
                log.info("task {} interrupted with no prior decision; converging to CANCELLED", taskId);
                workerConverge(record, TaskStatus.CANCELLED, FailureReason.CANCELLED, null);
                return;
            } catch (Throwable throwable) {
                log.warn("task {} handler threw: {}", taskId, throwable.getClass().getSimpleName());
                workerConverge(record, TaskStatus.FAILED, FailureReason.HANDLER_ERROR, null);
                return;
            }
            workerConverge(record, TaskStatus.SUCCEEDED, FailureReason.NONE, handlerResult);
        } finally {
            // Signals external decision winners (awaiting in settle()) that the
            // interrupted handler has fully returned. The interrupt flag that
            // Future.cancel(true) leaves even when the worker made the decision
            // must not bleed into the next task run by this pooled thread.
            Thread.interrupted();
            record.signalWorkerDone();
        }
    }

    /** Terminal decision taken from within the worker thread (handler has stopped). */
    private void workerConverge(TaskRecord record, TaskStatus terminal, FailureReason reason, String result) {
        if (record.decideTerminal(terminal, reason)) {
            log.info("task {} worker won terminal decision: {}", record.getId(), terminal);
            settle(record, terminal, reason, result, false);
        } else {
            log.info("task {} handler stopped after losing terminal decision; final state {}",
                    record.getId(), record.getStatus());
        }
    }

    /**
     * Winner-only convergence: wait for the interrupted worker if needed, release
     * permits, then publish the terminal status while the counters move. Terminal
     * visibility therefore implies full stop + released resources.
     */
    private void settle(TaskRecord record, TaskStatus terminal, FailureReason reason,
                        String result, boolean awaitWorker) {
        if (awaitWorker) {
            try {
                record.awaitWorkerDone();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }

        snapshotLock.lock();
        try {
            boolean wasRunning;
            synchronized (record) {
                wasRunning = record.isStarted();
            }
            if (wasRunning) {
                runningCount--;
            } else {
                queuedCount--;
            }
            SourceCounters counters = sourceCounters.get(record.getSource());
            if (counters != null) {
                if (wasRunning) {
                    counters.running--;
                } else {
                    counters.queued--;
                }
                if (counters.running == 0 && counters.queued == 0) {
                    sourceCounters.remove(record.getSource());
                }
            }

            Semaphore sourceSemaphore = sourcePermits.get(record.getSource());
            if (sourceSemaphore != null) {
                sourceSemaphore.release();
            }
            globalPermits.release();

            synchronized (record) {
                record.publishTerminal(terminal, reason, result);
            }
            log.info("task {} converged: status={} reason={} (worker stopped, permits released)",
                    record.getId(), terminal, reason);
        } finally {
            snapshotLock.unlock();
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

    /** Mutable per-source occupancy; accessed only under {@link #snapshotLock}. */
    private static final class SourceCounters {
        private int running;
        private int queued;
    }
}
