package com.github.highcumontoa.concurrencytaskgovernorjava.task;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
 * <p>Capacity model: a global semaphore of {@code maxConcurrency + maxQueueSize}
 * permits bounds total in-flight (queued + running) tasks; a fixed worker pool of
 * {@code maxConcurrency} threads bounds actual parallelism; a per-source semaphore
 * of {@code perSourceMaxInFlight} permits guarantees fair sharing between callers.
 * Permits are released exactly once per task, when the task reaches a terminal
 * state (guarded by {@link #releasedResources}).
 */
@Component
@EnableConfigurationProperties(TaskGovernorProperties.class)
public class DefaultTaskGovernor implements TaskGovernor {

    private static final Logger log = LoggerFactory.getLogger(DefaultTaskGovernor.class);

    private final TaskGovernorProperties properties;
    private final Map<String, TaskHandler> handlersByType;

    private final Map<String, TaskRecord> tasksById = new ConcurrentHashMap<>();
    private final Map<String, String> idByIdempotencyKey = new ConcurrentHashMap<>();
    private final Map<String, Future<?>> runningFutures = new ConcurrentHashMap<>();
    private final Map<String, Semaphore> sourcePermits = new ConcurrentHashMap<>();
    private final Set<String> releasedResources = ConcurrentHashMap.newKeySet();

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
        log.info("task {} accepted: source={} type={} deadline={}",
                record.getId(), record.getSource(), record.getType(), record.getDeadline());

        scheduler.schedule(() -> onDeadline(record.getId()),
                timeoutMs, TimeUnit.MILLISECONDS);
        Future<?> future = workers.submit(() -> execute(record));
        runningFutures.put(record.getId(), future);
        if (record.getStatus().isTerminal()) {
            // Terminal transition (timeout/cancel) raced with future registration.
            future.cancel(true);
        }
        return SubmissionResult.accepted(record);
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
        if (record.transitionTo(TaskStatus.CANCELLED)) {
            record.setFailureReason(FailureReason.CANCELLED);
            log.info("task {} cancelled by caller (was not terminal)", id);
            Future<?> future = runningFutures.get(id);
            if (future != null) {
                future.cancel(true);
            }
            releaseResources(record);
        }
        return Optional.of(record);
    }

    private void onDeadline(String taskId) {
        TaskRecord record = tasksById.get(taskId);
        if (record == null) {
            return;
        }
        if (record.transitionTo(TaskStatus.TIMED_OUT)) {
            record.setFailureReason(FailureReason.TIMEOUT);
            log.info("task {} timed out (deadline {} reached)", taskId, record.getDeadline());
            Future<?> future = runningFutures.get(taskId);
            if (future != null) {
                future.cancel(true);
            }
            releaseResources(record);
        }
    }

    private void execute(TaskRecord record) {
        String taskId = record.getId();
        if (!record.transitionTo(TaskStatus.RUNNING)) {
            // Cancelled or timed out while still queued; never execute.
            log.info("task {} skipped execution, already terminal: {}", taskId, record.getStatus());
            releaseResources(record);
            return;
        }
        log.info("task {} running", taskId);
        try {
            TaskHandler handler = handlersByType.get(record.getType());
            if (handler == null) {
                if (record.transitionTo(TaskStatus.FAILED)) {
                    record.setFailureReason(FailureReason.UNSUPPORTED_TYPE);
                }
                return;
            }
            String result = handler.execute(record.getPayload());
            if (record.transitionTo(TaskStatus.SUCCEEDED)) {
                record.setResult(result);
                log.info("task {} succeeded", taskId);
            } else {
                log.info("task {} finished but lost race to terminal state {}", taskId, record.getStatus());
            }
        } catch (InterruptedException interrupted) {
            // Interrupt always follows a terminal transition (cancel/timeout);
            // this fallback only covers an unlikely ordering race.
            Thread.currentThread().interrupt();
            if (record.transitionTo(TaskStatus.CANCELLED)) {
                record.setFailureReason(FailureReason.CANCELLED);
            }
            log.info("task {} interrupted, final state {}", taskId, record.getStatus());
        } catch (Throwable throwable) {
            if (record.transitionTo(TaskStatus.FAILED)) {
                record.setFailureReason(FailureReason.HANDLER_ERROR);
                log.warn("task {} failed: {}", taskId, throwable.getClass().getSimpleName());
            } else {
                log.info("task {} threw after terminal state {}: {}",
                        taskId, record.getStatus(), throwable.getClass().getSimpleName());
            }
        } finally {
            runningFutures.remove(taskId);
            releaseResources(record);
        }
    }

    /** Releases capacity permits exactly once per task. */
    private void releaseResources(TaskRecord record) {
        if (releasedResources.add(record.getId())) {
            Semaphore sourceSemaphore = sourcePermits.get(record.getSource());
            if (sourceSemaphore != null) {
                sourceSemaphore.release();
            }
            globalPermits.release();
            log.debug("task {} resources released", record.getId());
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
