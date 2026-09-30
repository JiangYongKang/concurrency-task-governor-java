package com.github.highcumontoa.concurrencytaskgovernorjava.task;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Internal state of one governed task.
 *
 * <p>Two-phase terminal protocol, both phases guarded by this record's monitor:
 * <ol>
 *   <li><b>decision</b> ({@link #decideTerminal}) — racing timeout / cancel /
 *       completion signals contend; exactly one wins and chooses the terminal
 *       status. Status is still non-terminal at this point.</li>
 *   <li><b>publication</b> ({@link #publishTerminal}) — performed by the winner
 *       only after the worker has actually stopped (or was guaranteed never to
 *       start) and every capacity permit has been released. This is the single
 *       instant at which a terminal status becomes observable, so "terminal is
 *       visible" implies "execution ended and resources are already given back".</li>
 * </ol>
 *
 * <p>The {@link #events} list is append-only, ordered, and shares the same
 * publication points, so it can never contain a status the task did not reach
 * and can never grow after the terminal event.
 */
public final class TaskRecord {

    private final String id;
    private final String idempotencyKey;
    private final String source;
    private final String type;
    private final String payload;
    private final Instant submittedAt;
    private final Instant deadline;

    private final AtomicReference<TaskStatus> status = new AtomicReference<>(TaskStatus.QUEUED);
    private volatile FailureReason failureReason = FailureReason.NONE;
    private volatile String result;
    private volatile Instant finishedAt;

    private final CopyOnWriteArrayList<TaskEvent> events = new CopyOnWriteArrayList<>();

    // Terminal-decision bookkeeping; all four fields are guarded by 'this'.
    private boolean decided;
    private boolean started;
    private Future<?> boundFuture;

    /** Released exactly once, after the worker runnable returns (started tasks only). */
    private final CountDownLatch workerDone = new CountDownLatch(1);

    public TaskRecord(String id, String idempotencyKey, String source, String type,
                      String payload, Instant submittedAt, Instant deadline) {
        this.id = id;
        this.idempotencyKey = idempotencyKey;
        this.source = source;
        this.type = type;
        this.payload = payload;
        this.submittedAt = submittedAt;
        this.deadline = deadline;
        this.events.add(new TaskEvent(0, TaskStatus.QUEUED, FailureReason.NONE, submittedAt));
    }

    /**
     * Publish the QUEUED -&gt; RUNNING transition. Caller must hold this record's
     * monitor and must have already verified the task was neither decided nor
     * started; occupancy counters are updated by the caller at the same instant.
     */
    void markRunning() {
        started = true;
        status.compareAndSet(TaskStatus.QUEUED, TaskStatus.RUNNING);
        appendEvent(TaskStatus.RUNNING, FailureReason.NONE);
    }

    /** Interrupt/dismiss the bound worker after a terminal decision (idempotent). */
    synchronized void interruptIfBound() {
        if (boundFuture != null) {
            boundFuture.cancel(true);
        }
    }

    /**
     * Atomically decide the task's terminal outcome. Exactly one racing signal
     * wins. The winner is responsible for stopping the worker, releasing capacity
     * and calling {@link #publishTerminal}.
     *
     * @return {@code true} if this call is the unique terminal decision
     */
    public synchronized boolean decideTerminal(TaskStatus terminal, FailureReason reason) {
        if (decided) {
            return false;
        }
        decided = true;
        if (boundFuture != null) {
            boundFuture.cancel(true);
        }
        return true;
    }

    /** Bind the worker future after submission; a task already decided is cancelled at once. */
    public synchronized boolean bindFuture(Future<?> future) {
        if (decided) {
            future.cancel(true);
            return false;
        }
        this.boundFuture = future;
        return true;
    }

    public synchronized boolean isDecided() {
        return decided;
    }

    public synchronized boolean isStarted() {
        return started;
    }

    /** Block until the worker runnable of a started task has fully returned. */
    public void awaitWorkerDone() throws InterruptedException {
        workerDone.await();
    }

    /** Invoked by the worker wrapper in {@code finally}, exactly once. */
    public void signalWorkerDone() {
        workerDone.countDown();
    }

    /**
     * Publish the unique terminal transition: finalize fields, flip status and
     * append the terminal event as one indivisible observer-visible step.
     * Must be called by the decision winner only, after convergence bookkeeping.
     */
    public synchronized void publishTerminal(TaskStatus terminal, FailureReason reason, String taskResult) {
        if (status.get().isTerminal()) {
            return; // defensive: publication happens at most once
        }
        this.failureReason = reason;
        if (terminal == TaskStatus.SUCCEEDED) {
            this.result = taskResult;
        }
        Instant at = Instant.now();
        this.finishedAt = at;
        status.set(terminal);
        appendEvent(terminal, reason, at);
    }

    private void appendEvent(TaskStatus eventStatus, FailureReason reason) {
        appendEvent(eventStatus, reason, Instant.now());
    }

    private void appendEvent(TaskStatus eventStatus, FailureReason reason, Instant at) {
        events.add(new TaskEvent(events.size(), eventStatus, reason, at));
    }

    /** Immutable copy of the lifecycle history in occurrence order. */
    public List<TaskEvent> historySnapshot() {
        return List.copyOf(events);
    }

    public String getId() { return id; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public String getSource() { return source; }
    public String getType() { return type; }
    public String getPayload() { return payload; }
    public Instant getSubmittedAt() { return submittedAt; }
    public Instant getDeadline() { return deadline; }
    public TaskStatus getStatus() { return status.get(); }
    public FailureReason getFailureReason() { return failureReason; }
    public String getResult() { return result; }
    public Instant getFinishedAt() { return finishedAt; }
}
