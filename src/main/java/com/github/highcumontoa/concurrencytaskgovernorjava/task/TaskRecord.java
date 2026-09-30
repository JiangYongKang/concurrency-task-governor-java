package com.github.highcumontoa.concurrencytaskgovernorjava.task;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Internal state of one governed task.
 *
 * <p><b>Convergence protocol.</b> A terminal <em>claim</em> (timeout, cancel) is
 * distinct from the terminal <em>publication</em>. Exactly one claim can win
 * (first signal wins), but the task does not visibly become terminal until its
 * executor has actually stopped and every resource (global + source permits,
 * occupancy counters) has been released. Therefore: when a caller observes a
 * terminal status, execution has fully stopped and all occupancy is already
 * released, and no later observation can change that.
 *
 * <ul>
 *   <li>A claim while {@link TaskStatus#QUEUED}: the claiming thread finalizes
 *       immediately; the handler never runs.</li>
 *   <li>A claim while {@link TaskStatus#RUNNING}: the worker running the handler
 *       is interrupted and that same worker performs the single finalization
 *       after the handler stops.</li>
 *   <li>No claim before the handler returns: the worker finalizes with the
 *       handler's natural outcome.</li>
 * </ul>
 *
 * <p>All state access is guarded by the record's monitor; the governor performs
 * permit/counter release via the {@code cleanup} callback <em>inside</em> that
 * monitor, immediately before publishing the terminal status and appending the
 * terminal lifecycle event.
 */
public final class TaskRecord {

    /** Result of a terminal-claim race. */
    public enum ClaimOutcome {
        /** Claim won while queued; claimant finalizes, handler must never run. */
        CLAIMED_QUEUED,
        /** Claim won while running; the worker must be interrupted and finalizes. */
        CLAIMED_RUNNING,
        /** A claim already won or the task already converged; signal is ignored. */
        TOO_LATE
    }

    private final String id;
    private final String idempotencyKey;
    private final String source;
    private final String type;
    private final String payload;
    private final Instant submittedAt;
    private final Instant deadline;
    private final List<LifecycleEvent> events = new ArrayList<>();

    private TaskStatus status = TaskStatus.QUEUED;
    private FailureReason failureReason = FailureReason.NONE;
    private String result;
    private Instant startedAt;
    private Instant finishedAt;

    private boolean claimed;
    private TaskStatus claimedStatus;
    private FailureReason claimedReason;
    private Thread runnerThread;
    private boolean converged;

    public TaskRecord(String id, String idempotencyKey, String source, String type,
                      String payload, Instant submittedAt, Instant deadline) {
        this.id = id;
        this.idempotencyKey = idempotencyKey;
        this.source = source;
        this.type = type;
        this.payload = payload;
        this.submittedAt = submittedAt;
        this.deadline = deadline;
        events.add(new LifecycleEvent(submittedAt, null, TaskStatus.QUEUED, FailureReason.NONE, null));
    }

    /**
     * Worker entry: commit QUEUED -&gt; RUNNING. Fails if a terminal claim won
     * first (or the task is already terminal), in which case the handler must
     * never execute. {@code onRunning} runs while holding the record monitor,
     * immediately after the status commit, so RUNNING visibility and running
     * accounting flip together.
     */
    public synchronized boolean beginRun(Runnable onRunning) {
        if (claimed || status != TaskStatus.QUEUED) {
            return false;
        }
        status = TaskStatus.RUNNING;
        startedAt = Instant.now();
        runnerThread = Thread.currentThread();
        onRunning.run();
        events.add(new LifecycleEvent(startedAt, TaskStatus.QUEUED, TaskStatus.RUNNING,
                FailureReason.NONE, null));
        return true;
    }

    /**
     * Race a terminal signal (timeout/cancel) against the execution. The first
     * claim wins; later signals get {@link ClaimOutcome#TOO_LATE}.
     */
    public synchronized ClaimOutcome claimTerminal(TaskStatus claimed, FailureReason reason) {
        if (this.claimed || converged) {
            return ClaimOutcome.TOO_LATE;
        }
        this.claimed = true;
        this.claimedStatus = claimed;
        this.claimedReason = reason;
        return status == TaskStatus.RUNNING ? ClaimOutcome.CLAIMED_RUNNING : ClaimOutcome.CLAIMED_QUEUED;
    }

    /** Thread currently executing the handler, if any; used to deliver the interrupt. */
    public synchronized Thread runnerThread() {
        return runnerThread;
    }

    /**
     * Finalize a task claimed while still queued: handler never runs, so the
     * claimant performs the single finalization. Runs {@code cleanup} (resource
     * release) before the terminal status becomes visible.
     *
     * @return {@code true} if this call performed the finalization
     */
    public synchronized boolean finalizeFromClaim(Runnable cleanup) {
        if (converged) {
            return false;
        }
        if (!claimed) {
            throw new IllegalStateException("finalizeFromClaim without a winning claim: " + id);
        }
        publishTerminal(claimedStatus, claimedReason, null, null, cleanup);
        return true;
    }

    /**
     * Worker-side finalization after the handler stopped (normally or via an
     * interrupt). A winning claim always overrides the handler's natural
     * outcome, so the published conclusion matches what actually happened
     * (handler interrupted vs. never run). Runs {@code cleanup} before the
     * terminal status becomes visible.
     *
     * @return {@code true} if this call performed the finalization
     */
    public synchronized boolean finalizeFromWorker(TaskStatus naturalStatus,
                                                   FailureReason naturalReason,
                                                   String naturalResult,
                                                   String detail,
                                                   Runnable cleanup) {
        if (converged) {
            return false;
        }
        TaskStatus terminal = claimed ? claimedStatus : naturalStatus;
        FailureReason reason = claimed ? claimedReason : naturalReason;
        String value = claimed ? null : naturalResult;
        publishTerminal(terminal, reason, value, detail, cleanup);
        return true;
    }

    private void publishTerminal(TaskStatus terminal, FailureReason reason, String value,
                                 String detail, Runnable cleanup) {
        TaskStatus previous = status;
        // Release all occupancy BEFORE the terminal status is observable, so
        // "terminal observed" implies "fully stopped and fully released".
        cleanup.run();
        status = terminal;
        failureReason = reason;
        if (value != null) {
            result = value;
        }
        finishedAt = Instant.now();
        runnerThread = null;
        converged = true;
        events.add(new LifecycleEvent(finishedAt, previous, terminal, reason, detail));
        notifyAll();
    }

    /** Block until the terminal publication, or the timeout elapses. */
    public synchronized boolean awaitConverged(long timeout, TimeUnit unit) throws InterruptedException {
        long remainingNanos = unit.toNanos(timeout);
        long deadlineNanos = System.nanoTime() + remainingNanos;
        while (!converged && remainingNanos > 0) {
            wait(TimeUnit.NANOSECONDS.toMillis(remainingNanos),
                    (int) (remainingNanos % 1_000_000L));
            remainingNanos = deadlineNanos - System.nanoTime();
        }
        return converged;
    }

    /** Ordered copy of the lifecycle events, safe to hand to callers. */
    public synchronized List<LifecycleEvent> snapshotEvents() {
        return List.copyOf(events);
    }

    public synchronized boolean isConverged() { return converged; }
    public String getId() { return id; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public String getSource() { return source; }
    public String getType() { return type; }
    public String getPayload() { return payload; }
    public Instant getSubmittedAt() { return submittedAt; }
    public Instant getDeadline() { return deadline; }
    public synchronized TaskStatus getStatus() { return status; }
    public synchronized FailureReason getFailureReason() { return failureReason; }
    public synchronized String getResult() { return result; }
    public synchronized Instant getStartedAt() { return startedAt; }
    public synchronized Instant getFinishedAt() { return finishedAt; }
}
