package com.github.highcumontoa.concurrencytaskgovernorjava.task;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Internal mutable state of one governed task. Status transitions are guarded
 * by a CAS so that racing timeout/cancel/completion signals resolve to exactly
 * one terminal state.
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

    public TaskRecord(String id, String idempotencyKey, String source, String type,
                      String payload, Instant submittedAt, Instant deadline) {
        this.id = id;
        this.idempotencyKey = idempotencyKey;
        this.source = source;
        this.type = type;
        this.payload = payload;
        this.submittedAt = submittedAt;
        this.deadline = deadline;
    }

    /**
     * Attempt a status transition. Only one terminal transition can win.
     *
     * @return {@code true} if this call performed the transition
     */
    public boolean transitionTo(TaskStatus next) {
        while (true) {
            TaskStatus current = status.get();
            if (current.isTerminal()) {
                return false;
            }
            if (next == TaskStatus.RUNNING && current != TaskStatus.QUEUED) {
                return false;
            }
            if (status.compareAndSet(current, next)) {
                if (next.isTerminal()) {
                    finishedAt = Instant.now();
                }
                return true;
            }
        }
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

    void setFailureReason(FailureReason failureReason) { this.failureReason = failureReason; }
    void setResult(String result) { this.result = result; }
}
