package com.github.highcumontoa.concurrencytaskgovernorjava.task;

/**
 * Lifecycle states of a governed task.
 *
 * <p>Transitions: QUEUED -&gt; RUNNING -&gt; (SUCCEEDED | FAILED | TIMED_OUT | CANCELLED).
 * A queued task may also move directly to CANCELLED or TIMED_OUT (deadline exceeded
 * before execution). Terminal states are SUCCEEDED, FAILED, TIMED_OUT, CANCELLED and
 * are reached exactly once.
 */
public enum TaskStatus {
    QUEUED,
    RUNNING,
    SUCCEEDED,
    FAILED,
    TIMED_OUT,
    CANCELLED;

    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == TIMED_OUT || this == CANCELLED;
    }
}
