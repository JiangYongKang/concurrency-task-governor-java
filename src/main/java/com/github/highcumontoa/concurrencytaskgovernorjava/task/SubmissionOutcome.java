package com.github.highcumontoa.concurrencytaskgovernorjava.task;

/**
 * Distinguishable outcomes of a task submission attempt.
 */
public enum SubmissionOutcome {
    /** Task accepted and queued for execution. */
    ACCEPTED,
    /** Same idempotency key was already submitted; the existing task is returned, no re-execution. */
    DUPLICATE,
    /** Input failed validation; nothing was created. */
    REJECTED_INVALID,
    /** Global capacity (running + queued) is exhausted; nothing was created. */
    REJECTED_CAPACITY,
    /** The submitting source exceeded its fair-share in-flight limit; nothing was created. */
    REJECTED_SOURCE_LIMIT
}
