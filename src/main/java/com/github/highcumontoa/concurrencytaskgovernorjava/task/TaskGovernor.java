package com.github.highcumontoa.concurrencytaskgovernorjava.task;

import java.util.Optional;

/**
 * Controlled background task execution service. Accepts submissions, enforces
 * concurrency/queue/fair-share limits, applies deadlines and cancellation, and
 * normalizes failures. Skeleton only; behavior is filled in incrementally.
 */
public interface TaskGovernor {

    /**
     * Submit a task. Never throws for caller-facing problems; they are encoded
     * in the returned {@link SubmissionOutcome}.
     */
    SubmissionResult submit(TaskRequest request);

    /** Look up a task by its stable id. */
    Optional<TaskRecord> findById(String id);

    /**
     * Request cancellation. Idempotent: cancelling a terminal task returns the
     * task unchanged.
     *
     * @return the record after the cancel attempt, or empty if unknown id
     */
    Optional<TaskRecord> cancel(String id);

    /**
     * Read-only occupancy snapshot of the whole service: running/queued/
     * in-flight totals and per-source breakdown. Never triggers execution or
     * changes task state.
     */
    GovernorSnapshot snapshot();

    /** Gracefully stop accepting work and release resources. */
    void shutdown();
}
