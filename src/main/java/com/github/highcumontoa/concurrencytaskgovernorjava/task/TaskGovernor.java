package com.github.highcumontoa.concurrencytaskgovernorjava.task;

import java.util.List;
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
     * <p>Convergence guarantee: when the returned record is terminal, the task's
     * handler has fully stopped (it never ran, or it returned/was interrupted) and
     * every capacity permit the task held has already been released; later queries,
     * cancels or waits can observe no further state or occupancy change.
     *
     * @return the record after the cancel attempt, or empty if unknown id
     */
    Optional<TaskRecord> cancel(String id);

    /**
     * Ordered lifecycle history of one task (accepted -&gt; running -&gt; terminal).
     * The last event's status always equals the task's current terminal status;
     * no event can be appended after the terminal one.
     *
     * @return an immutable copy of the event list, or empty if unknown id
     */
    List<TaskEvent> history(String id);

    /**
     * Current whole-service occupancy. Pure read: never executes a task, never
     * changes any state. When no task is in flight every counter is exactly zero.
     */
    GovernorSnapshot snapshot();

    /** Gracefully stop accepting work and release resources. */
    void shutdown();
}
