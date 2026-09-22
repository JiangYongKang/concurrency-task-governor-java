package com.github.highcumontoa.concurrencytaskgovernorjava.task;

/**
 * Executes a task of a given type. Implementations must be interrupt-aware:
 * a {@link InterruptedException} or a cleared interrupt flag signals
 * cancellation/timeout and must abort execution promptly.
 */
public interface TaskHandler {

    /** The task type this handler supports. */
    String type();

    /**
     * Execute the task.
     *
     * @param payload opaque payload from the request, may be {@code null}
     * @return an optional human-readable result summary
     * @throws Exception any failure; normalized to {@link FailureReason#HANDLER_ERROR}
     */
    String execute(String payload) throws Exception;
}
