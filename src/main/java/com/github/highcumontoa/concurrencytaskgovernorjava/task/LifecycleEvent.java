package com.github.highcumontoa.concurrencytaskgovernorjava.task;

import java.time.Instant;

/**
 * One immutable entry in a task's ordered lifecycle record.
 *
 * <p>Events are appended in occurrence order and never appended after the task
 * has converged to a terminal state.
 *
 * @param at     time the event occurred
 * @param from   status before the event ({@code null} for the acceptance event)
 * @param to     status after the event
 * @param reason normalized failure reason associated with the event
 * @param detail optional caller-safe explanation
 */
public record LifecycleEvent(Instant at,
                             TaskStatus from,
                             TaskStatus to,
                             FailureReason reason,
                             String detail) {
}
