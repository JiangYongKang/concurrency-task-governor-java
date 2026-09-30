package com.github.highcumontoa.concurrencytaskgovernorjava.task;

import java.time.Instant;

/**
 * One immutable entry in a task's ordered lifecycle history.
 *
 * <p>Skeleton: fields/shape only; production is filled in incrementally.
 *
 * @param sequence      0-based position in occurrence order
 * @param status        status reached by this event ({@code QUEUED} = accepted,
 *                      {@code RUNNING} = execution started, any terminal status = finished)
 * @param failureReason normalized reason for terminal events; {@link FailureReason#NONE} otherwise
 * @param at            instant the transition was observed by the governor
 */
public record TaskEvent(int sequence,
                        TaskStatus status,
                        FailureReason failureReason,
                        Instant at) {
}
