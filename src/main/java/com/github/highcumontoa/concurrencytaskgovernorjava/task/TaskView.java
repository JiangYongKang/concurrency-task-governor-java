package com.github.highcumontoa.concurrencytaskgovernorjava.task;

import java.time.Instant;
import java.util.List;

/**
 * Caller-facing, serializable view of a task. Never carries raw exception data.
 *
 * @param events ordered lifecycle from acceptance to the terminal event; once
 *               the task is terminal the list is complete and never grows again
 */
public record TaskView(String id,
                       String idempotencyKey,
                       String source,
                       String type,
                       TaskStatus status,
                       FailureReason failureReason,
                       String result,
                       Instant submittedAt,
                       Instant deadline,
                       Instant finishedAt,
                       List<LifecycleEvent> events) {

    public static TaskView of(TaskRecord r) {
        return new TaskView(r.getId(), r.getIdempotencyKey(), r.getSource(), r.getType(),
                r.getStatus(), r.getFailureReason(), r.getResult(),
                r.getSubmittedAt(), r.getDeadline(), r.getFinishedAt(),
                r.snapshotEvents());
    }
}
