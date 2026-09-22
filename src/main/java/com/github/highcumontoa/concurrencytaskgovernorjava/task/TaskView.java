package com.github.highcumontoa.concurrencytaskgovernorjava.task;

import java.time.Instant;

/**
 * Caller-facing, serializable view of a task. Never carries raw exception data.
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
                       Instant finishedAt) {

    public static TaskView of(TaskRecord r) {
        return new TaskView(r.getId(), r.getIdempotencyKey(), r.getSource(), r.getType(),
                r.getStatus(), r.getFailureReason(), r.getResult(),
                r.getSubmittedAt(), r.getDeadline(), r.getFinishedAt());
    }
}
