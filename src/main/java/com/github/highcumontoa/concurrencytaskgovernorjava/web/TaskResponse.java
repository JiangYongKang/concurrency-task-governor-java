package com.github.highcumontoa.concurrencytaskgovernorjava.web;

import com.github.highcumontoa.concurrencytaskgovernorjava.domain.FailureReason;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.TaskSnapshot;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.TaskStatus;

/**
 * 任务查询响应。由 {@link TaskSnapshot} 映射而来，字段语义与快照一致。
 */
public record TaskResponse(
        String taskId,
        String idempotencyKey,
        String source,
        String taskType,
        TaskStatus status,
        FailureReason failureReason,
        String failureDetail,
        String result,
        long createdAtMillis,
        long startedAtMillis,
        long finishedAtMillis,
        long deadlineAtMillis,
        int executionCount) {

    public static TaskResponse from(TaskSnapshot s) {
        return new TaskResponse(
                s.taskId(), s.idempotencyKey(), s.source(), s.taskType(),
                s.status(), s.failureReason(), s.failureDetail(), s.result(),
                s.createdAtMillis(), s.startedAtMillis(), s.finishedAtMillis(),
                s.deadlineAtMillis(), s.executionCount());
    }
}
