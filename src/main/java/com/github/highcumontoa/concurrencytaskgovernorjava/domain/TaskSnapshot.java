package com.github.highcumontoa.concurrencytaskgovernorjava.domain;

/**
 * 任务某一时刻的不可变状态快照，供查询与测试断言使用。
 *
 * <p>时间戳字段未发生时为 0；{@code failureReason} 在非失败场景为 {@link FailureReason#NONE}；
 * {@code executionCount} 为任务实际开始执行的次数，受控执行器保证其不会超过 1。</p>
 */
public record TaskSnapshot(
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
}
