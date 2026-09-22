package com.github.highcumontoa.concurrencytaskgovernorjava.domain;

/**
 * 任务执行时可见的上下文：标识、截止时间与取消信号。
 */
public interface TaskContext {

    String taskId();

    String source();

    String idempotencyKey();

    long deadlineAtMillis();

    long remainingMillis();

    boolean cancellationRequested();
}
