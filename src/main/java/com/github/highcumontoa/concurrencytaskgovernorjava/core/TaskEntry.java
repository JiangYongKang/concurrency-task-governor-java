package com.github.highcumontoa.concurrencytaskgovernorjava.core;

import com.github.highcumontoa.concurrencytaskgovernorjava.domain.FailureReason;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.TaskAction;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.TaskContext;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.TaskSnapshot;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.TaskStatus;

import java.util.concurrent.atomic.AtomicReference;

/**
 * 单个任务的全部可变状态。
 *
 * <p>状态字段通过 {@link AtomicReference} 上的 CAS 迁移，保证终态唯一：
 * 超时、取消与正常完成同时发生时，只有第一个 CAS 成功的一方决定最终状态。</p>
 *
 * <p>本类同时实现 {@link TaskContext}，供任务执行体读取标识、截止时间与取消信号。</p>
 */
final class TaskEntry implements TaskContext {

    private final String taskId;
    private final String idempotencyKey;
    private final String source;
    private final String taskType;
    private final TaskAction action;
    private final long createdAtMillis;
    private final long deadlineAtMillis;
    private final long enqueuedSeq;

    private final AtomicReference<TaskStatus> status = new AtomicReference<>(TaskStatus.QUEUED);

    /** 实际执行任务的工作线程；排队期间为 null，运行开始时设置。 */
    private volatile Thread runner;

    /** 取消请求标志（排队中取消会直接迁移状态；运行中取消会置位并中断工作线程）。 */
    private volatile boolean cancelRequested;

    /** 截止时间守护任务的句柄，终态后用于取消守护任务本身。 */
    private volatile java.util.concurrent.ScheduledFuture<?> deadlineGuard;

    private volatile long startedAtMillis;
    private volatile long finishedAtMillis;
    private volatile int executionCount;
    private volatile FailureReason failureReason = FailureReason.NONE;
    private volatile String failureDetail;
    private volatile String result;

    TaskEntry(String taskId, String idempotencyKey, String source, String taskType,
              TaskAction action, long nowMillis, long deadlineAtMillis, long enqueuedSeq) {
        this.taskId = taskId;
        this.idempotencyKey = idempotencyKey;
        this.source = source;
        this.taskType = taskType;
        this.action = action;
        this.createdAtMillis = nowMillis;
        this.deadlineAtMillis = deadlineAtMillis;
        this.enqueuedSeq = enqueuedSeq;
    }

    // ---------------- 身份与不可变属性（同时是 TaskContext 视图） ----------------

    @Override public String taskId() { return taskId; }

    @Override public String idempotencyKey() { return idempotencyKey; }

    @Override public String source() { return source; }

    String taskType() { return taskType; }

    TaskAction action() { return action; }

    @Override public long deadlineAtMillis() { return deadlineAtMillis; }

    long enqueuedSeq() { return enqueuedSeq; }

    /** 该任务实际被授予的执行预算（毫秒）。 */
    long effectiveTimeoutMillis() {
        return deadlineAtMillis - createdAtMillis;
    }

    @Override
    public long remainingMillis() {
        long remaining = deadlineAtMillis - System.currentTimeMillis();
        return Math.max(remaining, 0L);
    }

    @Override
    public boolean cancellationRequested() {
        return cancelRequested;
    }

    // ---------------- 状态迁移（CAS，终态唯一） ----------------

    TaskStatus status() { return status.get(); }

    boolean isTerminal() { return status.get().isTerminal(); }

    /**
     * 从期望状态 CAS 迁移到目标状态；仅当当前状态等于 expect 时成功。
     */
    boolean casStatus(TaskStatus expect, TaskStatus target) {
        return status.compareAndSet(expect, target);
    }

    Thread runner() {
        return runner;
    }

    void markRunning(Thread worker) {
        this.runner = worker;
        this.startedAtMillis = System.currentTimeMillis();
        this.executionCount = 1;
    }

    void markSucceeded(String result) {
        this.result = result;
        this.finishedAtMillis = System.currentTimeMillis();
    }

    void markFailed(FailureReason reason, String detail) {
        this.failureReason = reason;
        this.failureDetail = detail;
        this.finishedAtMillis = System.currentTimeMillis();
    }

    void requestCancel() {
        this.cancelRequested = true;
        Thread r = runner;
        if (r != null) {
            r.interrupt();
        }
    }

    boolean cancelRequested() {
        return cancelRequested;
    }

    void bindDeadlineGuard(java.util.concurrent.ScheduledFuture<?> guard) {
        this.deadlineGuard = guard;
    }

    void cancelDeadlineGuard() {
        var guard = this.deadlineGuard;
        if (guard != null) {
            guard.cancel(false);
        }
    }

    TaskSnapshot snapshot() {
        return new TaskSnapshot(
                taskId, idempotencyKey, source, taskType,
                status.get(), failureReason, failureDetail, result,
                createdAtMillis, startedAtMillis, finishedAtMillis,
                deadlineAtMillis, executionCount);
    }
}
