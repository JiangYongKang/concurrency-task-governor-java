package com.github.highcumontoa.concurrencytaskgovernorjava.domain;

/**
 * 任务生命周期状态。
 *
 * <pre>
 * 提交受理 ──► QUEUED ──► RUNNING ──► SUCCEEDED
 *                │           │
 *                │           ├──────► FAILED
 *                │           ├──────► CANCELLED （运行中收到取消且任务响应中断）
 *                │           └──────► TIMED_OUT  （到达截止时间）
 *                ├──────────────────► CANCELLED （排队中被取消）
 *                └──────────────────► TIMED_OUT  （排队期间到达截止时间）
 * 提交拒绝不产生任务实体（调用方直接得到 REJECTED 结论与 CAPACITY_EXHAUSTED 原因）。
 * </pre>
 * <p>所有终态都不可逆，状态迁移只允许通过比较并设置（CAS）完成，
 * 因此超时 / 取消 / 正常完成并发发生时结论唯一、可解释。</p>
 */
public enum TaskStatus {
    QUEUED,
    RUNNING,
    SUCCEEDED,
    FAILED,
    CANCELLED,
    TIMED_OUT,
    REJECTED;

    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED
                || this == CANCELLED || this == TIMED_OUT || this == REJECTED;
    }
}
