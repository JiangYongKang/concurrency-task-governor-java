package com.github.highcumontoa.concurrencytaskgovernorjava.domain;

/**
 * 提交结果：受理结论 + 当前任务快照 + 可解释说明。
 */
public record SubmitResult(
        SubmitOutcome outcome,
        TaskSnapshot snapshot,
        String message) {
}
