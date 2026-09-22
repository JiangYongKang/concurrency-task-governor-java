package com.github.highcumontoa.concurrencytaskgovernorjava.web;

/**
 * 提交任务请求体（骨架）。
 */
public record SubmitTaskRequest(
        String source,
        String idempotencyKey,
        String taskType,
        String payload,
        Long timeoutMillis) {
}
