package com.github.highcumontoa.concurrencytaskgovernorjava.web;

/**
 * 提交受理响应（骨架）。
 */
public record SubmitTaskResponse(
        String outcome,
        String message,
        TaskResponse task) {
}
