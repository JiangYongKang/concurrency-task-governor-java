package com.github.highcumontoa.concurrencytaskgovernorjava.web;

import com.github.highcumontoa.concurrencytaskgovernorjava.domain.FailureReason;

/**
 * 统一错误响应体：稳定的失败原因码 + 简短可解释说明。底层异常不会出现在响应中。
 */
public record ErrorResponse(
        FailureReason reason,
        String message,
        long timestampMillis) {
}
