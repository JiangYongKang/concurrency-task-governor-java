package com.github.highcumontoa.concurrencytaskgovernorjava.domain;

/**
 * 归一化后的失败原因。底层异常不会直接透出给调用方。
 */
public enum FailureReason {
    NONE,
    INVALID_INPUT,
    NOT_FOUND,
    DUPLICATE_SUBMISSION,
    CAPACITY_EXHAUSTED,
    EXECUTION_ERROR,
    TIMEOUT,
    CANCELLED,
    INTERNAL_ERROR,
    SERVICE_SHUTDOWN
}
