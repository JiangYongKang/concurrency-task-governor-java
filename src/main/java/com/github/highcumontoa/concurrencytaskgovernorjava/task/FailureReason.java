package com.github.highcumontoa.concurrencytaskgovernorjava.task;

/**
 * Stable, normalized failure reasons. Underlying exceptions are never exposed
 * to callers; they are mapped to one of these values.
 */
public enum FailureReason {
    /** No failure (task not failed or not finished yet). */
    NONE,
    /** The task handler threw an exception. */
    HANDLER_ERROR,
    /** The task exceeded its deadline. */
    TIMEOUT,
    /** The task was cancelled by the caller. */
    CANCELLED,
    /** The task type is not supported by any registered handler. */
    UNSUPPORTED_TYPE
}
