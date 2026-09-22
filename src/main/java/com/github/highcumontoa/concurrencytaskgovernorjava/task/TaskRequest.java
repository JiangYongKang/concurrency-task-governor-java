package com.github.highcumontoa.concurrencytaskgovernorjava.task;

/**
 * A task submission request.
 *
 * @param idempotencyKey caller-supplied key; resubmissions with the same key
 *                       return the original task instead of executing again
 * @param source         logical caller identity used for fair-share limiting
 * @param type           task type resolved against the handler registry
 * @param payload        opaque payload passed to the handler
 * @param timeoutMs      deadline budget in milliseconds, measured from submission;
 *                       {@code null} means the configured default is used
 */
public record TaskRequest(String idempotencyKey, String source, String type,
                          String payload, Long timeoutMs) {
}
