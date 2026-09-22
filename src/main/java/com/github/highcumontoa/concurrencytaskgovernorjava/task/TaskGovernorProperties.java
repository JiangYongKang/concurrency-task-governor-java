package com.github.highcumontoa.concurrencytaskgovernorjava.task;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Tuning knobs for the task governor.
 *
 * @param maxConcurrency      maximum number of tasks executing in parallel
 * @param maxQueueSize        maximum number of tasks waiting for a worker slot
 * @param perSourceMaxInFlight maximum in-flight (queued + running) tasks per source
 * @param defaultTimeoutMs    default deadline budget when the request omits one
 * @param maxTimeoutMs        upper bound for caller-supplied timeout values
 */
@ConfigurationProperties(prefix = "task-governor")
public record TaskGovernorProperties(int maxConcurrency,
                                     int maxQueueSize,
                                     int perSourceMaxInFlight,
                                     long defaultTimeoutMs,
                                     long maxTimeoutMs) {

    public TaskGovernorProperties {
        if (maxConcurrency <= 0) {
            maxConcurrency = 4;
        }
        if (maxQueueSize < 0) {
            maxQueueSize = 0;
        }
        if (perSourceMaxInFlight <= 0) {
            perSourceMaxInFlight = maxConcurrency + maxQueueSize;
        }
        if (defaultTimeoutMs <= 0) {
            defaultTimeoutMs = 30_000L;
        }
        if (maxTimeoutMs <= 0) {
            maxTimeoutMs = 300_000L;
        }
    }
}
