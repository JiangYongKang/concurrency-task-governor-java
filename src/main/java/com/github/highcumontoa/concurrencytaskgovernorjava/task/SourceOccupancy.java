package com.github.highcumontoa.concurrencytaskgovernorjava.task;

/**
 * Occupancy of one submitting {@code source} at a snapshot instant.
 *
 * <p>Skeleton: fields/shape only.
 */
public record SourceOccupancy(String source,
                              int running,
                              int queued,
                              int totalInFlight) {
}
