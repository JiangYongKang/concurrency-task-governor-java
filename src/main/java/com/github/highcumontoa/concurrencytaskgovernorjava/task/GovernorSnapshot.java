package com.github.highcumontoa.concurrencytaskgovernorjava.task;

import java.util.List;

/**
 * Point-in-time occupancy of the whole governor. A snapshot is a read-only,
 * internally consistent view: taking it never executes a task or changes state.
 *
 * <p>Skeleton: fields/shape only.
 */
public record GovernorSnapshot(int running,
                               int queued,
                               int totalInFlight,
                               List<SourceOccupancy> sources) {
}
