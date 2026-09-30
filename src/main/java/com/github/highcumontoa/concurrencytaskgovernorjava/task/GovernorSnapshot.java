package com.github.highcumontoa.concurrencytaskgovernorjava.task;

import java.time.Instant;
import java.util.List;

/**
 * Point-in-time occupancy snapshot of the whole governor. Querying a snapshot
 * never executes tasks nor changes any task state.
 *
 * @param capturedAt      time the snapshot was taken
 * @param running         tasks whose handlers are currently executing
 * @param queued          tasks accepted but not yet running
 * @param inFlight        running + queued
 * @param globalCapacity  total global in-flight capacity
 * @param globalAvailable free global capacity at capture time
 * @param sources         per-source occupancy breakdown
 */
public record GovernorSnapshot(Instant capturedAt,
                               int running,
                               int queued,
                               int inFlight,
                               int globalCapacity,
                               int globalAvailable,
                               List<SourceSnapshot> sources) {

    /**
     * Occupancy of one logical caller source.
     */
    public record SourceSnapshot(String source,
                                 int running,
                                 int queued,
                                 int inFlight,
                                 int limit,
                                 int available) {
    }
}
