package com.github.highcumontoa.concurrencytaskgovernorjava.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Authoritative occupancy bookkeeping for {@link DefaultTaskGovernor}.
 *
 * <p>The governor owns admission control (semaphores, idempotency) and task
 * lifecycle; this ledger owns the counters that answer "how many tasks are
 * running / queued / in flight, globally and per source".
 *
 * <p><b>Single-lock linearization.</b> Every counter mutation and every
 * {@link #snapshot()} read run while holding this ledger's monitor. A snapshot
 * is therefore a single linearizable point: its totals and its per-source
 * breakdown are necessarily drawn from one consistent state, never assembled
 * from independent reads that another thread can interleave updates between.
 * This is what makes the per-query invariants unconditional:
 *
 * <ul>
 *   <li>{@code inFlight == running + queued}, both globally and per source;</li>
 *   <li>the per-source {@code inFlight} values sum exactly to the global total;</li>
 *   <li>no counter can be negative and in-flight never exceeds the global
 *       capacity enforced externally by the admission semaphore;</li>
 *   <li>when every accepted task has converged, every counter is exactly zero
 *       and the source breakdown is empty.</li>
 * </ul>
 *
 * <p>Counters are never clamped: admission is serialized per idempotency key
 * (see {@link DefaultTaskGovernor}), so each accepted task contributes exactly
 * one balanced +1/-1 pair ({@link #acceptQueued} paired with
 * {@link #release}, with {@link #beginRunning} in between when it ran). A
 * negative counter would indicate a real accounting bug rather than an
 * observation race.
 *
 * <p>Lock ordering note: callers may invoke the mutators while holding a
 * {@link TaskRecord}'s monitor (terminal cleanup runs inside it). The ledger
 * never calls back into a record, so the lock order is always
 * record-monitor &rarr; ledger-monitor; {@link #snapshot()} takes only the
 * ledger monitor. No deadlock is possible.
 */
final class OccupancyLedger {

    /**
     * Immutable occupancy view captured in one atomic step.
     *
     * @param running  tasks currently executing
     * @param queued   accepted tasks not yet executing
     * @param inFlight running + queued
     * @param sources  per-source breakdown, only sources with in-flight work,
     *                 sorted by source name
     */
    record View(int running, int queued, int inFlight, List<SourceView> sources) {

        /** Per-source occupancy. */
        record SourceView(String source, int running, int queued, int inFlight) {
        }
    }

    /** Per-source counters: {@code [0] = running, [1] = inFlight}. */
    private final Map<String, int[]> bySource = new TreeMap<>();
    private int running;
    private int inFlight;

    OccupancyLedger() {
    }

    /**
     * Account for a newly accepted task: it starts queued. Must be paired with
     * exactly one {@link #release}.
     */
    synchronized void acceptQueued(String source) {
        bySource.computeIfAbsent(source, key -> new int[2])[1]++;
        inFlight++;
    }

    /**
     * Move one accepted task of {@code source} from queued to running. Runs
     * inside the record monitor together with the QUEUED -&gt; RUNNING status
     * commit, so a task can never be observed as running in one place and
     * queued in another.
     */
    synchronized void beginRunning(String source) {
        int[] counters = requireCounters(source);
        counters[0]++;
        running++;
        // inFlight is unchanged: the accepted task simply moves from queued to
        // running (queued is derived as inFlight - running).
    }

    /**
     * Account for one converging task: in-flight always drops by one, and the
     * running counter drops by one iff the task had actually started. This is
     * the exact inverse of {@link #acceptQueued} plus (when it ran)
     * {@link #beginRunning}, so every accepted task converges the counters
     * back precisely once.
     */
    synchronized void release(String source, boolean wasRunning) {
        int[] counters = requireCounters(source);
        counters[1]--;
        inFlight--;
        if (wasRunning) {
            counters[0]--;
            running--;
        }
        pruneIfIdle(source, counters);
    }

    /**
     * Capture a single linearizable occupancy view. Read-only with respect to
     * task state: it neither executes nor mutates any task and consumes no
     * admission quota.
     */
    public synchronized View snapshot() {
        List<View.SourceView> views = new ArrayList<>(bySource.size());
        for (Map.Entry<String, int[]> entry : bySource.entrySet()) {
            int[] counters = entry.getValue();
            int sourceRunning = counters[0];
            int sourceInFlight = counters[1];
            if (sourceInFlight == 0) {
                // Idle entries are normally pruned on release; skip defensively.
                continue;
            }
            views.add(new View.SourceView(entry.getKey(), sourceRunning,
                    sourceInFlight - sourceRunning, sourceInFlight));
        }
        return new View(running, inFlight - running, inFlight, List.copyOf(views));
    }

    private int[] requireCounters(String source) {
        int[] counters = bySource.get(source);
        if (counters == null) {
            // Every release/begin is paired with an accept that created the
            // entry; reaching here means the accounting itself is broken.
            throw new IllegalStateException("occupancy counters missing for source: " + source);
        }
        return counters;
    }

    private void pruneIfIdle(String source, int[] counters) {
        if (counters[0] == 0 && counters[1] == 0) {
            bySource.remove(source);
        }
    }
}
