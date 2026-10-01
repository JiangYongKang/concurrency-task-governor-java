package com.github.highcumontoa.concurrencytaskgovernorjava.tests;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.github.highcumontoa.concurrencytaskgovernorjava.task.DefaultTaskGovernor;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.GovernorSnapshot;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.SubmissionOutcome;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.SubmissionResult;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.TaskGovernorProperties;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.TaskHandler;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.TaskRecord;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.TaskRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * High-density stress coverage for the per-query snapshot self-consistency
 * guarantee.
 *
 * <p>Across many independent rounds, submit / cancel / timeout are driven
 * concurrently while several observers spin on {@code snapshot()} and validate
 * <em>every single</em> returned snapshot. If the counters were assembled from
 * independent non-atomic reads (the original bug), one of these tight-loop
 * observations deterministically catches a torn view: negative queued count,
 * running+queued != inFlight, per-source sums != total, or inFlight over
 * capacity. The test fails immediately on the first such observation rather
 * than relying on timing luck.
 */
class SnapshotConsistencyStressTests {

    private static final Logger log = LoggerFactory.getLogger(SnapshotConsistencyStressTests.class);

    private DefaultTaskGovernor governor;

    @AfterEach
    void tearDown() {
        if (governor != null) {
            governor.shutdown();
        }
    }

    /** Completes immediately, freeing running slots naturally and fast. */
    static final class NoopHandler implements TaskHandler {
        @Override
        public String type() {
            return "stress-noop";
        }

        @Override
        public String execute(String payload) {
            return "ok";
        }
    }

    /** Blocks on a latch; interrupt-aware (so timeout/cancel converge quickly). */
    static final class LatchHandler implements TaskHandler {
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch exit = new CountDownLatch(1);

        @Override
        public String type() {
            return "stress-latch";
        }

        @Override
        public String execute(String payload) throws Exception {
            started.countDown();
            try {
                release.await();
                return "done";
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw interrupted;
            } finally {
                exit.countDown();
            }
        }
    }

    private static TaskRequest request(String key, String source, String type, long timeoutMs) {
        return new TaskRequest(key, source, type, null, timeoutMs);
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static void assertSelfConsistent(GovernorSnapshot s) {
        assertTrue(s.running() >= 0, "running must never be negative: " + s);
        assertTrue(s.queued() >= 0, "queued must never be negative: " + s);
        assertTrue(s.inFlight() >= 0, "inFlight must never be negative: " + s);
        assertEquals(s.inFlight(), s.running() + s.queued(),
                "running + queued must equal inFlight in every snapshot: " + s);
        assertTrue(s.inFlight() <= s.globalCapacity(),
                "inFlight must never exceed global capacity: " + s);
        assertTrue(s.running() <= s.inFlight(),
                "running cannot exceed inFlight: " + s);

        int sourceInFlight = 0;
        int sourceRunning = 0;
        int sourceQueued = 0;
        for (GovernorSnapshot.SourceSnapshot src : s.sources()) {
            assertTrue(src.running() >= 0 && src.queued() >= 0 && src.inFlight() >= 0,
                    "per-source counters must never be negative: " + src);
            assertEquals(src.inFlight(), src.running() + src.queued(),
                    "per-source running + queued must equal per-source inFlight: " + src);
            assertTrue(src.inFlight() <= src.limit(),
                    "per-source inFlight must not exceed its limit: " + src);
            sourceInFlight += src.inFlight();
            sourceRunning += src.running();
            sourceQueued += src.queued();
        }
        assertEquals(s.inFlight(), sourceInFlight,
                "sum of per-source inFlight must equal the total inFlight: " + s);
        assertEquals(s.running(), sourceRunning,
                "sum of per-source running must equal the total running: " + s);
        assertEquals(s.queued(), sourceQueued,
                "sum of per-source queued must equal the total queued: " + s);
        assertTrue(s.globalAvailable() >= 0 && s.globalAvailable() <= s.globalCapacity(),
                "globalAvailable out of range: " + s);
    }

    @Test
    void everySnapshotIsSelfConsistent_throughoutRepeatedMixedSubmitCancelTimeout() throws Exception {
        final int rounds = 8;
        final int submitterThreads = 6;
        final int observerThreads = 3;
        final int sources = 6; // stay under the per-source limit while the 4 global slots churn
        final long churnMsPerRound = 800;

        final AtomicInteger totalObservations = new AtomicInteger();

        for (int round = 0; round < rounds; round++) {
            final int roundId = round;
            LatchHandler handler = new LatchHandler();
            // cap 4 (2 running + 2 queue), per-source limit high enough that only
            // the global cap can reject; short round-dependent timeouts force
            // frequent TIMED_OUT claims alongside explicit cancels.
            governor = new DefaultTaskGovernor(
                    new TaskGovernorProperties(2, 2, 8, 5_000L, 60_000L),
                    List.of(handler, new NoopHandler()));
            DefaultTaskGovernor g = governor;

            final List<TaskRecord> accepted = java.util.Collections.synchronizedList(
                    new java.util.ArrayList<TaskRecord>());
            final List<String> acceptedKeys = java.util.Collections.synchronizedList(
                    new java.util.ArrayList<String>());
            final AtomicInteger acceptedCount = new AtomicInteger();
            final AtomicInteger capacityRejections = new AtomicInteger();
            final AtomicInteger sourceRejections = new AtomicInteger();
            final AtomicInteger duplicates = new AtomicInteger();
            final AtomicReference<Throwable> firstFailure = new AtomicReference<>();
            final AtomicBoolean observing = new AtomicBoolean(true);
            final CountDownLatch start = new CountDownLatch(1);
            final long churnDeadline = System.nanoTime()
                    + Duration.ofMillis(churnMsPerRound).toNanos();
            final AtomicInteger sequence = new AtomicInteger();

            // Observers spin as tightly as possible (no sleeps) to maximise the
            // chance of landing inside an update window; every snapshot is
            // validated and the first violation fails the whole test.
            List<Thread> observers = new java.util.ArrayList<>();
            for (int o = 0; o < observerThreads; o++) {
                Thread t = new Thread(() -> {
                    int local = 0;
                    while (observing.get() || g.snapshot().inFlight() != 0) {
                        GovernorSnapshot snapshot = g.snapshot();
                        local++;
                        try {
                            assertSelfConsistent(snapshot);
                        } catch (Throwable failure) {
                            firstFailure.compareAndSet(null, failure);
                            observing.set(false);
                            return;
                        }
                    }
                    totalObservations.addAndGet(local);
                }, "snapshot-observer-" + o);
                t.setDaemon(true);
                t.start();
                observers.add(t);
            }

            // Submitters keep submitting (paced) for the whole churn window while
            // cancels and timeouts constantly free slots again: accept, cancel,
            // timeout and capacity-rejection all overlap continuously. A fraction
            // reuses a previously accepted key, which deterministically exercises
            // the DUPLICATE path.
            List<Thread> submitters = new java.util.ArrayList<>();
            for (int i = 0; i < submitterThreads; i++) {
                Thread t = new Thread(() -> {
                    awaitQuietly(start);
                    while (System.nanoTime() < churnDeadline) {
                        int idx = sequence.getAndIncrement();
                        String key;
                        String source;
                        if (idx % 8 == 7 && !acceptedKeys.isEmpty()) {
                            int pick = idx % acceptedKeys.size();
                            key = acceptedKeys.get(pick);
                            source = "stress-src-" + Math.floorMod(key.hashCode(), sources);
                        } else {
                            key = "round-" + roundId + "-key-" + idx;
                            source = "stress-src-" + (idx % sources);
                        }
                        // About half the tasks complete immediately (freeing
                        // slots naturally); the rest block and churn via
                        // timeout/cancel/interrupt.
                        String type = (idx % 2 == 0) ? "stress-noop" : "stress-latch";
                        long timeoutMs = (idx % 3 == 0) ? 8L + (idx % 8) * 3L : 60_000L;
                        SubmissionResult result = g.submit(request(key, source, type, timeoutMs));
                        switch (result.outcome()) {
                            case ACCEPTED -> {
                                acceptedCount.incrementAndGet();
                                accepted.add(result.record());
                                acceptedKeys.add(key);
                                if ("stress-latch".equals(type) && idx % 2 == 1) {
                                    g.cancel(result.record().getId());
                                }
                            }
                            case DUPLICATE -> duplicates.incrementAndGet();
                            case REJECTED_CAPACITY -> capacityRejections.incrementAndGet();
                            case REJECTED_SOURCE_LIMIT -> sourceRejections.incrementAndGet();
                            default -> throw new AssertionError("unexpected outcome "
                                    + result.outcome() + ": " + result.detail());
                        }
                        try {
                            // Pace the burst so released slots are reused and the
                            // four operations genuinely interleave for the whole
                            // window instead of slamming the cap once.
                            Thread.sleep(1L);
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }, "submitter-" + i);
                t.start();
                submitters.add(t);
            }

            start.countDown();
            for (Thread t : submitters) {
                t.join(30_000);
                assertTrue(!t.isAlive(), "submitter thread did not finish in round " + round);
            }

            // Drain: cancel everything still alive, release blocked handlers and
            // wait for every accepted task to converge.
            for (TaskRecord record : accepted) {
                if (!record.getStatus().isTerminal()) {
                    g.cancel(record.getId());
                }
            }
            handler.release.countDown();
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (System.nanoTime() < deadline) {
                boolean allTerminal = accepted.stream().allMatch(r -> r.getStatus().isTerminal());
                if (allTerminal && g.snapshot().inFlight() == 0) {
                    break;
                }
                Thread.sleep(5);
            }
            assertTrue(accepted.stream().allMatch(r -> r.getStatus().isTerminal()),
                    "round " + round + ": not every accepted task converged");

            // Stop the observers only after occupancy demonstrably hit zero; the
            // final zero snapshot must be self-consistent and fully zeroed.
            observing.set(false);
            for (Thread t : observers) {
                t.join(10_000);
                assertTrue(!t.isAlive(), "observer did not stop after drain in round " + round);
            }
            if (firstFailure.get() != null) {
                throw new AssertionError(
                        "round " + round + ": observed an inconsistent snapshot",
                        firstFailure.get());
            }

            GovernorSnapshot zero = g.snapshot();
            assertSelfConsistent(zero);
            assertEquals(0, zero.inFlight(), "round " + round + ": inFlight must be exactly zero");
            assertEquals(0, zero.running(), "round " + round + ": running must be exactly zero");
            assertEquals(0, zero.queued(), "round " + round + ": queued must be exactly zero");
            assertTrue(zero.sources().isEmpty(),
                    "round " + round + ": no source may report occupancy: " + zero.sources());
            assertEquals(zero.globalCapacity(), zero.globalAvailable(),
                    "round " + round + ": all global permits must be available at zero");

            long terminalEvents = accepted.stream()
                    .flatMap(r -> r.snapshotEvents().stream())
                    .filter(e -> e.to().isTerminal())
                    .count();
            assertEquals(acceptedCount.get(), terminalEvents,
                    "round " + round + ": exactly one terminal event per accepted task");
            assertTrue(acceptedCount.get() >= 50,
                    "round " + round + ": churn must recycle slots into many accepts, got "
                            + acceptedCount.get());
            assertTrue(duplicates.get() > 0,
                    "round " + round + ": repeated keys must produce duplicates");
            assertTrue(capacityRejections.get() > 0,
                    "round " + round + ": cap pressure must still produce capacity rejections");

            log.info("[snapshot-stress round {}] accepted={} duplicates={} capacityRejected={} "
                            + "sourceRejected={} allConverged=true finalZero=true",
                    round, acceptedCount.get(), duplicates.get(), capacityRejections.get(),
                    sourceRejections.get());

            handler.release.countDown();
            g.shutdown();
        }

        assertTrue(totalObservations.get() >= 50_000,
                "observers must actually validate a large number of snapshots, got "
                        + totalObservations.get());
        log.info("[snapshot-stress] all {} rounds consistent; total validated snapshots={}",
                rounds, totalObservations.get());
    }
}
