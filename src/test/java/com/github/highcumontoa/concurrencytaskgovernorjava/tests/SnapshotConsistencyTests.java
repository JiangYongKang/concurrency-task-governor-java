package com.github.highcumontoa.concurrencytaskgovernorjava.tests;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.highcumontoa.concurrencytaskgovernorjava.task.DefaultTaskGovernor;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.GovernorSnapshot;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.LifecycleEvent;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.SubmissionOutcome;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.SubmissionResult;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.TaskGovernorProperties;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.TaskHandler;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.TaskRecord;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.TaskRequest;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.TaskStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deterministic stress coverage for the point-in-time self-consistency of
 * {@link GovernorSnapshot}: under sustained concurrent submit / cancel /
 * timeout churn with a tiny capacity, every single snapshot taken mid-flight
 * must already be self-consistent (never "eventually consistent after a few
 * queries"), and occupancy must return exactly to zero once everything
 * converges.
 *
 * <p>The churn deliberately maximises counter flips: hundreds of very short
 * tasks (1-10ms of interruptible work) constantly transition QUEUED -&gt;
 * RUNNING -&gt; terminal, some deadlines fire while queued or running, and
 * caller cancels race both. With split/non-atomic accounting this reproduces
 * torn snapshots (running+queued != in-flight, per-source sums mismatching the
 * total, negative queued) on essentially every run.
 */
class SnapshotConsistencyTests {

    private static final Logger log = LoggerFactory.getLogger(SnapshotConsistencyTests.class);

    /** 3 workers + 3 queue slots = global capacity 6; per-source limit 3. */
    private static final TaskGovernorProperties PROPS =
            new TaskGovernorProperties(3, 3, 3, 60_000L, 60_000L);

    private DefaultTaskGovernor governor;

    @AfterEach
    void tearDown() {
        if (governor != null) {
            governor.shutdown();
        }
    }

    /** Interrupt-aware handler that works for a few milliseconds then returns. */
    static final class SpinHandler implements TaskHandler {
        final AtomicInteger executions = new AtomicInteger();
        final AtomicInteger interruptions = new AtomicInteger();

        @Override
        public String type() {
            return "spin";
        }

        @Override
        public String execute(String payload) throws InterruptedException {
            executions.incrementAndGet();
            long workMs = payload == null ? 2L : Long.parseLong(payload);
            long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(workMs);
            try {
                while (System.nanoTime() < until) {
                    Thread.sleep(1); // wakes promptly on interrupt
                }
                return "done";
            } catch (InterruptedException interrupted) {
                interruptions.incrementAndGet();
                Thread.currentThread().interrupt();
                throw interrupted;
            }
        }
    }

    /** Handler that blocks until released; used by the read-only test. */
    static final class BlockedHandler implements TaskHandler {
        final CountDownLatch startedAll;
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger entered = new AtomicInteger();

        BlockedHandler(int expectedRunning) {
            this.startedAll = new CountDownLatch(expectedRunning);
        }

        @Override
        public String type() {
            return "blocked";
        }

        @Override
        public String execute(String payload) throws InterruptedException {
            entered.incrementAndGet();
            startedAll.countDown();
            try {
                release.await();
                return "done";
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw interrupted;
            }
        }
    }

    private static TaskRequest request(String key, String source, String type,
                                       String payload, Long timeoutMs) {
        return new TaskRequest(key, source, type, payload, timeoutMs);
    }

    private static void checkSnapshot(GovernorSnapshot s, int sourceLimit,
                                      ConcurrentLinkedQueue<String> violations) {
        int capacity = 3 + 3;
        if (s.running() < 0) {
            violations.add("negative running: " + s);
        }
        if (s.queued() < 0) {
            violations.add("negative queued: " + s);
        }
        if (s.inFlight() < 0) {
            violations.add("negative inFlight: " + s);
        }
        if (s.inFlight() != s.running() + s.queued()) {
            violations.add("running(" + s.running() + ") + queued(" + s.queued()
                    + ") != inFlight(" + s.inFlight() + ")");
        }
        if (s.inFlight() > s.globalCapacity() || s.globalCapacity() != capacity) {
            violations.add("inFlight/capacity out of bounds: " + s);
        }
        if (s.globalAvailable() != s.globalCapacity() - s.inFlight() || s.globalAvailable() < 0) {
            violations.add("globalAvailable inconsistent: " + s);
        }

        int srcRunning = 0;
        int srcInFlight = 0;
        for (GovernorSnapshot.SourceSnapshot src : s.sources()) {
            if (src.running() < 0 || src.queued() < 0 || src.inFlight() < 0) {
                violations.add("negative source counter: " + src);
            }
            if (src.inFlight() != src.running() + src.queued()) {
                violations.add("source '" + src.source() + "' running+queued != inFlight: " + src);
            }
            if (src.running() > src.inFlight()) {
                violations.add("source '" + src.source() + "' running exceeds inFlight: " + src);
            }
            if (src.limit() != sourceLimit) {
                violations.add("source '" + src.source() + "' limit changed: " + src);
            }
            if (src.available() != src.limit() - src.inFlight() || src.available() < 0) {
                violations.add("source '" + src.source() + "' available inconsistent: " + src);
            }
            if (src.inFlight() > src.limit()) {
                violations.add("source '" + src.source() + "' over its limit: " + src);
            }
            srcRunning += src.running();
            srcInFlight += src.inFlight();
        }
        if (srcInFlight != s.inFlight()) {
            violations.add("per-source inFlight sum (" + srcInFlight
                    + ") != total inFlight (" + s.inFlight() + ")");
        }
        if (srcRunning != s.running()) {
            violations.add("per-source running sum (" + srcRunning
                    + ") != total running (" + s.running() + ")");
        }
    }

    private static void assertZeroSnapshot(DefaultTaskGovernor g) {
        GovernorSnapshot s = g.snapshot();
        assertEquals(0, s.running(), "running must be exactly zero: " + s);
        assertEquals(0, s.queued(), "queued must be exactly zero: " + s);
        assertEquals(0, s.inFlight(), "inFlight must be exactly zero: " + s);
        assertEquals(s.globalCapacity(), s.globalAvailable(),
                "all global permits must be available: " + s);
        assertTrue(s.sources().isEmpty(), "no source may remain in the breakdown: " + s);
    }

    /**
     * Multi-round mixed churn: many submitters drive short tasks through a tiny
     * capacity while short deadlines and caller cancels race them; observers
     * validate every snapshot taken throughout, then everything must converge
     * exactly once and occupancy must return precisely to zero.
     */
    @Test
    void everySnapshotIsSelfConsistentDuringMixedChurn_andZeroAfterwards() throws Exception {
        final int rounds = 6;
        final int tasksPerRound = 240;
        final int submitterThreads = 6;
        final int observerThreads = 4;
        final int sources = 5;
        final int perWorker = tasksPerRound / submitterThreads;

        for (int round = 0; round < rounds; round++) {
            SpinHandler handler = new SpinHandler();
            governor = new DefaultTaskGovernor(PROPS, List.of(handler));
            ConcurrentLinkedQueue<String> violations = new ConcurrentLinkedQueue<>();
            AtomicInteger observations = new AtomicInteger();
            List<TaskRecord> accepted = java.util.Collections.synchronizedList(new ArrayList<>());
            AtomicInteger acceptedCount = new AtomicInteger();
            AtomicInteger rejectedCount = new AtomicInteger();
            AtomicInteger duplicateCount = new AtomicInteger();

            final int roundBase = round * 100_000;
            CountDownLatch go = new CountDownLatch(1);
            java.util.concurrent.atomic.AtomicBoolean submittersDone =
                    new java.util.concurrent.atomic.AtomicBoolean();

            List<Thread> submitters = new ArrayList<>();
            for (int t = 0; t < submitterThreads; t++) {
                final int base = t * perWorker;
                Thread thread = new Thread(() -> {
                    await(go);
                    for (int j = 0; j < perWorker; j++) {
                        int idx = roundBase + base + j;
                        String source = "churn-src-" + (idx % sources);
                        // ~1/3 of tasks get a tight deadline (may expire queued or
                        // running); others live long enough to complete or be cancelled.
                        Long timeoutMs = (idx % 3 == 0) ? 2L + (idx % 6) : null;
                        String payload = Long.toString(1L + (idx % 10)); // 1-10ms work
                        SubmissionResult r = governor.submit(
                                request("churn-key-" + idx, source, "spin", payload, timeoutMs));
                        if (r.outcome() == SubmissionOutcome.ACCEPTED) {
                            acceptedCount.incrementAndGet();
                            accepted.add(r.record());
                            if (idx % 4 == 0) {
                                // race an external cancel against timeout/completion
                                governor.cancel(r.record().getId());
                            }
                        } else if (r.outcome() == SubmissionOutcome.DUPLICATE) {
                            duplicateCount.incrementAndGet();
                        } else {
                            rejectedCount.incrementAndGet();
                            // Tiny pacing on rejection keeps the churn alive
                            // long enough for running/queued/timeout flips to
                            // interleave with the observers, instead of the
                            // whole burst ending while everything is rejected.
                            try {
                                Thread.sleep(1L + (idx % 3));
                            } catch (InterruptedException interrupted) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                        }
                    }
                }, "submitter-" + t);
                thread.setDaemon(true);
                thread.start();
                submitters.add(thread);
            }

            List<Thread> observers = new ArrayList<>();
            for (int t = 0; t < observerThreads; t++) {
                Thread thread = new Thread(() -> {
                    await(go);
                    while (true) {
                        GovernorSnapshot s = governor.snapshot();
                        observations.incrementAndGet();
                        checkSnapshot(s, PROPS.perSourceMaxInFlight(), violations);
                        if (submittersDone.get() && s.inFlight() == 0) {
                            return;
                        }
                    }
                }, "observer-" + t);
                thread.setDaemon(true);
                thread.start();
                observers.add(thread);
            }

            go.countDown();
            for (Thread t : submitters) {
                t.join(60_000);
                assertFalse(t.isAlive(), "submitter thread hung");
            }
            submittersDone.set(true);

            // Drain: cancel anything still alive; short-deadline tasks converge
            // on their own via the scheduler.
            long drainDeadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < drainDeadline) {
                for (TaskRecord record : accepted) {
                    if (!record.getStatus().isTerminal()) {
                        governor.cancel(record.getId());
                    }
                }
                boolean allTerminal = accepted.stream().allMatch(r -> r.getStatus().isTerminal());
                if (allTerminal && governor.snapshot().inFlight() == 0) {
                    break;
                }
                Thread.sleep(5);
            }

            for (Thread t : observers) {
                t.join(10_000);
                assertFalse(t.isAlive(), "observer thread should stop after occupancy hit zero");
            }

            assertTrue(accepted.stream().allMatch(r -> r.getStatus().isTerminal()),
                    "round " + round + ": every accepted task must converge");
            assertZeroSnapshot(governor);
            assertTrue(observations.get() >= 500,
                    "round " + round + ": observers must sample heavily during churn, got "
                            + observations.get());
            assertTrue(violations.isEmpty(),
                    "round " + round + ": inconsistent snapshots observed:\n  "
                            + String.join("\n  ", violations.stream().limit(10).toList()));

            // Each task converges exactly once: exactly one terminal event whose
            // target equals the final status, and it executes at most once.
            for (TaskRecord record : accepted) {
                List<LifecycleEvent> events = record.snapshotEvents();
                long terminalEvents = events.stream().filter(e -> e.to().isTerminal()).count();
                assertEquals(1, terminalEvents,
                        "round " + round + " task " + record.getId() + ": expected one terminal event");
                assertEquals(record.getStatus(), events.get(events.size() - 1).to(),
                        "round " + round + " task " + record.getId() + ": lifecycle tail mismatch");
                TaskStatus status = record.getStatus();
                if (status != TaskStatus.CANCELLED && status != TaskStatus.TIMED_OUT
                        && status != TaskStatus.SUCCEEDED && status != TaskStatus.FAILED) {
                    throw new AssertionError("unexpected final status " + status);
                }
            }
            assertTrue(handler.executions.get() <= accepted.size(),
                    "a task must never execute more than once");
            log.info("[snapshot-stress round {}] accepted={} rejected={} duplicate={} "
                            + "executions={} interruptions={} observations={} violations=0",
                    round, acceptedCount.get(), rejectedCount.get(), duplicateCount.get(),
                    handler.executions.get(), handler.interruptions.get(), observations.get());

            governor.shutdown();
            governor = null;
        }
    }

    /**
     * Snapshot queries are strictly read-only: with workers fully blocked they
     * must neither start tasks, change any task status, nor move a single
     * counter or permit; a queued task must stay QUEUED across many queries.
     */
    @Test
    void snapshotIsReadOnly_doesNotExecuteOrMutateOrConsumeCapacity() throws Exception {
        BlockedHandler handler = new BlockedHandler(PROPS.maxConcurrency());
        governor = new DefaultTaskGovernor(PROPS, List.of(handler));

        List<TaskRecord> records = new ArrayList<>();
        for (int i = 0; i < PROPS.maxConcurrency() + PROPS.maxQueueSize(); i++) {
            SubmissionResult r = governor.submit(request(
                    "ro-key-" + i, "ro-src-" + i, "blocked", null, 60_000L));
            assertEquals(SubmissionOutcome.ACCEPTED, r.outcome());
            records.add(r.record());
        }
        assertTrue(handler.startedAll.await(5, TimeUnit.SECONDS), "workers should saturate");

        // Snapshot repeatedly: nothing may move while every handler is blocked.
        GovernorSnapshot first = governor.snapshot();
        assertEquals(PROPS.maxConcurrency(), first.running());
        assertEquals(PROPS.maxQueueSize(), first.queued());
        assertEquals(PROPS.maxConcurrency() + PROPS.maxQueueSize(), first.inFlight());
        for (int i = 0; i < 50; i++) {
            GovernorSnapshot s = governor.snapshot();
            assertEquals(first.running(), s.running());
            assertEquals(first.queued(), s.queued());
            assertEquals(first.inFlight(), s.inFlight());
            assertEquals(first.globalAvailable(), s.globalAvailable());
            assertEquals(first.sources().size(), s.sources().size());
            // A snapshot must never move a task's status: the same 3 stay
            // RUNNING and the same 3 stay QUEUED throughout the read loop.
            long running = records.stream().filter(r -> r.getStatus() == TaskStatus.RUNNING).count();
            long queued = records.stream().filter(r -> r.getStatus() == TaskStatus.QUEUED).count();
            assertEquals(PROPS.maxConcurrency(), running, "a snapshot changed RUNNING membership");
            assertEquals(PROPS.maxQueueSize(), queued, "a snapshot changed QUEUED membership");
            checkSnapshot(s, PROPS.perSourceMaxInFlight(), new ConcurrentLinkedQueue<>());
        }
        assertEquals(PROPS.maxConcurrency(), handler.entered.get(),
                "snapshots must not start additional executions");

        // Released capacity is unchanged by the read-only queries: a new submit
        // while full is still rejected for capacity.
        SubmissionResult extra = governor.submit(
                request("ro-extra", "ro-extra", "blocked", null, 60_000L));
        assertEquals(SubmissionOutcome.REJECTED_CAPACITY, extra.outcome());

        for (TaskRecord record : records) {
            governor.cancel(record.getId());
        }
        handler.release.countDown();
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (governor.snapshot().inFlight() != 0 && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertZeroSnapshot(governor);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
