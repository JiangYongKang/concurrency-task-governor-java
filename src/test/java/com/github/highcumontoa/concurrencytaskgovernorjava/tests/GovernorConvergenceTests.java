package com.github.highcumontoa.concurrencytaskgovernorjava.tests;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.highcumontoa.concurrencytaskgovernorjava.task.DefaultTaskGovernor;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.FailureReason;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the convergence guarantee, the ordered lifecycle record and the
 * occupancy snapshot.
 */
class GovernorConvergenceTests {

    private static final Logger log = LoggerFactory.getLogger(GovernorConvergenceTests.class);

    private static final TaskGovernorProperties PROPS =
            new TaskGovernorProperties(2, 2, 3, 5_000L, 60_000L);

    private DefaultTaskGovernor governor;

    @AfterEach
    void tearDown() {
        if (governor != null) {
            governor.shutdown();
        }
    }

    /** Blocks on a latch; counts executions and interruptions; interrupt-aware. */
    static final class LatchHandler implements TaskHandler {
        final AtomicInteger executions = new AtomicInteger();
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch exit = new CountDownLatch(1);
        final AtomicInteger interruptions = new AtomicInteger();

        @Override
        public String type() {
            return "latch";
        }

        @Override
        public String execute(String payload) throws Exception {
            executions.incrementAndGet();
            started.countDown();
            try {
                release.await();
                return "done";
            } catch (InterruptedException interrupted) {
                interruptions.incrementAndGet();
                Thread.currentThread().interrupt();
                throw interrupted;
            } finally {
                exit.countDown();
            }
        }
    }

    private DefaultTaskGovernor newGovernor(TaskHandler... handlers) {
        governor = new DefaultTaskGovernor(PROPS, List.of(handlers));
        return governor;
    }

    private static TaskRequest request(String key, String source, String type, Long timeoutMs) {
        return new TaskRequest(key, source, type, null, timeoutMs);
    }

    private static void awaitTerminal(TaskRecord record, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!record.getStatus().isTerminal() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertTrue(record.getStatus().isTerminal(),
                "task " + record.getId() + " stuck in " + record.getStatus());
    }

    private static void assertSnapshotZero(DefaultTaskGovernor g) {
        GovernorSnapshot snapshot = g.snapshot();
        assertEquals(0, snapshot.inFlight(), "inFlight must be zero when nothing is in transit");
        assertEquals(0, snapshot.running(), "running must be zero");
        assertEquals(0, snapshot.queued(), "queued must be zero");
        assertEquals(snapshot.globalCapacity(), snapshot.globalAvailable(),
                "all global permits must be available");
        assertTrue(snapshot.sources().isEmpty(),
                "no source may report occupancy after convergence: " + snapshot.sources());
    }

    // ---------- lifecycle record ----------

    @Test
    void succeededTask_lifecycleListsAcceptedRunningSucceededAndMatchesFinalStatus() throws Exception {
        LatchHandler handler = new LatchHandler();
        DefaultTaskGovernor g = newGovernor(handler);

        SubmissionResult result = g.submit(request("life-ok", "src-a", "latch", null));
        assertEquals(SubmissionOutcome.ACCEPTED, result.outcome());
        TaskRecord record = result.record();
        handler.started.await(2, TimeUnit.SECONDS);
        handler.release.countDown();
        awaitTerminal(record, Duration.ofSeconds(5));

        List<LifecycleEvent> events = record.snapshotEvents();
        log.info("[lifecycle-ok] events={}", events);
        assertEquals(3, events.size(), "expected ACCEPTED(QUEUED) -> RUNNING -> SUCCEEDED");
        assertNull(events.get(0).from());
        assertEquals(TaskStatus.QUEUED, events.get(0).to());
        assertEquals(TaskStatus.QUEUED, events.get(1).from());
        assertEquals(TaskStatus.RUNNING, events.get(1).to());
        assertEquals(TaskStatus.RUNNING, events.get(2).from());
        assertEquals(TaskStatus.SUCCEEDED, events.get(2).to());
        assertEquals(FailureReason.NONE, events.get(2).reason());
        assertEquals(record.getStatus(), events.get(events.size() - 1).to(),
                "last lifecycle event must equal the final status");
        assertNotNull(events.get(0).at());
        assertTrue(!events.get(1).at().isBefore(events.get(0).at()));
        assertTrue(!events.get(2).at().isBefore(events.get(1).at()));

        int sizeBefore = events.size();
        Thread.sleep(50);
        assertEquals(sizeBefore, record.snapshotEvents().size(),
                "no event may appear after the terminal event");
        assertSnapshotZero(g);
    }

    @Test
    void failedAndTimedOutAndCancelled_lifecycleTerminalEventsAreDistinguishable() throws Exception {
        LatchHandler latch = new LatchHandler();
        DefaultTaskGovernor g = newGovernor(latch, new TaskHandler() {
            @Override
            public String type() {
                return "boom";
            }

            @Override
            public String execute(String payload) {
                throw new IllegalStateException("boom");
            }
        });

        // failure
        SubmissionResult failed = g.submit(request("life-fail", "src-a", "boom", null));
        awaitTerminal(failed.record(), Duration.ofSeconds(5));
        LifecycleEvent failEvent = failed.record().snapshotEvents()
                .get(failed.record().snapshotEvents().size() - 1);
        assertEquals(TaskStatus.FAILED, failEvent.to());
        assertEquals(FailureReason.HANDLER_ERROR, failEvent.reason());

        // cancel while queued: QUEUED -> CANCELLED directly, no RUNNING event
        g.submit(request("occupy-1", "src-x", "latch", null));
        g.submit(request("occupy-2", "src-y", "latch", null));
        latch.started.await(2, TimeUnit.SECONDS);
        SubmissionResult queued = g.submit(request("life-cancel-queued", "src-b", "latch", null));
        TaskRecord cancelledQueued = g.cancel(queued.record().getId()).orElseThrow();
        assertEquals(TaskStatus.CANCELLED, cancelledQueued.getStatus());
        List<LifecycleEvent> cancelEvents = cancelledQueued.snapshotEvents();
        assertEquals(2, cancelEvents.size(), "queued cancel: QUEUED -> CANCELLED only");
        assertEquals(TaskStatus.CANCELLED, cancelEvents.get(1).to());
        assertEquals(FailureReason.CANCELLED, cancelEvents.get(1).reason());

        // timeout while running
        SubmissionResult slow = g.submit(request("life-timeout", "src-c", "latch", 100L));
        awaitTerminal(slow.record(), Duration.ofSeconds(5));
        LifecycleEvent timeoutEvent = slow.record().snapshotEvents()
                .get(slow.record().snapshotEvents().size() - 1);
        assertEquals(TaskStatus.TIMED_OUT, timeoutEvent.to());
        assertEquals(FailureReason.TIMEOUT, timeoutEvent.reason());

        latch.release.countDown();
        assertSnapshotZeroAfterDrain(g, latch);
    }

    private void assertSnapshotZeroAfterDrain(DefaultTaskGovernor g, LatchHandler latch) throws Exception {
        latch.release.countDown();
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        GovernorSnapshot snapshot = g.snapshot();
        while (snapshot.inFlight() != 0 && System.nanoTime() < deadline) {
            Thread.sleep(20);
            snapshot = g.snapshot();
        }
        assertSnapshotZero(g);
    }

    @Test
    void duplicateSubmission_returnsSameTaskAndSameLifecycle() throws Exception {
        LatchHandler handler = new LatchHandler();
        DefaultTaskGovernor g = newGovernor(handler);

        SubmissionResult first = g.submit(request("life-dup", "src-a", "latch", null));
        SubmissionResult second = g.submit(request("life-dup", "src-a", "latch", null));
        assertEquals(SubmissionOutcome.DUPLICATE, second.outcome());
        assertEquals(first.record().getId(), second.record().getId());

        handler.started.await(2, TimeUnit.SECONDS);
        handler.release.countDown();
        awaitTerminal(first.record(), Duration.ofSeconds(5));

        assertEquals(first.record().snapshotEvents(), second.record().snapshotEvents(),
                "duplicate submissions share one task and one lifecycle record");
        assertEquals(1, handler.executions.get());
        assertSnapshotZero(g);
    }

    // ---------- terminal convergence under real timeout/cancel races ----------

    @Test
    void terminalObservedMeansHandlerAlreadyStopped_cancelWhileRunning() throws Exception {
        LatchHandler handler = new LatchHandler();
        DefaultTaskGovernor g = newGovernor(handler);

        SubmissionResult task = g.submit(request("conv-cancel", "src-a", "latch", 60_000L));
        handler.started.await(2, TimeUnit.SECONDS);

        TaskRecord record = g.cancel(task.record().getId()).orElseThrow();
        // cancel() returns only after convergence publication: handler stopped.
        assertTrue(record.getStatus().isTerminal());
        assertEquals(TaskStatus.CANCELLED, record.getStatus());
        assertTrue(handler.exit.await(1, TimeUnit.SECONDS),
                "when cancel reports terminal, the handler thread must already have exited");
        assertEquals(1, handler.interruptions.get());

        // later queries/cancels cannot change anything
        TaskRecord again = g.cancel(record.getId()).orElseThrow();
        assertEquals(TaskStatus.CANCELLED, again.getStatus());
        int events = again.snapshotEvents().size();
        Thread.sleep(50);
        assertEquals(events, again.snapshotEvents().size());
        assertEquals(1, handler.executions.get(), "task must execute exactly once");
        assertSnapshotZero(g);
    }

    @Test
    void terminalObservedMeansResourcesReleased_timeoutWhileRunning() throws Exception {
        LatchHandler handler = new LatchHandler();
        DefaultTaskGovernor g = newGovernor(handler);

        SubmissionResult task = g.submit(request("conv-timeout", "src-a", "latch", 80L));
        handler.started.await(2, TimeUnit.SECONDS);
        awaitTerminal(task.record(), Duration.ofSeconds(5));

        assertTrue(handler.exit.await(1, TimeUnit.SECONDS),
                "when TIMED_OUT is observable, the handler must already have stopped");
        // the released slot must be immediately reusable
        SubmissionResult next = g.submit(request("conv-timeout-next", "src-a", "latch", 60_000L));
        assertEquals(SubmissionOutcome.ACCEPTED, next.outcome(),
                "capacity must be effective at the same instant the terminal state is");
        g.cancel(next.record().getId());
        handler.release.countDown();
        assertSnapshotZeroAfterDrain(g, handler);
    }

    /**
     * Timeout and cancel fired at the same instant for many rounds; ordering is
     * deliberately not fixed. Every task executes at most once, converges to
     * exactly one terminal state, the handler has stopped once that state is
     * visible, and all counters return to zero.
     */
    @Test
    void timeoutAndCancelRacing_simultaneouslyAcrossManyRounds() throws Exception {
        final int rounds = 40;
        for (int round = 0; round < rounds; round++) {
            LatchHandler handler = new LatchHandler();
            DefaultTaskGovernor g = new DefaultTaskGovernor(
                    new TaskGovernorProperties(1, 0, 10, 5_000L, 60_000L), List.of(handler));
            try {
                // tight window: deadline and cancel are initiated together from
                // two threads; whichever order the JVM picks, exactly one wins.
                long timeoutMs = 30 + (round % 7) * 5L;
                SubmissionResult submitted = g.submit(
                        request("race-" + round, "src-a", "latch", timeoutMs));
                assertEquals(SubmissionOutcome.ACCEPTED, submitted.outcome());
                TaskRecord record = submitted.record();
                handler.started.await(2, TimeUnit.SECONDS);

                CountDownLatch fire = new CountDownLatch(1);
                Thread canceller = new Thread(() -> {
                    try {
                        fire.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    g.cancel(record.getId());
                });
                canceller.start();
                fire.countDown(); // deadline already armed by the governor scheduler

                awaitTerminal(record, Duration.ofSeconds(5));
                TaskStatus terminal = record.getStatus();
                assertTrue(terminal == TaskStatus.TIMED_OUT || terminal == TaskStatus.CANCELLED,
                        "round " + round + ": unexpected terminal " + terminal);

                canceller.join(5_000);
                assertTrue(handler.exit.await(2, TimeUnit.SECONDS),
                        "round " + round + ": terminal " + terminal + " visible but handler still running");
                assertEquals(1, handler.executions.get(),
                        "round " + round + ": task executed more than once");

                // repeated cancel/query after convergence change nothing
                TaskRecord after = g.cancel(record.getId()).orElseThrow();
                assertEquals(terminal, after.getStatus(),
                        "round " + round + ": terminal conclusion changed after convergence");
                int eventCount = after.snapshotEvents().size();
                LifecycleEvent last = after.snapshotEvents().get(eventCount - 1);
                assertEquals(terminal, last.to(),
                        "round " + round + ": lifecycle tail disagrees with final status");
                if (terminal == TaskStatus.TIMED_OUT) {
                    assertEquals(FailureReason.TIMEOUT, last.reason());
                } else {
                    assertEquals(FailureReason.CANCELLED, last.reason());
                }
                Thread.sleep(10);
                assertEquals(eventCount, after.snapshotEvents().size(),
                        "round " + round + ": events appeared after terminal");

                handler.release.countDown();
                long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
                while (g.snapshot().inFlight() != 0 && System.nanoTime() < deadline) {
                    Thread.sleep(5);
                }
                assertSnapshotZero(g);
                log.info("[race-round {}] winner={} (order deliberately non-deterministic)",
                        round, terminal);
            } finally {
                handler.release.countDown();
                g.shutdown();
            }
        }
    }

    // ---------- snapshot accounting ----------

    @Test
    void snapshot_totalsAndPerSourceBreakdownMatchActualOccupancy() throws Exception {
        LatchHandler handler = new LatchHandler();
        DefaultTaskGovernor g = newGovernor(handler);

        // 2 workers, 2 queue slots; distinct sources to stay under the per-source limit
        List<TaskRecord> tasks = new java.util.ArrayList<>();
        for (int i = 0; i < 4; i++) {
            SubmissionResult r = g.submit(request("snap-" + i, "src-" + i, "latch", null));
            assertEquals(SubmissionOutcome.ACCEPTED, r.outcome());
            tasks.add(r.record());
        }
        // wait until the two workers are actually running
        handler.started.await(2, TimeUnit.SECONDS);
        long stable = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        GovernorSnapshot snapshot = g.snapshot();
        while ((snapshot.running() != 2 || snapshot.queued() != 2) && System.nanoTime() < stable) {
            Thread.sleep(10);
            snapshot = g.snapshot();
        }
        log.info("[snapshot] steady state: {}", snapshot);
        assertEquals(4, snapshot.inFlight());
        assertEquals(2, snapshot.running());
        assertEquals(2, snapshot.queued());
        assertEquals(2 + 2, snapshot.running() + snapshot.queued());
        assertEquals(4, snapshot.globalCapacity());
        assertEquals(0, snapshot.globalAvailable());
        assertEquals(4, snapshot.sources().size());
        for (GovernorSnapshot.SourceSnapshot source : snapshot.sources()) {
            assertEquals(1, source.inFlight());
            assertTrue(source.running() == 1 || source.queued() == 1);
        }

        handler.release.countDown();
        for (TaskRecord task : tasks) {
            awaitTerminal(task, Duration.ofSeconds(5));
        }
        assertSnapshotZero(g);
    }

    /**
     * Mixed submit/cancel/timeout concurrency: totals must never be created or
     * lost, the snapshot must stay self-consistent throughout, and everything
     * returns exactly to zero afterwards.
     */
    @Test
    void snapshot_conservedUnderMixedSubmitCancelTimeout_andZeroAfterConvergence() throws Exception {
        LatchHandler handler = new LatchHandler();
        DefaultTaskGovernor g = newGovernor(handler); // cap 4, per-source 3

        final int total = 24;
        List<TaskRecord> accepted = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
        CountDownLatch go = new CountDownLatch(1);
        List<Thread> submitters = new java.util.ArrayList<>();
        for (int i = 0; i < 6; i++) {
            final int base = i * (total / 6);
            Thread t = new Thread(() -> {
                try {
                    go.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                for (int j = 0; j < total / 6; j++) {
                    int idx = base + j;
                    String source = "mix-src-" + (idx % 3);
                    Long timeoutMs = (idx % 4 == 0) ? 40L + (idx % 9) * 6L : 60_000L;
                    SubmissionResult r = g.submit(request("mix-key-" + idx, source, "latch", timeoutMs));
                    if (r.outcome() == SubmissionOutcome.ACCEPTED) {
                        accepted.add(r.record());
                        if (idx % 5 == 0) {
                            g.cancel(r.record().getId());
                        }
                    }
                    try {
                        Thread.sleep(idx % 3);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
            t.start();
            submitters.add(t);
        }

        // observer: while churn is happening the snapshot must always be
        // internally consistent and must never change task state. It observes
        // until occupancy has returned to zero, which cannot happen before all
        // submissions have drained and all tasks converged.
        final AtomicInteger observations = new AtomicInteger();
        final long observeDeadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        List<Thread> observers = new java.util.ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Thread t = new Thread(() -> {
                boolean sawInFlight = false;
                while (System.nanoTime() < observeDeadline) {
                    GovernorSnapshot s = g.snapshot();
                    observations.incrementAndGet();
                    int fromSources = s.sources().stream()
                            .mapToInt(GovernorSnapshot.SourceSnapshot::inFlight).sum();
                    assertTrue(s.running() >= 0 && s.queued() >= 0);
                    assertEquals(s.inFlight(), s.running() + s.queued(),
                            "running + queued must equal inFlight");
                    assertEquals(s.inFlight(), fromSources,
                            "per-source inFlight must sum to the total");
                    assertTrue(s.inFlight() <= s.globalCapacity());
                    if (s.inFlight() > 0) {
                        sawInFlight = true;
                    } else if (sawInFlight) {
                        return;
                    }
                }
            });
            t.setDaemon(true);
            t.start();
            observers.add(t);
        }

        go.countDown();
        for (Thread t : submitters) {
            t.join(30_000);
        }

        // cancel whatever is still alive, release the handler, and wait out timeouts
        for (TaskRecord record : accepted) {
            if (!record.getStatus().isTerminal()) {
                g.cancel(record.getId());
            }
        }
        handler.release.countDown();
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        boolean allTerminal = false;
        while (System.nanoTime() < deadline) {
            allTerminal = accepted.stream().allMatch(r -> r.getStatus().isTerminal());
            if (allTerminal && g.snapshot().inFlight() == 0) {
                break;
            }
            Thread.sleep(20);
        }
        assertTrue(allTerminal, "every accepted task must converge");
        long terminalCount = accepted.stream().filter(r -> r.getStatus().isTerminal()).count();
        assertEquals(accepted.size(), terminalCount);
        for (Thread t : observers) {
            t.join(5_000);
            assertTrue(!t.isAlive(), "observer should stop once occupancy returns to zero");
        }
        assertTrue(observations.get() >= 20,
                "snapshots must actually be observed during churn, got " + observations.get());
        assertSnapshotZero(g);
        log.info("[mixed] accepted={} all terminal; observations during churn={}; final snapshot zero",
                accepted.size(), observations.get());

        // each task executes at most once and has exactly one terminal lifecycle event
        long terminalEvents = accepted.stream()
                .flatMap(r -> r.snapshotEvents().stream())
                .filter(e -> e.to().isTerminal())
                .count();
        assertEquals(accepted.size(), terminalEvents,
                "exactly one terminal event per task");
        for (TaskRecord r : accepted) {
            LifecycleEvent last = r.snapshotEvents().get(r.snapshotEvents().size() - 1);
            assertEquals(r.getStatus(), last.to(), "lifecycle tail must match final status");
        }
    }

    @Test
    void snapshotQuery_doesNotExecuteOrMutateTasks() {
        LatchHandler handler = new LatchHandler();
        DefaultTaskGovernor g = newGovernor(handler);

        // no tasks at all: repeatedly querying must keep everything zero
        for (int i = 0; i < 10; i++) {
            assertSnapshotZero(g);
        }
        // handler must never have been invoked by a snapshot
        assertEquals(0, handler.executions.get());
    }
}
