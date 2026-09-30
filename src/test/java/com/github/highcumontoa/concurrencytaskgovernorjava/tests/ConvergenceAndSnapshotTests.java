package com.github.highcumontoa.concurrencytaskgovernorjava.tests;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.highcumontoa.concurrencytaskgovernorjava.task.DefaultTaskGovernor;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.FailureReason;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.GovernorSnapshot;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.SourceOccupancy;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * New-behavior tests: terminal-visible-means-converged guarantee and occupancy
 * snapshots that reconcile exactly (no phantom/missing counts, exact zero when
 * nothing is in flight).
 */
class ConvergenceAndSnapshotTests {

    private static final Logger log = LoggerFactory.getLogger(ConvergenceAndSnapshotTests.class);

    /** maxConcurrency=2, maxQueueSize=2 -> 4 global slots, 3 per source. */
    private static final TaskGovernorProperties PROPS =
            new TaskGovernorProperties(2, 2, 3, 5_000L, 60_000L);

    private DefaultTaskGovernor governor;

    @AfterEach
    void tearDown() {
        if (governor != null) {
            governor.shutdown();
        }
    }

    /** Blocks until released; records started/stopped/interrupted/executions. */
    static final class TraceHandler implements TaskHandler {
        final AtomicInteger executions = new AtomicInteger();
        final AtomicInteger active = new AtomicInteger();
        final AtomicInteger stopped = new AtomicInteger();
        final AtomicInteger interruptions = new AtomicInteger();
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        @Override
        public String type() {
            return "trace";
        }

        @Override
        public String execute(String payload) throws Exception {
            executions.incrementAndGet();
            active.incrementAndGet();
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException interrupted) {
                interruptions.incrementAndGet();
                Thread.currentThread().interrupt();
                throw interrupted;
            } finally {
                active.decrementAndGet();
                stopped.incrementAndGet();
            }
            return "done";
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

    @Test
    void emptyService_snapshotIsExactlyZero() {
        DefaultTaskGovernor g = newGovernor(new TraceHandler());
        GovernorSnapshot snapshot = g.snapshot();
        log.info("[snapshot-empty] running={} queued={} total={} sources={}",
                snapshot.running(), snapshot.queued(), snapshot.totalInFlight(),
                snapshot.sources().size());
        assertEquals(0, snapshot.running());
        assertEquals(0, snapshot.queued());
        assertEquals(0, snapshot.totalInFlight());
        assertTrue(snapshot.sources().isEmpty(), "no sources must be reported at zero occupancy");

        // Pure read: repeated snapshots stay zero and never start a task.
        for (int i = 0; i < 5; i++) {
            GovernorSnapshot again = g.snapshot();
            assertEquals(0, again.totalInFlight());
        }
    }

    @Test
    void snapshot_countsRunningAndQueuedAndPerSource_andReturnsToZeroAfterConvergence() throws Exception {
        TraceHandler handler = new TraceHandler();
        DefaultTaskGovernor g = newGovernor(handler);

        TaskRecord r1 = g.submit(request("snap-1", "src-a", "trace", null)).record();
        TaskRecord r2 = g.submit(request("snap-2", "src-a", "trace", null)).record();
        TaskRecord r3 = g.submit(request("snap-3", "src-b", "trace", null)).record();

        // Wait until both workers have published RUNNING and the third is queued
        // (handler latches alone only prove one task started).
        GovernorSnapshot busy = null;
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (System.nanoTime() < deadline) {
            busy = g.snapshot();
            if (busy.running() == 2 && busy.queued() == 1) {
                break;
            }
            Thread.sleep(5);
        }
        log.info("[snapshot-busy] running={} queued={} total={} sources={}",
                busy.running(), busy.queued(), busy.totalInFlight(), busy.sources());
        assertEquals(2, busy.running());
        assertEquals(1, busy.queued());
        assertEquals(3, busy.totalInFlight());
        assertEquals(2, busy.sources().size());
        SourceOccupancy a = busy.sources().stream().filter(s -> s.source().equals("src-a")).findFirst().orElseThrow();
        SourceOccupancy b = busy.sources().stream().filter(s -> s.source().equals("src-b")).findFirst().orElseThrow();
        assertEquals(2, a.running());
        assertEquals(0, a.queued());
        assertEquals(2, a.totalInFlight());
        assertEquals(0, b.running());
        assertEquals(1, b.queued());
        assertEquals(1, b.totalInFlight());

        handler.release.countDown();
        for (TaskRecord r : List.of(r1, r2, r3)) {
            awaitTerminal(r, Duration.ofSeconds(5));
        }

        GovernorSnapshot empty = g.snapshot();
        log.info("[snapshot-zero] running={} queued={} total={} sources={} (expect all zero)",
                empty.running(), empty.queued(), empty.totalInFlight(), empty.sources().size());
        assertEquals(0, empty.running());
        assertEquals(0, empty.queued());
        assertEquals(0, empty.totalInFlight());
        assertTrue(empty.sources().isEmpty(), "converged sources must disappear from the snapshot");
    }

    @Test
    void terminalVisibility_meansHandlerAlreadyStopped_andPermitsAlreadyReleased() throws Exception {
        TraceHandler handler = new TraceHandler();
        DefaultTaskGovernor g = newGovernor(handler);

        SubmissionResult slow = g.submit(request("conv-slow", "src-a", "trace", 100L));
        assertEquals(SubmissionOutcome.ACCEPTED, slow.outcome());
        TaskRecord record = slow.record();
        awaitTerminal(record, Duration.ofSeconds(3));

        // The instant the terminal state is observable, the interrupted handler
        // runnable must have fully returned (active == 0, stopped == 1).
        log.info("[converge-timeout] task={} status={} reason={} active={} stopped={} "
                        + "(terminal visibility must imply full stop)",
                record.getId(), record.getStatus(), record.getFailureReason(),
                handler.active.get(), handler.stopped.get());
        assertEquals(TaskStatus.TIMED_OUT, record.getStatus());
        assertEquals(FailureReason.TIMEOUT, record.getFailureReason());
        assertEquals(0, handler.active.get(), "handler must not still be running when terminal is visible");
        assertEquals(1, handler.stopped.get());

        // Capacity permits are released at the same instant: a follow-up fits
        // immediately even without waiting or releasing anything.
        SubmissionResult followUp = g.submit(request("conv-follow", "src-a", "trace", null));
        log.info("[converge-timeout] follow-up after terminal visible -> {} (expect ACCEPTED)",
                followUp.outcome());
        assertEquals(SubmissionOutcome.ACCEPTED, followUp.outcome());

        // Status/occupancy must remain stable under repeated reads and cancels.
        for (int i = 0; i < 3; i++) {
            TaskRecord again = g.cancel(record.getId()).orElseThrow();
            assertEquals(TaskStatus.TIMED_OUT, again.getStatus());
            g.findById(record.getId()).orElseThrow();
        }
        handler.release.countDown();
        awaitTerminal(followUp.record(), Duration.ofSeconds(5));
    }

    @Test
    void concurrentSubmitsCancelsAndTimeouts_snapshotNeverDrifts_andEndsAtZero() throws Exception {
        TraceHandler handler = new TraceHandler();
        DefaultTaskGovernor g = newGovernor(handler);
        int rounds = 30;
        List<TaskRecord> accepted = new ArrayList<>();

        for (int i = 0; i < rounds; i++) {
            // Half the tasks are on a tight timeout; we cancel another half
            // concurrently; submissions from mixed sources race with limits.
            Long timeout = (i % 2 == 0) ? 60L : 5_000L;
            String source = "src-" + (i % 3);
            SubmissionResult result = g.submit(request("mix-" + i, source, "trace", timeout));
            if (result.outcome() == SubmissionOutcome.ACCEPTED) {
                accepted.add(result.record());
                if (i % 2 == 1) {
                    final int idx = i;
                    Thread canceller = new Thread(() -> {
                        try {
                            Thread.sleep((idx % 7) * 2L);
                        } catch (InterruptedException ignored) {
                            Thread.currentThread().interrupt();
                        }
                        g.cancel(result.record().getId());
                    });
                    canceller.setDaemon(true);
                    canceller.start();
                }
            }
        }

        for (TaskRecord record : accepted) {
            awaitTerminal(record, Duration.ofSeconds(10));
            assertTrue(record.getStatus().isTerminal());
        }

        GovernorSnapshot empty = g.snapshot();
        log.info("[mixed] {} accepted tasks converged; snapshot running={} queued={} total={} sources={}",
                accepted.size(), empty.running(), empty.queued(), empty.totalInFlight(),
                empty.sources().size());
        assertEquals(0, empty.running());
        assertEquals(0, empty.queued());
        assertEquals(0, empty.totalInFlight());
        assertTrue(empty.sources().isEmpty());

        long terminal = accepted.stream().filter(r -> r.getStatus().isTerminal()).count();
        assertEquals(accepted.size(), terminal, "every accepted task must converge to one terminal state");
        handler.release.countDown();

        // After every release the service must accept a fresh task: all permits
        // released exactly once, no leak.
        SubmissionResult sanity = g.submit(request("mix-sanity", "src-x", "trace", null));
        assertEquals(SubmissionOutcome.ACCEPTED, sanity.outcome());
        handler.release.countDown();
        awaitTerminal(sanity.record(), Duration.ofSeconds(5));
        assertEquals(0, g.snapshot().totalInFlight(), "final snapshot must be exactly zero");
    }
}
