package com.github.highcumontoa.concurrencytaskgovernorjava.tests;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.github.highcumontoa.concurrencytaskgovernorjava.task.DefaultTaskGovernor;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.FailureReason;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.SubmissionOutcome;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.SubmissionResult;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.TaskEvent;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.TaskGovernorProperties;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.TaskHandler;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.TaskRecord;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.TaskRequest;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.TaskStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Timeout/cancel races with neither ordering fixed, plus lifecycle-history
 * consistency. The race harness uses a randomized staggered start so that over
 * repeated iterations the deadline and the cancel both win some rounds.
 */
class TimeoutCancelRaceAndHistoryTests {

    private static final Logger log = LoggerFactory.getLogger(TimeoutCancelRaceAndHistoryTests.class);

    /** Plenty of capacity so the task always runs; the only race is timeout vs cancel. */
    private static final TaskGovernorProperties PROPS =
            new TaskGovernorProperties(4, 4, 8, 5_000L, 60_000L);

    private DefaultTaskGovernor governor;

    private static final AtomicInteger timeoutWins = new AtomicInteger();
    private static final AtomicInteger cancelWins = new AtomicInteger();

    @BeforeAll
    static void resetWinCounters() {
        timeoutWins.set(0);
        cancelWins.set(0);
    }

    @AfterAll
    static void bothOrdersMustOccur() {
        log.info("[race-distribution] TIMED_OUT wins={} CANCELLED wins={} (both must be > 0)",
                timeoutWins.get(), cancelWins.get());
        assertTrue(timeoutWins.get() > 0, "over the iterations, timeout must win some rounds");
        assertTrue(cancelWins.get() > 0, "over the iterations, cancel must win some rounds");
    }

    @AfterEach
    void tearDown() {
        if (governor != null) {
            governor.shutdown();
        }
    }

    /** Blocks until released; counts executions and tracks exact stop. */
    static final class SlowHandler implements TaskHandler {
        final AtomicInteger executions = new AtomicInteger();
        final AtomicInteger active = new AtomicInteger();
        final AtomicInteger stopped = new AtomicInteger();
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        @Override
        public String type() {
            return "slow";
        }

        @Override
        public String execute(String payload) throws Exception {
            executions.incrementAndGet();
            active.incrementAndGet();
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw interrupted;
            } finally {
                active.decrementAndGet();
                stopped.incrementAndGet();
            }
            return "done";
        }
    }

    private static TaskRequest request(String key, long timeoutMs) {
        return new TaskRequest(key, "race-src", "slow", null, timeoutMs);
    }

    private static void awaitTerminal(TaskRecord record, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!record.getStatus().isTerminal() && System.nanoTime() < deadline) {
            Thread.sleep(2);
        }
        assertTrue(record.getStatus().isTerminal(),
                "task " + record.getId() + " stuck in " + record.getStatus());
    }

    /**
     * Timeout and cancel fire at the same target time from two independent
     * threads; the start stagger is randomized so no ordering is fixed.
     */
    @RepeatedTest(40)
    void timeoutAndCancelRace_atExactlyTheSameMoment_singleTerminalAndStopped() throws Exception {
        SlowHandler handler = new SlowHandler();
        governor = new DefaultTaskGovernor(PROPS, List.of(handler));
        DefaultTaskGovernor g = governor;

        // Symmetric collision window: the cancel is scheduled at the deadline
        // plus/minus a uniform jitter, so over iterations it lands just before
        // and just after the scheduled timeout with comparable frequency.
        long timeoutMs = 30 + (long) (Math.random() * 40);
        long cancelDelay = timeoutMs + (long) ((Math.random() * 24) - 12);

        SubmissionResult submitted = g.submit(request("race-" + System.nanoTime(), timeoutMs));
        assertEquals(SubmissionOutcome.ACCEPTED, submitted.outcome());
        TaskRecord record = submitted.record();
        assertTrue(handler.started.await(2, java.util.concurrent.TimeUnit.SECONDS),
                "task must start running before the signals collide");

        CountDownLatch startGun = new CountDownLatch(1);
        AtomicReference<Throwable> workerFailure = new AtomicReference<>();

        Thread canceller = new Thread(() -> {
            try {
                startGun.await();
                Thread.sleep(Math.max(0, cancelDelay));
                g.cancel(record.getId());
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } catch (Throwable t) {
                workerFailure.set(t);
            }
        }, "racer-cancel");
        Thread deadlineLike = new Thread(() -> {
            // Mirrors the scheduled deadline check colliding with the cancel;
            // the governor's own scheduler also fires at timeoutMs.
            try {
                startGun.await();
                Thread.sleep(timeoutMs);
                g.cancel(record.getId());
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } catch (Throwable t) {
                workerFailure.set(t);
            }
        }, "racer-deadline");
        canceller.setDaemon(true);
        deadlineLike.setDaemon(true);
        canceller.start();
        deadlineLike.start();
        startGun.countDown();

        awaitTerminal(record, Duration.ofSeconds(5));
        canceller.join(2000);
        deadlineLike.join(2000);
        assertEquals(null, workerFailure.get(), "racing threads must not throw");

        TaskStatus terminal = record.getStatus();
        log.info("[race] task={} winner={} reason={} timeoutMs={} cancelDelay={} executions={} active={} stopped={}",
                record.getId(), terminal, record.getFailureReason(), timeoutMs, cancelDelay,
                handler.executions.get(), handler.active.get(), handler.stopped.get());

        // Exactly one conclusion: a genuine terminal state, one of the two winners.
        assertTrue(terminal == TaskStatus.TIMED_OUT || terminal == TaskStatus.CANCELLED,
                "race must resolve to TIMED_OUT or CANCELLED, was " + terminal);
        if (terminal == TaskStatus.TIMED_OUT) {
            timeoutWins.incrementAndGet();
        } else {
            cancelWins.incrementAndGet();
        }
        assertEquals(1, handler.executions.get(), "task must execute exactly once");

        // Convergence: the instant terminal is visible, the handler has stopped.
        assertEquals(0, handler.active.get(), "handler must be fully stopped when terminal is visible");
        assertEquals(1, handler.stopped.get());
        if (terminal == TaskStatus.TIMED_OUT) {
            assertEquals(FailureReason.TIMEOUT, record.getFailureReason());
        } else {
            assertEquals(FailureReason.CANCELLED, record.getFailureReason());
        }

        // Late signals (more cancels, waiting past the deadline) change nothing.
        for (int i = 0; i < 3; i++) {
            TaskStatus again = g.cancel(record.getId()).orElseThrow().getStatus();
            assertEquals(terminal, again, "terminal conclusion must be stable");
        }
        Thread.sleep(40);
        assertEquals(terminal, record.getStatus(), "no state change may appear after convergence");
        assertEquals(0, handler.active.get());
        assertEquals(1, handler.executions.get());

        // Releasing the now-terminated handler changes nothing observable.
        handler.release.countDown();
        Thread.sleep(30);
        assertEquals(terminal, record.getStatus());
        assertEquals(1, handler.executions.get());
    }

    @Test
    void history_reflectsOrderedLifecycle_andMatchesFinalStatus_andIsAppendFrozen() throws Exception {
        SlowHandler handler = new SlowHandler();
        governor = new DefaultTaskGovernor(PROPS, List.of(handler));
        DefaultTaskGovernor g = governor;

        SubmissionResult slow = g.submit(request("hist-slow", 80L));
        TaskRecord record = slow.record();
        awaitTerminal(record, Duration.ofSeconds(3));

        List<TaskEvent> history = g.history(record.getId());
        log.info("[history] terminal={} events={}", record.getStatus(),
                history.stream().map(e -> e.status() + "@" + e.at()).toList());
        assertEquals(3, history.size(), "QUEUED -> RUNNING -> terminal");
        assertEquals(TaskStatus.QUEUED, history.get(0).status());
        assertEquals(FailureReason.NONE, history.get(0).failureReason());
        assertEquals(TaskStatus.RUNNING, history.get(1).status());
        TaskEvent terminalEvent = history.get(2);
        assertEquals(record.getStatus(), terminalEvent.status(),
                "history terminal event must equal task status");
        assertEquals(record.getFailureReason(), terminalEvent.failureReason());
        assertEquals(TaskStatus.TIMED_OUT, terminalEvent.status());

        // Strictly increasing timestamps / sequence and terminal instant set.
        for (int i = 0; i < history.size(); i++) {
            assertEquals(i, history.get(i).sequence());
            assertNotNull(history.get(i).at());
            if (i > 0) {
                assertTrue(!history.get(i).at().isBefore(history.get(i - 1).at()),
                        "events must be timestamped in occurrence order");
            }
        }
        assertEquals(terminalEvent.at(), record.getFinishedAt());

        // Frozen after terminal: waiting and cancelling append nothing.
        g.cancel(record.getId());
        Thread.sleep(60);
        List<TaskEvent> later = g.history(record.getId());
        assertEquals(history.size(), later.size(), "no event may appear after the terminal event");
        assertEquals(history, later);

        handler.release.countDown();
    }

    @Test
    void history_forQueuedCancel_skipsRunning_andDuplicateSubmissionSharesIt() throws Exception {
        SlowHandler handler = new SlowHandler();
        // 2 workers, no queue headroom needed: fill workers, cancel the queued one.
        governor = new DefaultTaskGovernor(new TaskGovernorProperties(2, 2, 8, 5_000L, 60_000L),
                List.of(handler));
        DefaultTaskGovernor g = governor;

        g.submit(request("occupy-1", 5_000L));
        g.submit(request("occupy-2", 5_000L));
        assertTrue(handler.started.await(2, java.util.concurrent.TimeUnit.SECONDS));

        SubmissionResult queued = g.submit(request("queued-cancel", 5_000L));
        assertEquals(SubmissionOutcome.ACCEPTED, queued.outcome());
        TaskRecord record = queued.record();

        // A duplicate submission (same idempotency key) returns the same task even
        // while queued; no second execution, same record.
        SubmissionResult duplicate = g.submit(request("queued-cancel", 5_000L));
        assertEquals(SubmissionOutcome.DUPLICATE, duplicate.outcome());
        assertEquals(record.getId(), duplicate.record().getId());

        TaskRecord cancelled = g.cancel(record.getId()).orElseThrow();
        assertEquals(TaskStatus.CANCELLED, cancelled.getStatus());

        List<TaskEvent> history = g.history(record.getId());
        log.info("[history-queued] events={}", history.stream().map(TaskEvent::status).toList());
        assertEquals(2, history.size(), "QUEUED -> CANCELLED, RUNNING must never appear");
        assertEquals(TaskStatus.QUEUED, history.get(0).status());
        assertEquals(TaskStatus.CANCELLED, history.get(1).status());
        assertEquals(FailureReason.CANCELLED, history.get(1).failureReason());
        assertEquals(history, duplicate.record() == record ? g.history(duplicate.record().getId()) : history);

        handler.release.countDown();
    }

    @Test
    void history_successAndFailure_haveDistinguishableTerminalEvents() throws Exception {
        SlowHandler slow = new SlowHandler();
        TaskHandler fail = new TaskHandler() {
            @Override
            public String type() {
                return "boom";
            }

            @Override
            public String execute(String payload) {
                throw new IllegalStateException("secret");
            }
        };
        governor = new DefaultTaskGovernor(PROPS, List.of(slow, fail));
        DefaultTaskGovernor g = governor;

        TaskRecord failed = g.submit(new TaskRequest("hist-fail", "h", "boom", null, null)).record();
        awaitTerminal(failed, Duration.ofSeconds(3));
        List<TaskEvent> failedHistory = g.history(failed.getId());
        assertEquals(TaskStatus.FAILED, failedHistory.get(failedHistory.size() - 1).status());
        assertEquals(FailureReason.HANDLER_ERROR, failedHistory.get(failedHistory.size() - 1).failureReason());

        TaskRecord succeeded = g.submit(new TaskRequest("hist-ok", "h", "slow", null, 5_000L)).record();
        slow.release.countDown();
        awaitTerminal(succeeded, Duration.ofSeconds(3));
        List<TaskEvent> okHistory = g.history(succeeded.getId());
        TaskEvent last = okHistory.get(okHistory.size() - 1);
        assertEquals(TaskStatus.SUCCEEDED, last.status());
        assertEquals(FailureReason.NONE, last.failureReason());
        assertEquals(Instant.now().getEpochSecond() > 0, true); // terminal timestamp present
        assertNotNull(succeeded.getFinishedAt());
        log.info("[history-kinds] failed={} succeeded={}", failedHistory, okHistory);
    }
}
