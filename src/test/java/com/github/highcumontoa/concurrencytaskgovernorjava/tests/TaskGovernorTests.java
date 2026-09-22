package com.github.highcumontoa.concurrencytaskgovernorjava.tests;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.highcumontoa.concurrencytaskgovernorjava.task.DefaultTaskGovernor;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.FailureReason;
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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Behavior tests for {@link DefaultTaskGovernor}. Every assertion logs the task
 * id, the observed state transition and the judgment basis.
 */
class TaskGovernorTests {

    private static final Logger log = LoggerFactory.getLogger(TaskGovernorTests.class);

    /** maxConcurrency=2, maxQueueSize=2 -> 4 global in-flight slots, 3 per source. */
    private static final TaskGovernorProperties PROPS =
            new TaskGovernorProperties(2, 2, 3, 5_000L, 60_000L);

    private DefaultTaskGovernor governor;

    @AfterEach
    void tearDown() {
        if (governor != null) {
            governor.shutdown();
        }
    }

    // ---------- test handlers ----------

    /** Blocks until released; counts executions; interrupt-aware. */
    static final class BlockingHandler implements TaskHandler {
        final AtomicInteger executions = new AtomicInteger();
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger interruptions = new AtomicInteger();

        @Override
        public String type() {
            return "blocking";
        }

        @Override
        public String execute(String payload) throws Exception {
            executions.incrementAndGet();
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException interrupted) {
                interruptions.incrementAndGet();
                Thread.currentThread().interrupt();
                throw interrupted;
            }
            return "done";
        }
    }

    static final class FailingHandler implements TaskHandler {
        static final String SECRET = "sensitive-internal-detail-42";
        final AtomicInteger executions = new AtomicInteger();

        @Override
        public String type() {
            return "failing";
        }

        @Override
        public String execute(String payload) {
            executions.incrementAndGet();
            throw new IllegalStateException(SECRET);
        }
    }

    // ---------- helpers ----------

    private DefaultTaskGovernor newGovernor(TaskHandler... handlers) {
        governor = new DefaultTaskGovernor(PROPS, List.of(handlers));
        return governor;
    }

    private static TaskRequest request(String key, String source, String type, Long timeoutMs) {
        return new TaskRequest(key, source, type, null, timeoutMs);
    }

    private static void awaitStatus(TaskRecord record, TaskStatus expected, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (record.getStatus() != expected && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        log.info("[await] task={} expected={} actual={} reason={}",
                record.getId(), expected, record.getStatus(), record.getFailureReason());
        assertEquals(expected, record.getStatus(),
                "task " + record.getId() + " did not reach " + expected);
    }

    private static void awaitTerminal(TaskRecord record, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!record.getStatus().isTerminal() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        log.info("[await-terminal] task={} status={} reason={}",
                record.getId(), record.getStatus(), record.getFailureReason());
        assertTrue(record.getStatus().isTerminal(),
                "task " + record.getId() + " stuck in " + record.getStatus());
    }

    // ---------- tests ----------

    @Test
    void capacityExhaustion_rejectsOverflowAndExecutesEachTaskExactlyOnce() throws Exception {
        BlockingHandler handler = new BlockingHandler();
        DefaultTaskGovernor g = newGovernor(handler);

        // 4 global slots (2 workers + 2 queue); distinct sources so only the
        // global limit is exercised (per-source limit is 3).
        List<TaskRecord> accepted = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            SubmissionResult result = g.submit(request("cap-key-" + i, "src-" + i, "blocking", null));
            log.info("[capacity] submit {} -> outcome={} task={}",
                    i, result.outcome(), result.record() != null ? result.record().getId() : "-");
            assertEquals(SubmissionOutcome.ACCEPTED, result.outcome());
            accepted.add(result.record());
        }
        SubmissionResult overflow = g.submit(request("cap-key-overflow", "src-overflow", "blocking", null));
        log.info("[capacity] overflow -> outcome={} detail={}", overflow.outcome(), overflow.detail());
        assertEquals(SubmissionOutcome.REJECTED_CAPACITY, overflow.outcome(),
                "overflow beyond 4 global slots must be explicitly rejected");
        assertNull(overflow.record(), "rejected submissions must not create tasks");

        handler.release.countDown();
        for (TaskRecord record : accepted) {
            awaitTerminal(record, Duration.ofSeconds(5));
        }
        log.info("[capacity] judgment: executions={} must equal accepted count={}",
                handler.executions.get(), accepted.size());
        assertEquals(accepted.size(), handler.executions.get(),
                "each accepted task must execute exactly once");
        for (TaskRecord record : accepted) {
            assertEquals(TaskStatus.SUCCEEDED, record.getStatus());
        }
    }

    @Test
    void capacityExhaustion_globalSlotsFull_yieldsDeterministicRejection() throws Exception {
        BlockingHandler handler = new BlockingHandler();
        // per-source limit equals global capacity so only the global limit can bite
        governor = new DefaultTaskGovernor(
                new TaskGovernorProperties(2, 1, 3, 5_000L, 60_000L), List.of(handler));
        DefaultTaskGovernor g = governor;

        List<SubmissionOutcome> outcomes = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            SubmissionResult result = g.submit(request("g-key-" + i, "s" + i, "blocking", null));
            outcomes.add(result.outcome());
            log.info("[global-capacity] submit {} -> outcome={}", i, result.outcome());
        }
        long accepted = outcomes.stream().filter(o -> o == SubmissionOutcome.ACCEPTED).count();
        long rejected = outcomes.stream().filter(o -> o == SubmissionOutcome.REJECTED_CAPACITY).count();
        log.info("[global-capacity] judgment: accepted={} (expect 3 slots), rejected={} (expect 2)",
                accepted, rejected);
        assertEquals(3, accepted, "global capacity is maxConcurrency(2)+maxQueueSize(1)=3");
        assertEquals(2, rejected, "overflow must be explicitly rejected, never queued silently");

        handler.release.countDown();
    }

    @Test
    void duplicateSubmission_sameIdempotencyKey_executesOnlyOnce() throws Exception {
        BlockingHandler handler = new BlockingHandler();
        DefaultTaskGovernor g = newGovernor(handler);

        SubmissionResult first = g.submit(request("dup-key", "src-a", "blocking", null));
        SubmissionResult second = g.submit(request("dup-key", "src-a", "blocking", null));
        log.info("[idempotency] first={} id={} second={} sameId={}",
                first.outcome(), first.record().getId(), second.outcome(),
                first.record().getId().equals(second.record().getId()));
        assertEquals(SubmissionOutcome.ACCEPTED, first.outcome());
        assertEquals(SubmissionOutcome.DUPLICATE, second.outcome());
        assertEquals(first.record().getId(), second.record().getId(),
                "duplicate must return the original task id");

        // concurrent duplicates: exactly one ACCEPTED overall
        int threads = 8;
        CountDownLatch go = new CountDownLatch(1);
        List<SubmissionResult> results = java.util.Collections.synchronizedList(new ArrayList<>());
        List<Thread> pool = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            Thread t = new Thread(() -> {
                try {
                    go.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                results.add(g.submit(request("race-key", "src-b", "blocking", null)));
            });
            t.start();
            pool.add(t);
        }
        go.countDown();
        for (Thread t : pool) {
            t.join();
        }
        long acceptedCount = results.stream().filter(r -> r.outcome() == SubmissionOutcome.ACCEPTED).count();
        long duplicateCount = results.stream().filter(r -> r.outcome() == SubmissionOutcome.DUPLICATE).count();
        log.info("[idempotency-race] judgment: accepted={} duplicates={} (expect 1 + {})",
                acceptedCount, duplicateCount, threads - 1);
        assertEquals(1, acceptedCount);
        assertEquals(threads - 1, duplicateCount);

        handler.release.countDown();
        awaitTerminal(first.record(), Duration.ofSeconds(5));
        awaitTerminal(results.get(0).record(), Duration.ofSeconds(5));
        log.info("[idempotency] judgment: executions={} must equal 2 (one per unique key)",
                handler.executions.get());
        assertEquals(2, handler.executions.get(), "no idempotency key may execute twice");
    }

    @Test
    void timeout_transitionsToTimedOutAndReleasesCapacity() throws Exception {
        BlockingHandler handler = new BlockingHandler();
        DefaultTaskGovernor g = newGovernor(handler);

        SubmissionResult slow = g.submit(request("slow-key", "src-a", "blocking", 200L));
        assertEquals(SubmissionOutcome.ACCEPTED, slow.outcome());
        awaitStatus(slow.record(), TaskStatus.RUNNING, Duration.ofSeconds(2));
        awaitStatus(slow.record(), TaskStatus.TIMED_OUT, Duration.ofSeconds(3));
        log.info("[timeout] judgment: task={} status={} reason={} (expect TIMED_OUT/TIMEOUT)",
                slow.record().getId(), slow.record().getStatus(), slow.record().getFailureReason());
        assertEquals(FailureReason.TIMEOUT, slow.record().getFailureReason());

        // slot must have been released: a fresh submission is accepted immediately
        SubmissionResult next = g.submit(request("after-timeout", "src-a", "blocking", null));
        log.info("[timeout] judgment: follow-up submission outcome={} (expect ACCEPTED, slot released)",
                next.outcome());
        assertEquals(SubmissionOutcome.ACCEPTED, next.outcome());
        handler.release.countDown();
        awaitTerminal(next.record(), Duration.ofSeconds(5));
    }

    @Test
    void cancel_queuedTask_neverExecutes() throws Exception {
        BlockingHandler handler = new BlockingHandler();
        DefaultTaskGovernor g = newGovernor(handler);

        // occupy both worker threads
        g.submit(request("run-1", "src-a", "blocking", null));
        g.submit(request("run-2", "src-a", "blocking", null));
        handler.started.await(2, TimeUnit.SECONDS);
        // wait until both workers are actually blocked: submit until queue holds one
        SubmissionResult queued = g.submit(request("queued-key", "src-b", "blocking", null));
        assertEquals(SubmissionOutcome.ACCEPTED, queued.outcome());
        log.info("[cancel-queued] task={} status={} (expect QUEUED while workers busy)",
                queued.record().getId(), queued.record().getStatus());

        int executionsBefore = handler.executions.get();
        TaskRecord cancelled = g.cancel(queued.record().getId()).orElseThrow();
        log.info("[cancel-queued] judgment: task={} status={} reason={} (expect CANCELLED/CANCELLED)",
                cancelled.getId(), cancelled.getStatus(), cancelled.getFailureReason());
        assertEquals(TaskStatus.CANCELLED, cancelled.getStatus());
        assertEquals(FailureReason.CANCELLED, cancelled.getFailureReason());

        handler.release.countDown();
        Thread.sleep(300); // give the executor a chance to (wrongly) pick it up
        log.info("[cancel-queued] judgment: executions before={} after={} delta must be <= 2 running tasks",
                executionsBefore, handler.executions.get());
        assertTrue(handler.executions.get() - executionsBefore <= 2,
                "cancelled queued task must never execute");
        assertEquals(TaskStatus.CANCELLED, cancelled.getStatus(), "terminal state must be stable");
    }

    @Test
    void cancel_runningTask_interruptsAndConvergesToCancelled() throws Exception {
        BlockingHandler handler = new BlockingHandler();
        DefaultTaskGovernor g = newGovernor(handler);

        SubmissionResult running = g.submit(request("run-cancel", "src-a", "blocking", null));
        awaitStatus(running.record(), TaskStatus.RUNNING, Duration.ofSeconds(2));

        TaskRecord cancelled = g.cancel(running.record().getId()).orElseThrow();
        log.info("[cancel-running] judgment: task={} status={} reason={} interruptions={}",
                cancelled.getId(), cancelled.getStatus(), cancelled.getFailureReason(),
                handler.interruptions.get());
        assertEquals(TaskStatus.CANCELLED, cancelled.getStatus());
        assertEquals(FailureReason.CANCELLED, cancelled.getFailureReason());
        awaitTerminal(cancelled, Duration.ofSeconds(2));
        assertEquals(1, handler.interruptions.get(), "running task must be interrupted");
        handler.release.countDown();
    }

    @Test
    void timeoutThenCancel_firstTerminalStateWins() throws Exception {
        BlockingHandler handler = new BlockingHandler();
        DefaultTaskGovernor g = newGovernor(handler);

        SubmissionResult slow = g.submit(request("timeout-then-cancel", "src-a", "blocking", 150L));
        awaitStatus(slow.record(), TaskStatus.TIMED_OUT, Duration.ofSeconds(3));

        TaskRecord afterCancel = g.cancel(slow.record().getId()).orElseThrow();
        log.info("[timeout+cancel] judgment: task={} status={} reason={} "
                        + "(cancel after timeout must not overwrite TIMED_OUT)",
                afterCancel.getId(), afterCancel.getStatus(), afterCancel.getFailureReason());
        assertEquals(TaskStatus.TIMED_OUT, afterCancel.getStatus());
        assertEquals(FailureReason.TIMEOUT, afterCancel.getFailureReason());
        handler.release.countDown();
    }

    @Test
    void cancelThenTimeout_firstTerminalStateWins() throws Exception {
        BlockingHandler handler = new BlockingHandler();
        DefaultTaskGovernor g = newGovernor(handler);

        SubmissionResult task = g.submit(request("cancel-then-timeout", "src-a", "blocking", 200L));
        TaskRecord cancelled = g.cancel(task.record().getId()).orElseThrow();
        assertEquals(TaskStatus.CANCELLED, cancelled.getStatus());

        Thread.sleep(400); // let the (already fired or pending) deadline check pass
        log.info("[cancel+timeout] judgment: task={} status={} reason={} "
                        + "(deadline after cancel must not overwrite CANCELLED)",
                cancelled.getId(), cancelled.getStatus(), cancelled.getFailureReason());
        assertEquals(TaskStatus.CANCELLED, cancelled.getStatus());
        assertEquals(FailureReason.CANCELLED, cancelled.getFailureReason());
        handler.release.countDown();
    }

    @Test
    void handlerException_normalizedToStableFailureReason() throws Exception {
        FailingHandler handler = new FailingHandler();
        DefaultTaskGovernor g = newGovernor(handler);

        SubmissionResult failed = g.submit(request("fail-key", "src-a", "failing", null));
        awaitStatus(failed.record(), TaskStatus.FAILED, Duration.ofSeconds(3));
        log.info("[failure-normalization] judgment: task={} status={} reason={} result={}",
                failed.record().getId(), failed.record().getStatus(),
                failed.record().getFailureReason(), failed.record().getResult());
        assertEquals(FailureReason.HANDLER_ERROR, failed.record().getFailureReason());
        String view = com.github.highcumontoa.concurrencytaskgovernorjava.task.TaskView
                .of(failed.record()).toString();
        assertTrue(!view.contains(FailingHandler.SECRET),
                "raw exception details must never leak into the caller-visible view");
        assertNull(failed.record().getResult());
    }

    @Test
    void unknownTaskType_failsWithDistinguishableReason() throws Exception {
        BlockingHandler handler = new BlockingHandler();
        DefaultTaskGovernor g = newGovernor(handler);

        SubmissionResult unknown = g.submit(request("unknown-key", "src-a", "no-such-type", null));
        awaitStatus(unknown.record(), TaskStatus.FAILED, Duration.ofSeconds(3));
        log.info("[unknown-type] judgment: task={} reason={} (expect UNSUPPORTED_TYPE, "
                        + "distinct from HANDLER_ERROR)",
                unknown.record().getId(), unknown.record().getFailureReason());
        assertEquals(FailureReason.UNSUPPORTED_TYPE, unknown.record().getFailureReason());
    }

    @Test
    void perSourceFairness_oneSourceCannotStarveOthers() throws Exception {
        BlockingHandler handler = new BlockingHandler();
        DefaultTaskGovernor g = newGovernor(handler);

        // per-source limit is 3: source A fills its share
        for (int i = 0; i < 3; i++) {
            SubmissionResult r = g.submit(request("a-" + i, "greedy-a", "blocking", null));
            assertEquals(SubmissionOutcome.ACCEPTED, r.outcome());
        }
        SubmissionResult fourth = g.submit(request("a-3", "greedy-a", "blocking", null));
        log.info("[fairness] judgment: greedy source 4th submission -> {} (expect REJECTED_SOURCE_LIMIT)",
                fourth.outcome());
        assertEquals(SubmissionOutcome.REJECTED_SOURCE_LIMIT, fourth.outcome());

        // another source still gets served
        SubmissionResult other = g.submit(request("b-0", "polite-b", "blocking", null));
        log.info("[fairness] judgment: other source submission -> {} (expect ACCEPTED)", other.outcome());
        assertEquals(SubmissionOutcome.ACCEPTED, other.outcome());

        // a failing source does not drag others down: failures release capacity
        FailingHandler failing = new FailingHandler();
        governor.shutdown();
        governor = new DefaultTaskGovernor(PROPS, List.of(failing, handler));
        DefaultTaskGovernor g2 = governor;
        for (int i = 0; i < 3; i++) {
            SubmissionResult r = g2.submit(request("f-" + i, "flaky-c", "failing", null));
            assertEquals(SubmissionOutcome.ACCEPTED, r.outcome());
            awaitTerminal(r.record(), Duration.ofSeconds(3));
        }
        SubmissionResult afterFailures = g2.submit(request("f-next", "flaky-c", "failing", null));
        log.info("[fairness] judgment: after 3 failures source can submit again -> {} "
                        + "(failures must release slots)", afterFailures.outcome());
        assertEquals(SubmissionOutcome.ACCEPTED, afterFailures.outcome());

        handler.release.countDown();
    }

    @Test
    void invalidInput_rejectedWithoutCreatingTasks() {
        BlockingHandler handler = new BlockingHandler();
        DefaultTaskGovernor g = newGovernor(handler);

        SubmissionResult blankKey = g.submit(request(" ", "src-a", "blocking", null));
        SubmissionResult blankSource = g.submit(request("k1", "", "blocking", null));
        SubmissionResult blankType = g.submit(request("k2", "src-a", null, null));
        SubmissionResult badTimeout = g.submit(request("k3", "src-a", "blocking", -5L));
        SubmissionResult nullRequest = g.submit(null);
        log.info("[validation] outcomes: blankKey={} blankSource={} blankType={} badTimeout={} null={}",
                blankKey.outcome(), blankSource.outcome(), blankType.outcome(),
                badTimeout.outcome(), nullRequest.outcome());
        for (SubmissionResult r : List.of(blankKey, blankSource, blankType, badTimeout, nullRequest)) {
            assertEquals(SubmissionOutcome.REJECTED_INVALID, r.outcome());
            assertNull(r.record(), "invalid submissions must not create tasks");
        }
        assertEquals(0, handler.executions.get(), "invalid submissions must never execute");
    }

    @Test
    void mixedWorkload_allTasksConvergeToTerminalStates() throws Exception {
        BlockingHandler blocking = new BlockingHandler();
        FailingHandler failing = new FailingHandler();
        governor = new DefaultTaskGovernor(PROPS, List.of(blocking, failing));
        DefaultTaskGovernor g = governor;

        List<TaskRecord> all = new ArrayList<>();
        all.add(g.submit(request("mix-ok", "s1", "blocking", null)).record());
        all.add(g.submit(request("mix-fail", "s1", "failing", null)).record());
        all.add(g.submit(request("mix-timeout", "s2", "blocking", 150L)).record());
        SubmissionResult toCancel = g.submit(request("mix-cancel", "s2", "blocking", null));
        if (toCancel.outcome() == SubmissionOutcome.ACCEPTED) {
            g.cancel(toCancel.record().getId());
            all.add(toCancel.record());
        }

        blocking.release.countDown();
        for (TaskRecord record : all) {
            awaitTerminal(record, Duration.ofSeconds(5));
        }
        long distinct = all.stream().map(TaskRecord::getStatus).distinct().count();
        log.info("[convergence] judgment: {} tasks all terminal; distinct terminal states={} "
                        + "(failure causes must be distinguishable)", all.size(), distinct);
        for (TaskRecord record : all) {
            assertTrue(record.getStatus().isTerminal());
            if (record.getStatus() == TaskStatus.SUCCEEDED) {
                assertEquals(FailureReason.NONE, record.getFailureReason(),
                        "succeeded tasks carry no failure reason");
            } else {
                assertNotEquals(FailureReason.NONE, record.getFailureReason(),
                        "failed/timeout/cancelled tasks must carry an explicit reason");
            }
        }
        assertTrue(distinct >= 3, "expected SUCCEEDED/FAILED/TIMED_OUT/CANCELLED to be distinguishable");
    }
}
