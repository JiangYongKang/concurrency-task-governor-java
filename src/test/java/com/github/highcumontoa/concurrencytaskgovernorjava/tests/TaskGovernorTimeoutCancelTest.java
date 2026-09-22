package com.github.highcumontoa.concurrencytaskgovernorjava.tests;

import com.github.highcumontoa.concurrencytaskgovernorjava.core.TaskGovernor;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.FailureReason;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.SubmitResult;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.TaskSnapshot;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.TaskStatus;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.highcumontoa.concurrencytaskgovernorjava.tests.support.GovernorTestSupport.awaitQuiet;
import static com.github.highcumontoa.concurrencytaskgovernorjava.tests.support.GovernorTestSupport.awaitTerminal;
import static com.github.highcumontoa.concurrencytaskgovernorjava.tests.support.GovernorTestSupport.blocking;
import static com.github.highcumontoa.concurrencytaskgovernorjava.tests.support.GovernorTestSupport.properties;
import static com.github.highcumontoa.concurrencytaskgovernorjava.tests.support.GovernorTestSupport.sleepQuietly;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 超时 / 取消及其相互叠加：三种非正常终态必须可区分（TIMED_OUT / CANCELLED / FAILED），
 * 终态唯一且收敛；线程等资源必须释放。
 */
class TaskGovernorTimeoutCancelTest {

    private static final Logger log = LoggerFactory.getLogger(TaskGovernorTimeoutCancelTest.class);

    @Test
    void runningTask_exceedingDeadline_timesOut_withExactlyOneExecution() {
        // 并发 2；任务申请 150ms 超时，但动作要睡 800ms
        TaskGovernor governor = new TaskGovernor(properties(2, 8, 32, 5_000L, 100));
        try {
            AtomicInteger runs = new AtomicInteger();
            SubmitResult r = governor.submit("svc-A", "to-run-1", "delay", 150L, blocking(800, runs));
            String taskId = r.snapshot().taskId();
            log.info("[case] submit taskId={} timeout=150ms actionSleep=800ms", taskId);

            TaskSnapshot fin = awaitTerminal(governor, "to-run-1", Duration.ofSeconds(3));
            assertEquals(TaskStatus.TIMED_OUT, fin.status());
            assertEquals(FailureReason.TIMEOUT, fin.failureReason());
            assertEquals(1, fin.executionCount(), "timed-out task must not be retried");
            assertTrue(fin.finishedAtMillis() >= fin.startedAtMillis());
            log.info("[case] converged taskId={} status=TIMED_OUT reason=TIMEOUT detail='{}'",
                    taskId, fin.failureDetail());

            // 被中断的工作线程必须被释放：后续任务能正常跑
            SubmitResult follow = governor.submit("svc-A", "to-run-2", "delay", 5_000L,
                    ctx -> { Thread.sleep(30); return "follow-ok"; });
            TaskSnapshot followFin = awaitTerminal(governor, "to-run-2", Duration.ofSeconds(3));
            assertEquals(TaskStatus.SUCCEEDED, followFin.status(),
                    "worker thread must be released after timeout");
            log.info("[case] follow-up task SUCCEEDED -> worker released after timeout");
        } finally {
            governor.shutdown();
        }
    }

    @Test
    void queuedTaskExpiringBeforeStart_isTimedOut_andNeverExecutes() throws Exception {
        // 并发 1：第一个长任务占住唯一执行槽；排队任务给 100ms 超时，必然在排队中超时
        TaskGovernor governor = new TaskGovernor(properties(1, 8, 32, 5_000L, 100));
        try {
            AtomicInteger runs = new AtomicInteger();
            governor.submit("svc-A", "to-q-hold", "delay", 5_000L, blocking(800, runs));
            sleepQuietly(50);
            SubmitResult queued = governor.submit("svc-A", "to-q-1", "delay", 100L, blocking(10, runs));
            log.info("[case] queued taskId={} will expire while waiting", queued.snapshot().taskId());

            TaskSnapshot fin = awaitTerminal(governor, "to-q-1", Duration.ofSeconds(3));
            assertEquals(TaskStatus.TIMED_OUT, fin.status());
            assertEquals(FailureReason.TIMEOUT, fin.failureReason());
            assertEquals(0, fin.executionCount(), "task timed out in queue must never execute");
            log.info("[case] queued task status=TIMED_OUT executions=0 (capacity released)");

            // 取消占槽任务并收尾，确认系统排空
            governor.cancel("to-q-hold");
            awaitTerminal(governor, "to-q-hold", Duration.ofSeconds(3));
            assertTrue(awaitQuiet(governor, Duration.ofSeconds(3)));
            assertEquals(1, runs.get());
        } finally {
            governor.shutdown();
        }
    }

    @Test
    void cancelRunningTask_convergesCancelled_andReleasesWorker() {
        TaskGovernor governor = new TaskGovernor(properties(2, 8, 32, 5_000L, 100));
        try {
            AtomicInteger runs = new AtomicInteger();
            SubmitResult r = governor.submit("svc-A", "cx-run-1", "delay", 5_000L, blocking(800, runs));
            awaitStatus(governor, "cx-run-1", TaskStatus.RUNNING);

            TaskSnapshot afterCancel = governor.cancel("cx-run-1").orElseThrow();
            log.info("[case] cancel returned taskId={} immediateStatus={}",
                    afterCancel.taskId(), afterCancel.status());

            TaskSnapshot fin = awaitTerminal(governor, "cx-run-1", Duration.ofSeconds(3));
            assertEquals(TaskStatus.CANCELLED, fin.status());
            assertEquals(FailureReason.CANCELLED, fin.failureReason());
            assertEquals(1, fin.executionCount());

            SubmitResult follow = governor.submit("svc-A", "cx-run-2", "delay", 5_000L,
                    ctx -> { Thread.sleep(20); return "ok"; });
            TaskSnapshot followFin = awaitTerminal(governor, "cx-run-2", Duration.ofSeconds(3));
            assertEquals(TaskStatus.SUCCEEDED, followFin.status());
            log.info("[case] cancel converged CANCELLED; worker released; follow-up SUCCEEDED");
        } finally {
            governor.shutdown();
        }
    }

    @Test
    void cancelQueuedTask_isImmediateCancelled_andReleasesQueueSlot() throws Exception {
        TaskGovernor governor = new TaskGovernor(properties(1, 2, 4, 5_000L, 100));
        try {
            AtomicInteger runs = new AtomicInteger();
            governor.submit("svc-A", "cx-q-hold", "delay", 5_000L, blocking(600, runs));
            sleepQuietly(50);
            SubmitResult q = governor.submit("svc-A", "cx-q-1", "delay", 5_000L, blocking(10, runs));
            assertEquals(1, governor.currentSourceQueuedForTest("svc-A"));

            TaskSnapshot cancelled = governor.cancel("cx-q-1").orElseThrow();
            assertEquals(TaskStatus.CANCELLED, cancelled.status());
            assertEquals(FailureReason.CANCELLED, cancelled.failureReason());
            assertEquals(0, cancelled.executionCount());
            assertEquals(0, governor.currentSourceQueuedForTest("svc-A"),
                    "cancelled queued task must release its queue slot");
            log.info("[case] queued cancel immediate status=CANCELLED, queueSlot released");

            // 释放槽位后同键仍不可重复提交（任务在去重窗口内）
            SubmitResult dup = governor.submit("svc-A", "cx-q-1", "delay", 5_000L, blocking(10, runs));
            assertEquals(com.github.highcumontoa.concurrencytaskgovernorjava.domain.SubmitOutcome.DUPLICATE,
                    dup.outcome());
            log.info("[case] cancelled task remains deduplicated by key (no overwrite)");

            governor.cancel("cx-q-hold");
            awaitTerminal(governor, "cx-q-hold", Duration.ofSeconds(3));
            assertEquals(1, runs.get(), "queued-then-cancelled task never executes");
        } finally {
            governor.shutdown();
        }
    }

    @Test
    void timeoutAndCancelRacing_yieldExactlyOneStableTerminalState() throws Exception {
        // 任务 200ms 超时；在 200ms 附近发起取消，制造超时与取消的竞争。无论谁胜出，终态必须稳定唯一。
        TaskGovernor governor = new TaskGovernor(properties(4, 8, 32, 5_000L, 100));
        try {
            for (int i = 1; i <= 6; i++) {
                String key = "race-" + i;
                AtomicInteger runs = new AtomicInteger();
                governor.submit("svc-A", key, "delay", 200L, blocking(600, runs));
                sleepQuietly(180L + (i % 3) * 25L); // 180 / 205 / 230ms，覆盖超时前后
                governor.cancel(key);
                TaskSnapshot fin = awaitTerminal(governor, key, Duration.ofSeconds(3));
                assertTrue(fin.status() == TaskStatus.TIMED_OUT || fin.status() == TaskStatus.CANCELLED,
                        "race must resolve to TIMED_OUT or CANCELLED, was " + fin.status());
                assertTrue(fin.failureReason() == FailureReason.TIMEOUT
                                || fin.failureReason() == FailureReason.CANCELLED,
                        "reason must match one of the racing causes");
                // 再次取消不得改变已收敛的结论
                TaskSnapshot again = governor.cancel(key).orElseThrow();
                assertEquals(fin.status(), again.status());
                assertEquals(fin.failureReason(), again.failureReason());
                assertEquals(1, runs.get(), "racing task executes exactly once");
                log.info("[case] race#{} converged status={} reason={} (stable on repeated cancel)",
                        i, fin.status(), fin.failureReason());
            }
            assertTrue(awaitQuiet(governor, Duration.ofSeconds(3)));
        } finally {
            governor.shutdown();
        }
    }

    private void awaitStatus(TaskGovernor governor, String key, TaskStatus expected) {
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (System.nanoTime() < deadline) {
            TaskSnapshot s = governor.find(key).orElseThrow();
            if (s.status() == expected) {
                return;
            }
            sleepQuietly(2L);
        }
        throw new AssertionError(key + " did not reach " + expected);
    }
}
