package com.github.highcumontoa.concurrencytaskgovernorjava.tests;

import com.github.highcumontoa.concurrencytaskgovernorjava.core.TaskGovernor;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.FailureReason;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.SubmitOutcome;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.SubmitResult;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.TaskSnapshot;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.TaskStatus;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.highcumontoa.concurrencytaskgovernorjava.tests.support.GovernorTestSupport.awaitQuiet;
import static com.github.highcumontoa.concurrencytaskgovernorjava.tests.support.GovernorTestSupport.awaitTerminal;
import static com.github.highcumontoa.concurrencytaskgovernorjava.tests.support.GovernorTestSupport.blocking;
import static com.github.highcumontoa.concurrencytaskgovernorjava.tests.support.GovernorTestSupport.small;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 容量打满场景：并发硬上限、分来源队列与全局队列打满时必须给出确定、可区分的拒绝结论；
 * 被受理任务不得丢失或执行两次。
 */
class TaskGovernorCapacityTest {

    private static final Logger log = LoggerFactory.getLogger(TaskGovernorCapacityTest.class);

    @Test
    void concurrencyCap_and_capacityRejections_areDeterministic() throws Exception {
        // 配置：并发 2，单来源排队 2，全局排队 4
        TaskGovernor governor = new TaskGovernor(small());
        try {
            AtomicInteger runs = new AtomicInteger();

            // 1) 两个长任务占满 2 个执行槽
            SubmitResult r1 = governor.submit("svc-A", "cap-a1", "delay", null, blocking(400, runs));
            SubmitResult r2 = governor.submit("svc-A", "cap-a2", "delay", null, blocking(400, runs));
            assertEquals(SubmitOutcome.ACCEPTED, r1.outcome());
            assertEquals(SubmitOutcome.ACCEPTED, r2.outcome());
            awaitRunning(governor, "cap-a1");
            awaitRunning(governor, "cap-a2");
            log.info("[case] execution slots full: running=2 (concurrency cap)");

            // 2) 再来两个 A：排队（A 队列打满到 2）
            SubmitResult r3 = governor.submit("svc-A", "cap-a3", "delay", null, blocking(50, runs));
            SubmitResult r4 = governor.submit("svc-A", "cap-a4", "delay", null, blocking(50, runs));
            assertEquals(SubmitOutcome.ACCEPTED, r3.outcome());
            assertEquals(SubmitOutcome.ACCEPTED, r4.outcome());
            assertEquals(2, governor.currentSourceQueuedForTest("svc-A"));
            log.info("[case] source A queue full (2), globalQueued={}", governor.currentGlobalQueuedForTest());

            // 3) A 的第五个提交：必须被明确拒绝，原因是分来源容量
            SubmitResult rejectedPerSource = governor.submit("svc-A", "cap-a5", "delay", null, blocking(10, runs));
            assertEquals(SubmitOutcome.REJECTED, rejectedPerSource.outcome());
            assertEquals(FailureReason.CAPACITY_EXHAUSTED, rejectedPerSource.snapshot().failureReason());
            assertTrue(rejectedPerSource.message().contains("source 'svc-A'"),
                    "rejection message must identify the saturated source: " + rejectedPerSource.message());
            log.info("[case] A 5th submit REJECTED reason=CAPACITY_EXHAUSTED detail='{}'",
                    rejectedPerSource.message());

            // 4) 其他来源仍可使用剩余全局容量：B 两个（全局排队达 4 上限）
            SubmitResult r5 = governor.submit("svc-B", "cap-b1", "delay", null, blocking(50, runs));
            SubmitResult r6 = governor.submit("svc-B", "cap-b2", "delay", null, blocking(50, runs));
            assertEquals(SubmitOutcome.ACCEPTED, r5.outcome());
            assertEquals(SubmitOutcome.ACCEPTED, r6.outcome());
            assertEquals(4, governor.currentGlobalQueuedForTest());

            // 5) 此时全局队列打满：C 的提交必须被拒绝，原因是全局容量
            SubmitResult rejectedGlobal = governor.submit("svc-C", "cap-c1", "delay", null, blocking(10, runs));
            assertEquals(SubmitOutcome.REJECTED, rejectedGlobal.outcome());
            assertEquals(FailureReason.CAPACITY_EXHAUSTED, rejectedGlobal.snapshot().failureReason());
            assertTrue(rejectedGlobal.message().contains("global queue is full"),
                    "rejection message must identify global saturation: " + rejectedGlobal.message());
            log.info("[case] C submit REJECTED reason=CAPACITY_EXHAUSTED detail='{}'",
                    rejectedGlobal.message());

            // 6) 所有 6 个被受理任务最终成功，且每个恰好执行一次（无丢失、无重复执行）
            List<String> acceptedKeys = List.of("cap-a1", "cap-a2", "cap-a3", "cap-a4", "cap-b1", "cap-b2");
            List<TaskSnapshot> finals = new ArrayList<>();
            for (String key : acceptedKeys) {
                finals.add(awaitTerminal(governor, key, Duration.ofSeconds(5)));
            }
            for (TaskSnapshot s : finals) {
                assertEquals(TaskStatus.SUCCEEDED, s.status(),
                        "task " + s.taskId() + " ended " + s.status());
                assertEquals(1, s.executionCount(),
                        "task " + s.taskId() + " must execute exactly once");
            }
            assertEquals(6, runs.get(), "exactly 6 action invocations expected");
            assertTrue(awaitQuiet(governor, Duration.ofSeconds(3)), "governor should drain");
            log.info("[case] all 6 accepted tasks SUCCEEDED, total executions={} (exact)", runs.get());
        } finally {
            governor.shutdown();
        }
    }

    private void awaitRunning(TaskGovernor governor, String key) {
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (System.nanoTime() < deadline) {
            TaskSnapshot s = governor.find(key).orElseThrow();
            if (s.status() == TaskStatus.RUNNING) {
                log.info("[case] key={} observed RUNNING", key);
                return;
            }
            try {
                Thread.sleep(2L);
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }
        throw new AssertionError(key + " did not reach RUNNING");
    }
}
