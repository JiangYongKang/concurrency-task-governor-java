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
import static com.github.highcumontoa.concurrencytaskgovernorjava.tests.support.GovernorTestSupport.properties;
import static com.github.highcumontoa.concurrencytaskgovernorjava.tests.support.GovernorTestSupport.sleepQuietly;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 截止时间边界压力测试。
 *
 * <p>用“并发 1 + 一个占槽任务 + 大量 1ms 预算任务”制造极端时序：所有排队任务必然在被 worker
 * 取到之前到期，从而反复压测“worker 出队 / 截止守护判定”在同一时刻竞争的路径。
 * 期望：每个任务都稳定收敛到 TIMED_OUT，且其动作一次都不被调用（executionCount 恒为 0）。</p>
 */
class TaskGovernorDeadlineEdgeTest {

    private static final Logger log = LoggerFactory.getLogger(TaskGovernorDeadlineEdgeTest.class);

    @Test
    void tasksExpiringAroundDequeue_neverRun_andStablyTimeOut() throws Exception {
        // 并发 1、单源队列 400、全局 400、保留 5000
        TaskGovernor governor = new TaskGovernor(properties(1, 400, 400, 5_000L, 5_000));
        try {
            int total = 300;
            AtomicInteger actionStarts = new AtomicInteger();

            // 唯一 worker 被占槽任务占用 400ms
            governor.submit("edge", "edge-hold", "delay", 5_000L, ctx -> {
                Thread.sleep(400);
                return "hold";
            });
            sleepQuietly(30);

            int accepted = 0;
            int rejected = 0;
            for (int i = 1; i <= total; i++) {
                // 所有任务预算仅 1ms：在排队的 400ms 内必然到期；同时把守护触发点压缩到
                // 提交后的同一毫秒级窗口，最大化与 worker 出队的竞争密度。
                SubmitResult r = governor.submit("edge", "edge-" + i, "delay", 1L, ctx -> {
                    actionStarts.incrementAndGet();
                    Thread.sleep(5);
                    return "ran";
                });
                if (r.outcome() == com.github.highcumontoa.concurrencytaskgovernorjava.domain.SubmitOutcome.ACCEPTED) {
                    accepted++;
                } else {
                    rejected++;
                    assertEquals(FailureReason.CAPACITY_EXHAUSTED, r.snapshot().failureReason());
                }
            }
            log.info("[edge] submitted {}: accepted={}, rejected={}", total, accepted, rejected);

            int timedOut = 0;
            for (int i = 1; i <= total; i++) {
                String key = "edge-" + i;
                if (governor.find(key).isEmpty()) {
                    continue;
                }
                TaskSnapshot fin = awaitTerminal(governor, key, Duration.ofSeconds(15));
                assertEquals(TaskStatus.TIMED_OUT, fin.status(),
                        "1ms-budget tasks behind a 400ms holder must all time out, but "
                                + key + " was " + fin.status());
                assertEquals(FailureReason.TIMEOUT, fin.failureReason());
                assertEquals(0, fin.executionCount(),
                        "expired task " + key + " must never start execution");
                timedOut++;
            }

            // 占槽任务收尾
            governor.cancel("edge-hold");
            awaitTerminal(governor, "edge-hold", Duration.ofSeconds(5));

            assertTrue(awaitQuiet(governor, Duration.ofSeconds(10)), "all tasks must drain");
            log.info("[edge] converged timedOut={}, actionStarts={} (must be 0), accepted={}, rejected={}",
                    timedOut, actionStarts.get(), accepted, rejected);

            assertEquals(accepted, timedOut, "every accepted task must be accounted for");
            assertEquals(0, actionStarts.get(),
                    "no expired-at-dequeue task may invoke its action, but got "
                            + actionStarts.get() + " invocations");
        } finally {
            governor.shutdown();
        }
    }
}
