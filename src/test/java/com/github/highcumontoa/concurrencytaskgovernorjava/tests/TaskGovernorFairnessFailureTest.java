package com.github.highcumontoa.concurrencytaskgovernorjava.tests;

import com.github.highcumontoa.concurrencytaskgovernorjava.core.TaskGovernor;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.SubmitOutcome;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.SubmitResult;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.TaskSnapshot;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.TaskStatus;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.FailureReason;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.highcumontoa.concurrencytaskgovernorjava.tests.support.GovernorTestSupport.awaitQuiet;
import static com.github.highcumontoa.concurrencytaskgovernorjava.tests.support.GovernorTestSupport.awaitTerminal;
import static com.github.highcumontoa.concurrencytaskgovernorjava.tests.support.GovernorTestSupport.failing;
import static com.github.highcumontoa.concurrencytaskgovernorjava.tests.support.GovernorTestSupport.properties;
import static com.github.highcumontoa.concurrencytaskgovernorjava.tests.support.GovernorTestSupport.sleepQuietly;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 多来源共享容量时的公平性、失败隔离与终态保留回收。
 */
class TaskGovernorFairnessFailureTest {

    private static final Logger log = LoggerFactory.getLogger(TaskGovernorFairnessFailureTest.class);

    @Test
    void oneSourceCannotStarveOthers_roundRobinAcrossSources() throws Exception {
        // 并发 1（串行执行，最能体现轮转），单源队列 20
        TaskGovernor governor = new TaskGovernor(properties(1, 20, 200, 5_000L, 500));
        try {
            // A 先占满：1 个运行 + 6 个排队
            for (int i = 1; i <= 7; i++) {
                SubmitResult r = governor.submit("svc-A", "fair-a" + i, "delay", null,
                        ctx -> { Thread.sleep(40); return "a"; });
                assertEquals(SubmitOutcome.ACCEPTED, r.outcome());
            }
            sleepQuietly(30); // 确保 a1 已在唯一执行槽

            // B 随后提交 4 个排队任务
            for (int i = 1; i <= 4; i++) {
                SubmitResult r = governor.submit("svc-B", "fair-b" + i, "delay", null,
                        ctx -> { Thread.sleep(40); return "b"; });
                assertEquals(SubmitOutcome.ACCEPTED, r.outcome());
            }

            List<String> finishOrder = new ArrayList<>();
            // 记录进入终态的先后（a1 之后的部分即可看出 B 是否被穿插调度）
            for (int i = 1; i <= 4; i++) {
                TaskSnapshot b = awaitTerminal(governor, "fair-b" + i, Duration.ofSeconds(10));
                finishOrder.add("b" + i);
                long aDone = countTerminalWithPrefix(governor, "fair-a");
                log.info("[fair] {} finished; A terminals so far={}", "b" + i, aDone);
            }
            // B 全部完成时，A 不应仍然积压全部 6 个：
            // 若严格先进先出且不区分来源，B 必须等 A 全部完成；轮转会让 A 在此之前就被穿插执行。
            long aDoneWhenBFinished = countTerminalWithPrefix(governor, "fair-a");
            assertTrue(aDoneWhenBFinished >= 3,
                    "expected A tasks interleaved with B under round-robin, but only "
                            + aDoneWhenBFinished + " A tasks done when all B finished");
            assertTrue(aDoneWhenBFinished < 7,
                    "B must not wait for all A tasks (would indicate head-of-line blocking): A done="
                            + aDoneWhenBFinished);
            log.info("[fair] interleaving observed: A done={} while all 4 B done (no starvation)",
                    aDoneWhenBFinished);

            for (int i = 1; i <= 7; i++) {
                assertEquals(TaskStatus.SUCCEEDED,
                        awaitTerminal(governor, "fair-a" + i, Duration.ofSeconds(10)).status());
            }
            assertTrue(awaitQuiet(governor, Duration.ofSeconds(5)));
        } finally {
            governor.shutdown();
        }
    }

    @Test
    void failingSource_isNormalized_andDoesNotHarmOtherSources() throws Exception {
        TaskGovernor governor = new TaskGovernor(properties(4, 8, 64, 5_000L, 200));
        try {
            AtomicInteger badRuns = new AtomicInteger();
            AtomicInteger goodRuns = new AtomicInteger();

            // 来源 BAD 连续提交 5 个必定抛异常的任务（底层为 IllegalStateException）
            for (int i = 1; i <= 5; i++) {
                SubmitResult r = governor.submit("svc-BAD", "bad-" + i, "failing", null,
                        failing(10, new IllegalStateException("db down " + i), badRuns));
                assertEquals(SubmitOutcome.ACCEPTED, r.outcome());
            }
            // 来源 GOOD 同时提交 5 个正常任务
            for (int i = 1; i <= 5; i++) {
                int idx = i;
                SubmitResult r = governor.submit("svc-GOOD", "good-" + idx, "echo", null,
                        ctx -> {
                            goodRuns.incrementAndGet();
                            return "ok-" + idx;
                        });
                assertEquals(SubmitOutcome.ACCEPTED, r.outcome());
            }

            for (int i = 1; i <= 5; i++) {
                TaskSnapshot bad = awaitTerminal(governor, "bad-" + i, Duration.ofSeconds(5));
                assertEquals(TaskStatus.FAILED, bad.status());
                assertEquals(FailureReason.EXECUTION_ERROR, bad.failureReason(),
                        "underlying exception must be normalized");
                assertTrue(bad.failureDetail().contains("IllegalStateException"),
                        "detail keeps a stable category, not raw stack: " + bad.failureDetail());
                assertFalse(bad.failureDetail().contains("\n"),
                        "detail must be single-line normalized text, not a raw stacktrace");
                assertEquals(1, bad.executionCount());
            }
            for (int i = 1; i <= 5; i++) {
                TaskSnapshot good = awaitTerminal(governor, "good-" + i, Duration.ofSeconds(5));
                assertEquals(TaskStatus.SUCCEEDED, good.status());
                assertEquals(1, good.executionCount());
            }
            assertEquals(5, badRuns.get());
            assertEquals(5, goodRuns.get());
            assertTrue(awaitQuiet(governor, Duration.ofSeconds(3)));
            log.info("[isolation] BAD x5 -> FAILED/EXECUTION_ERROR(normalized); GOOD x5 -> SUCCEEDED; "
                    + "workers unaffected");
        } finally {
            governor.shutdown();
        }
    }

    @Test
    void terminalRetention_evictsOldestButKeepsActiveTasks() throws Exception {
        // 保留窗口仅 3：终态任务超过 3 个后，最老的终态标识被回收；活动任务绝不回收
        TaskGovernor governor = new TaskGovernor(properties(2, 8, 64, 5_000L, 3));
        try {
            for (int i = 1; i <= 6; i++) {
                int idx = i;
                String key = "ret-" + idx;
                governor.submit("svc-A", key, "echo", null, ctx -> "v" + idx);
                awaitTerminal(governor, key, Duration.ofSeconds(3));
            }
            sleepQuietly(30);
            // 最老的三个应已被回收
            assertFalse(governor.find("ret-1").isPresent(), "oldest terminal should be evicted");
            assertFalse(governor.find("ret-2").isPresent());
            assertFalse(governor.find("ret-3").isPresent());
            // 最新的三个仍可查询（去重窗口内）
            for (int i = 4; i <= 6; i++) {
                TaskSnapshot s = governor.find("ret-" + i).orElseThrow();
                assertEquals(TaskStatus.SUCCEEDED, s.status());
                assertEquals("v" + i, s.result());
            }
            // 被回收后，同键可再次提交（窗口外），这是有界内存的明确取舍
            SubmitResult again = governor.submit("svc-A", "ret-1", "echo", null, ctx -> "v-new");
            assertEquals(SubmitOutcome.ACCEPTED, again.outcome());
            awaitTerminal(governor, "ret-1", Duration.ofSeconds(3));
            log.info("[retention] oldest terminals evicted, newest retained, active tasks protected");
            assertTrue(awaitQuiet(governor, Duration.ofSeconds(3)));
        } finally {
            governor.shutdown();
        }
    }

    private long countTerminalWithPrefix(TaskGovernor governor, String prefix) {
        Map<String, TaskSnapshot> all = new HashMap<>();
        for (int i = 1; i <= 20; i++) {
            governor.find(prefix + i).ifPresent(s -> all.put(s.idempotencyKey(), s));
        }
        return all.values().stream().filter(s -> s.status().isTerminal()).count();
    }
}
