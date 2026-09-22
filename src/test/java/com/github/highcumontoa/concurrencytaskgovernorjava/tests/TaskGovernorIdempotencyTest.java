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
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.highcumontoa.concurrencytaskgovernorjava.tests.support.GovernorTestSupport.awaitTerminal;
import static com.github.highcumontoa.concurrencytaskgovernorjava.tests.support.GovernorTestSupport.blocking;
import static com.github.highcumontoa.concurrencytaskgovernorjava.tests.support.GovernorTestSupport.properties;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 幂等与非法输入：同一幂等键的重复提交返回同一任务且不重复执行；
 * 不同非法输入与容量拒绝原因必须可区分；未知标识查询为空。
 */
class TaskGovernorIdempotencyTest {

    private static final Logger log = LoggerFactory.getLogger(TaskGovernorIdempotencyTest.class);

    @Test
    void duplicateSubmission_returnsSameTask_andNeverReExecutes() {
        // 保留窗口很小（1），验证去重在保留窗口内稳定
        TaskGovernor governor = new TaskGovernor(
                properties(2, 8, 32, 5_000L, 100));
        try {
            AtomicInteger runs = new AtomicInteger();

            SubmitResult first = governor.submit("svc-A", "idem-1", "delay", null, blocking(60, runs));
            assertEquals(SubmitOutcome.ACCEPTED, first.outcome());
            String taskId = first.snapshot().taskId();

            // 立即重复提交（任务可能仍在运行）
            SubmitResult second = governor.submit("svc-A", "idem-1", "delay", null, blocking(60, runs));
            assertEquals(SubmitOutcome.DUPLICATE, second.outcome());
            assertEquals(taskId, second.snapshot().taskId(), "duplicate must return the same taskId");
            log.info("[case] duplicate submit key=idem-1 -> same taskId={}, status={}",
                    second.snapshot().taskId(), second.snapshot().status());

            TaskSnapshot terminal = awaitTerminal(governor, "idem-1", Duration.ofSeconds(5));
            assertEquals(TaskStatus.SUCCEEDED, terminal.status());

            // 终态后再次重复提交：仍返回同一任务，不重新执行
            SubmitResult third = governor.submit("svc-A", "idem-1", "delay", null, blocking(60, runs));
            assertEquals(SubmitOutcome.DUPLICATE, third.outcome());
            assertEquals(taskId, third.snapshot().taskId());
            assertEquals(TaskStatus.SUCCEEDED, third.snapshot().status());
            assertEquals(1, third.snapshot().executionCount());
            assertEquals(1, runs.get(), "action must run exactly once despite 3 submissions");
            log.info("[case] post-terminal duplicate still maps to taskId={}, executions={}",
                    taskId, runs.get());
        } finally {
            governor.shutdown();
        }
    }

    @Test
    void invalidInputs_areRejectedWithDistinguishableReason() {
        TaskGovernor governor = new TaskGovernor(
                properties(2, 8, 32, 5_000L, 100));
        try {
            assertInvalid(governor.submit("s", null, "delay", null, ctx -> "x"), "null key");
            assertInvalid(governor.submit("s", " ", "delay", null, ctx -> "x"), "blank key");
            assertInvalid(governor.submit("s", "k", null, null, ctx -> "x"), "null type");
            assertInvalid(governor.submit("s", "k", " ", null, ctx -> "x"), "blank type");
            assertInvalid(governor.submit("s", "k", "delay", null, null), "null action");
            assertInvalid(governor.submit("s", "k", "delay", 0L, ctx -> "x"), "zero timeout");
            assertInvalid(governor.submit("s", "k", "delay", -5L, ctx -> "x"), "negative timeout");
            assertInvalid(governor.submit("s", "k", "delay", 120_000L, ctx -> "x"), "over-max timeout");

            // 合法提交应成功，且非法提交不得占用任何任务标识/容量
            SubmitResult ok = governor.submit("s", "valid-1", "delay", 1_000L, ctx -> "ok");
            assertEquals(SubmitOutcome.ACCEPTED, ok.outcome());
            awaitTerminal(governor, "valid-1", Duration.ofSeconds(3));
            assertFalse(governor.find("k").isPresent(), "rejected invalid submit must not be stored");
            log.info("[case] all 8 invalid inputs classified INVALID_INPUT; valid task stored and done");
        } finally {
            governor.shutdown();
        }
    }

    @Test
    void unknownIdentifier_lookupAndCancel_areEmpty() {
        TaskGovernor governor = new TaskGovernor(
                properties(2, 8, 32, 5_000L, 100));
        try {
            assertTrue(governor.find("does-not-exist").isEmpty());
            assertTrue(governor.cancel("does-not-exist").isEmpty());
            assertTrue(governor.find(" ").isEmpty());
            log.info("[case] unknown identifiers return empty lookups/cancels (no silent creation)");
        } finally {
            governor.shutdown();
        }
    }

    private void assertInvalid(SubmitResult r, String what) {
        assertEquals(SubmitOutcome.REJECTED, r.outcome(), "should reject: " + what);
        assertEquals(FailureReason.INVALID_INPUT, r.snapshot().failureReason(),
                "failure reason for " + what + " must be INVALID_INPUT, got: " + r.message());
        log.info("[case] invalid input ({}) -> REJECTED/INVALID_INPUT: {}", what, r.message());
    }
}
