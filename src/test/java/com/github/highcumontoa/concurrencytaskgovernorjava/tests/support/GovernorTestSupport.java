package com.github.highcumontoa.concurrencytaskgovernorjava.tests.support;

import com.github.highcumontoa.concurrencytaskgovernorjava.config.GovernorProperties;
import com.github.highcumontoa.concurrencytaskgovernorjava.core.TaskGovernor;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.TaskSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 测试公共工具：构造隔离配置的 {@link TaskGovernor}、记录执行次数、
 * 带日志地等待状态收敛。每个测试自行 new 执行器，避免相互干扰。
 */
public final class GovernorTestSupport {

    private static final Logger log = LoggerFactory.getLogger(GovernorTestSupport.class);

    private GovernorTestSupport() {
    }

    public static GovernorProperties properties(int maxConcurrency, int maxQueuedPerSource,
                                                int maxQueuedGlobal, long defaultTimeout,
                                                int retained) {
        GovernorProperties p = new GovernorProperties();
        p.setMaxConcurrency(maxConcurrency);
        p.setMaxQueuedPerSource(maxQueuedPerSource);
        p.setMaxQueuedGlobal(maxQueuedGlobal);
        p.setDefaultTimeoutMillis(defaultTimeout);
        p.setMaxTimeoutMillis(60_000L);
        p.setRetainedTerminalTasks(retained);
        return p;
    }

    public static GovernorProperties small() {
        // 并发 2、单源队列 2、全局队列 4、默认超时 600ms、保留 1000
        return properties(2, 2, 4, 600L, 1000);
    }

    /**
     * 可阻塞指定毫秒并响应中断的任务体；每开始执行一次 runCount + 1。
     */
    public static com.github.highcumontoa.concurrencytaskgovernorjava.domain.TaskAction blocking(
            long sleepMillis, AtomicInteger runCount) {
        return ctx -> {
            int n = runCount.incrementAndGet();
            log.info("[action] taskId={} source={} execution#{} starting, sleep={}ms",
                    ctx.taskId(), ctx.source(), n, sleepMillis);
            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                log.info("[action] taskId={} execution#{} interrupted (cancelRequested={})",
                        ctx.taskId(), n, ctx.cancellationRequested());
                throw e;
            }
            log.info("[action] taskId={} execution#{} completed normally", ctx.taskId(), n);
            return "done-" + n;
        };
    }

    public static com.github.highcumontoa.concurrencytaskgovernorjava.domain.TaskAction failing(
            long sleepMillis, RuntimeException error, AtomicInteger runCount) {
        return ctx -> {
            int n = runCount.incrementAndGet();
            log.info("[action] taskId={} execution#{} starting, will fail after {}ms with {}",
                    ctx.taskId(), n, sleepMillis, error.getClass().getSimpleName());
            Thread.sleep(sleepMillis);
            throw error;
        };
    }

    /** 轮询等待任务到达终态，并打印判定依据。 */
    public static TaskSnapshot awaitTerminal(TaskGovernor governor, String key, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        TaskSnapshot last = null;
        while (System.nanoTime() < deadline) {
            Optional<TaskSnapshot> snap = governor.find(key);
            if (snap.isPresent()) {
                last = snap.get();
                if (last.status().isTerminal()) {
                    log.info("[await] key={} converged: status={} reason={} executions={}",
                            key, last.status(), last.failureReason(), last.executionCount());
                    return last;
                }
            }
            sleepQuietly(5L);
        }
        throw new AssertionError("task " + key + " did not reach terminal state within "
                + timeout + ", last=" + (last == null ? "<absent>" : last.status()));
    }

    public static boolean awaitQuiet(TaskGovernor governor, Duration timeout)
            throws InterruptedException {
        return governor.awaitQuiet(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    public static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

}

