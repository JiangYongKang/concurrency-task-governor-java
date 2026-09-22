package com.github.highcumontoa.concurrencytaskgovernorjava.task;

import com.github.highcumontoa.concurrencytaskgovernorjava.domain.TaskAction;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 服务内置的三种任务类型，便于通过 HTTP 演示并在测试中制造各类时序：
 * <ul>
 *   <li>{@code delay}：休眠 payload 指定的毫秒数（响应中断，用于超时/取消测试）。</li>
 *   <li>{@code echo}：立即回显 payload。</li>
 *   <li>{@code failing}：按 payload 指定的毫秒数延迟后抛出异常（用于失败归一化测试）。</li>
 * </ul>
 */
public final class BuiltinTaskHandlers {

    private BuiltinTaskHandlers() {
    }

    /** payload 形如 "120"（毫秒），缺省 50ms。 */
    @Component
    public static class DelayHandler implements TaskHandler {

        @Override
        public String type() {
            return "delay";
        }

        @Override
        public TaskAction action(String payload) {
            long sleepMillis = parseMillis(payload, 50L);
            return (TaskContext ctx) -> {
                sleepRespondingToCancellation(sleepMillis, ctx);
                return "slept " + sleepMillis + "ms";
            };
        }
    }

    /** 立即完成并返回 payload 原文。 */
    @Component
    public static class EchoHandler implements TaskHandler {

        @Override
        public String type() {
            return "echo";
        }

        @Override
        public TaskAction action(String payload) {
            return ctx -> "echo:" + (payload == null ? "" : payload);
        }
    }

    /** payload 形如 "20:boom"，先等待 20ms 再抛出以 boom 为消息的 IllegalStateException。 */
    @Component
    public static class FailingHandler implements TaskHandler {

        private static final Logger log = LoggerFactory.getLogger(FailingHandler.class);

        @Override
        public String type() {
            return "failing";
        }

        @Override
        public TaskAction action(String payload) {
            String p = payload == null ? "" : payload;
            String[] parts = p.split(":", 2);
            long delayMillis = parseMillis(parts[0], 10L);
            String message = parts.length > 1 && !parts[1].isBlank() ? parts[1] : "forced failure";
            return (TaskContext ctx) -> {
                sleepRespondingToCancellation(delayMillis, ctx);
                log.debug("failing task {} raising simulated exception", ctx.taskId());
                throw new IllegalStateException(message);
            };
        }
    }

    private static long parseMillis(String raw, long fallback) {
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            long value = Long.parseLong(raw.trim());
            return value < 0 ? fallback : value;
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    /**
     * 按小粒度睡眠，及时响应中断与协作式取消请求——
     * 这是任务能够在超时/取消时及时释放执行线程的关键。
     */
    static void sleepRespondingToCancellation(long totalMillis, TaskContext ctx)
            throws InterruptedException {
        long end = System.currentTimeMillis() + totalMillis;
        while (true) {
            if (ctx.cancellationRequested()) {
                throw new InterruptedException("cancellation requested by caller");
            }
            long remaining = end - System.currentTimeMillis();
            if (remaining <= 0) {
                return;
            }
            Thread.sleep(Math.min(remaining, 20L));
        }
    }
}
