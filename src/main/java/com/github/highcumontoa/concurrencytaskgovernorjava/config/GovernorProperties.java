package com.github.highcumontoa.concurrencytaskgovernorjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 任务执行器的可调参数。
 *
 * <ul>
 *   <li>maxConcurrency：全局同时执行的任务数上限（硬上限，任何时刻不会超过）。</li>
 *   <li>maxQueuedPerSource：单个来源允许排队的任务数上限，超过即明确拒绝。</li>
 *   <li>maxQueuedGlobal：全局排队任务数上限，超过即明确拒绝。</li>
 *   <li>defaultTimeoutMillis：提交方未指定超时时间时使用的默认截止时长。</li>
 *   <li>maxTimeoutMillis：提交方可申请的最大截止时长。</li>
 *   <li>retainedTerminalTasks：终态任务（成功/失败/取消/超时）在内存中的保留条数，
 *       仅用于控制内存占用；活动任务永不被清理。幂等去重在该保留窗口内保证有效。</li>
 * </ul>
 */
@ConfigurationProperties(prefix = "governor")
public class GovernorProperties {

    /** 全局执行并发硬上限。 */
    private int maxConcurrency = 4;

    /** 单个来源的排队容量上限。 */
    private int maxQueuedPerSource = 8;

    /** 全局排队容量上限。 */
    private int maxQueuedGlobal = 64;

    /** 未显式指定超时时使用的默认值（毫秒）。 */
    private long defaultTimeoutMillis = 5_000L;

    /** 允许申请的最大超时（毫秒）。 */
    private long maxTimeoutMillis = 60_000L;

    /** 终态任务保留条数；超过后最老的终态任务会被回收。 */
    private int retainedTerminalTasks = 1_000;

    public int getMaxConcurrency() {
        return maxConcurrency;
    }

    public void setMaxConcurrency(int maxConcurrency) {
        this.maxConcurrency = maxConcurrency;
    }

    public int getMaxQueuedPerSource() {
        return maxQueuedPerSource;
    }

    public void setMaxQueuedPerSource(int maxQueuedPerSource) {
        this.maxQueuedPerSource = maxQueuedPerSource;
    }

    public int getMaxQueuedGlobal() {
        return maxQueuedGlobal;
    }

    public void setMaxQueuedGlobal(int maxQueuedGlobal) {
        this.maxQueuedGlobal = maxQueuedGlobal;
    }

    public long getDefaultTimeoutMillis() {
        return defaultTimeoutMillis;
    }

    public void setDefaultTimeoutMillis(long defaultTimeoutMillis) {
        this.defaultTimeoutMillis = defaultTimeoutMillis;
    }

    public long getMaxTimeoutMillis() {
        return maxTimeoutMillis;
    }

    public void setMaxTimeoutMillis(long maxTimeoutMillis) {
        this.maxTimeoutMillis = maxTimeoutMillis;
    }

    public int getRetainedTerminalTasks() {
        return retainedTerminalTasks;
    }

    public void setRetainedTerminalTasks(int retainedTerminalTasks) {
        this.retainedTerminalTasks = retainedTerminalTasks;
    }
}
