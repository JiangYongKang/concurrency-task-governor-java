package com.github.highcumontoa.concurrencytaskgovernorjava.task;

import com.github.highcumontoa.concurrencytaskgovernorjava.domain.TaskAction;

/**
 * 具名任务处理器：按 taskType 注册，供服务内置任务类型使用。
 */
public interface TaskHandler {

    String type();

    TaskAction action(String payload);
}
