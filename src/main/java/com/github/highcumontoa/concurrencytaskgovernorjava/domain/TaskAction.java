package com.github.highcumontoa.concurrencytaskgovernorjava.domain;

/**
 * 由调用方提供的任务执行体。允许抛出任意异常，由执行器归一化处理。
 */
@FunctionalInterface
public interface TaskAction {

    String execute(TaskContext context) throws Exception;
}
