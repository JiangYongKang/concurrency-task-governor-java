package com.github.highcumontoa.concurrencytaskgovernorjava.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 按 {@code taskType} 查找内置处理器的注册表。
 *
 * <p>所有 {@link TaskHandler} 由 Spring 收集后按 {@link TaskHandler#type()} 建索引，
 * 类型重复时启动即失败（fail-fast），避免运行期出现不确定的处理器选择。</p>
 */
@Component
public class TaskHandlerRegistry {

    private static final Logger log = LoggerFactory.getLogger(TaskHandlerRegistry.class);

    private final Map<String, TaskHandler> handlers;

    public TaskHandlerRegistry(List<TaskHandler> handlers) {
        this.handlers = handlers.stream()
                .collect(Collectors.toUnmodifiableMap(
                        h -> h.type().toLowerCase(),
                        Function.identity(),
                        (a, b) -> {
                            throw new IllegalStateException(
                                    "duplicate task handler type: " + a.type());
                        }));
        log.info("TaskHandlerRegistry initialized with types: {}", this.handlers.keySet());
    }

    public Optional<TaskHandler> find(String type) {
        if (type == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(handlers.get(type.toLowerCase()));
    }
}
