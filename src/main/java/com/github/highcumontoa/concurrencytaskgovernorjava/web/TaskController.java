package com.github.highcumontoa.concurrencytaskgovernorjava.web;

import com.github.highcumontoa.concurrencytaskgovernorjava.core.TaskGovernor;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.FailureReason;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.SubmitOutcome;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.SubmitResult;
import com.github.highcumontoa.concurrencytaskgovernorjava.domain.TaskSnapshot;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.TaskHandler;
import com.github.highcumontoa.concurrencytaskgovernorjava.task.TaskHandlerRegistry;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 受控后台任务 HTTP 接口。
 *
 * <ul>
 *   <li>POST   /api/tasks             提交任务（201 受理 / 200 重复 / 400 非法输入 / 429 / 503）。</li>
 *   <li>GET    /api/tasks/{key}       按任务 id 或幂等键查询状态（404 表示未知标识）。</li>
 *   <li>POST   /api/tasks/{key}/cancel 请求取消（404 表示未知标识；终态任务返回原状）。</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/tasks")
public class TaskController {

    private final TaskGovernor governor;
    private final TaskHandlerRegistry registry;

    public TaskController(TaskGovernor governor, TaskHandlerRegistry registry) {
        this.governor = governor;
        this.registry = registry;
    }

    @PostMapping
    public ResponseEntity<SubmitTaskResponse> submit(@RequestBody SubmitTaskRequest request) {
        if (request == null) {
            throw new ApiException(FailureReason.INVALID_INPUT, HttpStatus.BAD_REQUEST,
                    "request body is required");
        }
        TaskHandler handler = registry.find(request.taskType())
                .orElseThrow(() -> new ApiException(FailureReason.INVALID_INPUT, HttpStatus.BAD_REQUEST,
                        "unknown taskType: " + request.taskType()));

        SubmitResult result = governor.submit(
                request.source(),
                request.idempotencyKey(),
                request.taskType(),
                request.timeoutMillis(),
                handler.action(request.payload()));

        TaskResponse task = TaskResponse.from(result.snapshot());
        SubmitTaskResponse body = new SubmitTaskResponse(
                result.outcome().name(), result.message(), task);

        return switch (result.outcome()) {
            case ACCEPTED -> ResponseEntity.status(HttpStatus.CREATED).body(body);
            case DUPLICATE -> ResponseEntity.ok(body);
            // 所有拒绝统一为 ErrorResponse 错误契约（稳定原因码 + 可解释说明），
            // 不返回任务实体，避免调用方误以为任务已被登记。
            case REJECTED -> throw new ApiException(
                    result.snapshot().failureReason(),
                    rejectionStatus(result.snapshot().failureReason()),
                    result.message());
        };
    }

    @GetMapping("/{key}")
    public TaskResponse get(@PathVariable String key) {
        return governor.find(key)
                .map(TaskResponse::from)
                .orElseThrow(() -> new ApiException(FailureReason.NOT_FOUND, HttpStatus.NOT_FOUND,
                        "no task found for id or idempotencyKey: " + key));
    }

    @PostMapping("/{key}/cancel")
    public TaskResponse cancel(@PathVariable String key) {
        return governor.cancel(key)
                .map(TaskResponse::from)
                .orElseThrow(() -> new ApiException(FailureReason.NOT_FOUND, HttpStatus.NOT_FOUND,
                        "no task found for id or idempotencyKey: " + key));
    }

    private HttpStatus rejectionStatus(FailureReason reason) {
        if (reason == FailureReason.CAPACITY_EXHAUSTED) {
            // 容量不足是暂时状态：客户端可重试。
            return HttpStatus.TOO_MANY_REQUESTS;
        }
        if (reason == FailureReason.SERVICE_SHUTDOWN) {
            return HttpStatus.SERVICE_UNAVAILABLE;
        }
        if (reason == FailureReason.INVALID_INPUT) {
            return HttpStatus.BAD_REQUEST;
        }
        return HttpStatus.CONFLICT;
    }
}
