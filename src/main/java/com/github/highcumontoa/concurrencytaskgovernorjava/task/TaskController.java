package com.github.highcumontoa.concurrencytaskgovernorjava.task;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST facade over {@link TaskGovernor}.
 *
 * <ul>
 *   <li>{@code POST /api/tasks} — 202 accepted, 200 duplicate (existing task),
 *       400 invalid input, 429 capacity/source-limit rejection</li>
 *   <li>{@code GET /api/tasks/{id}} — 200 task view, 404 unknown id</li>
 *   <li>{@code POST /api/tasks/{id}/cancel} — 200 task view after cancel attempt,
 *       404 unknown id. A returned terminal status means full convergence: the
 *       handler stopped and all occupancy was released already</li>
 *   <li>{@code GET /api/tasks/{id}/history} — 200 ordered lifecycle events,
 *       404 unknown id</li>
 *   <li>{@code GET /api/snapshot} — 200 whole-service occupancy (running /
 *       queued / total, per-source breakdown); pure read, never runs a task</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/tasks")
public class TaskController {

    private final TaskGovernor governor;

    public TaskController(TaskGovernor governor) {
        this.governor = governor;
    }

    @PostMapping
    public ResponseEntity<?> submit(@RequestBody(required = false) TaskRequest request) {
        SubmissionResult result = governor.submit(request);
        return switch (result.outcome()) {
            case ACCEPTED -> ResponseEntity.status(HttpStatus.ACCEPTED).body(TaskView.of(result.record()));
            case DUPLICATE -> ResponseEntity.ok(TaskView.of(result.record()));
            case REJECTED_INVALID -> ResponseEntity.badRequest().body(error(result));
            case REJECTED_CAPACITY, REJECTED_SOURCE_LIMIT ->
                    ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body(error(result));
        };
    }

    @GetMapping("/{id}")
    public ResponseEntity<TaskView> get(@PathVariable String id) {
        return governor.findById(id)
                .map(record -> ResponseEntity.ok(TaskView.of(record)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping("/{id}/cancel")
    public ResponseEntity<TaskView> cancel(@PathVariable String id) {
        return governor.cancel(id)
                .map(record -> ResponseEntity.ok(TaskView.of(record)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/{id}/history")
    public ResponseEntity<java.util.List<TaskEvent>> history(@PathVariable String id) {
        return governor.findById(id)
                .map(record -> ResponseEntity.ok(governor.history(record.getId())))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    private static ErrorView error(SubmissionResult result) {
        return new ErrorView(result.outcome().name(), result.detail());
    }

    /** Caller-safe rejection payload; never carries internal exception data. */
    public record ErrorView(String outcome, String detail) {
    }
}
