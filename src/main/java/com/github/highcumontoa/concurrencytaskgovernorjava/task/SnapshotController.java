package com.github.highcumontoa.concurrencytaskgovernorjava.task;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Read-only whole-service occupancy endpoint.
 *
 * <p>{@code GET /api/snapshot} returns running / queued / total in-flight counts
 * plus a per-source breakdown. Taking the snapshot never executes a task and
 * never changes any task state.
 */
@RestController
public class SnapshotController {

    private final TaskGovernor governor;

    public SnapshotController(TaskGovernor governor) {
        this.governor = governor;
    }

    @GetMapping("/api/snapshot")
    public ResponseEntity<GovernorSnapshot> snapshot() {
        return ResponseEntity.ok(governor.snapshot());
    }
}
