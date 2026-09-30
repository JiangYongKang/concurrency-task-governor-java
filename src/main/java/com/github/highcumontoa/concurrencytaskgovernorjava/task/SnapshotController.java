package com.github.highcumontoa.concurrencytaskgovernorjava.task;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Read-only occupancy endpoint.
 *
 * <ul>
 *   <li>{@code GET /api/snapshot} — 200 {@link GovernorSnapshot}: running /
 *       queued / in-flight totals plus per-source breakdown. Never executes
 *       tasks and never mutates task state.</li>
 * </ul>
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
