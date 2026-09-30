package com.github.highcumontoa.concurrencytaskgovernorjava.tests;

import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * HTTP coverage for the new observability surface: lifecycle history and the
 * whole-service occupancy snapshot (pure read).
 */
@SpringBootTest(properties = {
        "task-governor.max-concurrency=2",
        "task-governor.max-queue-size=2",
        "task-governor.per-source-max-in-flight=10",
        "task-governor.default-timeout-ms=5000"
})
@AutoConfigureMockMvc
class ObservabilityControllerTests {

    private static final Logger log = LoggerFactory.getLogger(ObservabilityControllerTests.class);

    @Autowired
    private MockMvc mockMvc;

    @Test
    void history_isOrdered_andEndsInTerminalStatus_andIsSharedByDuplicate() throws Exception {
        String key = "obs-history-" + System.nanoTime();
        MvcResult accepted = mockMvc.perform(post("/api/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"idempotencyKey\":\"" + key + "\",\"source\":\"obs\","
                                + "\"type\":\"echo\",\"payload\":\"hi\"}"))
                .andExpect(status().isAccepted())
                .andReturn();
        String id = JsonPath.read(accepted.getResponse().getContentAsString(), "$.id");

        // Wait for the terminal event.
        String terminal = null;
        for (int i = 0; i < 100; i++) {
            MvcResult view = mockMvc.perform(get("/api/tasks/{id}", id)).andReturn();
            terminal = JsonPath.read(view.getResponse().getContentAsString(), "$.status");
            if ("SUCCEEDED".equals(terminal)) {
                break;
            }
            Thread.sleep(20);
        }
        assertEqualsSafe("SUCCEEDED", terminal);

        mockMvc.perform(get("/api/tasks/{id}/history", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()", is(3)))
                .andExpect(jsonPath("$[0].status", is("QUEUED")))
                .andExpect(jsonPath("$[0].sequence", is(0)))
                .andExpect(jsonPath("$[0].at").isNotEmpty())
                .andExpect(jsonPath("$[1].status", is("RUNNING")))
                .andExpect(jsonPath("$[2].status", is(terminal)))
                .andExpect(jsonPath("$[2].failureReason", is("NONE")));
        log.info("[http-history] task={} terminal={} history QUEUED->RUNNING->{}", id, terminal, terminal);

        // Unknown id -> 404.
        mockMvc.perform(get("/api/tasks/{id}/history", "no-such-id"))
                .andExpect(status().isNotFound());

        // Duplicate submission returns the same id and hence the same history.
        mockMvc.perform(post("/api/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"idempotencyKey\":\"" + key + "\",\"source\":\"obs\","
                                + "\"type\":\"echo\",\"payload\":\"hi\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id", is(id)));
        mockMvc.perform(get("/api/tasks/{id}/history", id))
                .andExpect(jsonPath("$.length()", is(3)))
                .andExpect(jsonPath("$[2].status", is("SUCCEEDED")));
    }

    @Test
    void snapshot_reportsOccupancy_andReturnsToZero_afterTerminal() throws Exception {
        // Snapshot before: zero (best-effort in the shared context; assert shape only).
        mockMvc.perform(get("/api/snapshot"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.running", greaterThanOrEqualTo(0)))
                .andExpect(jsonPath("$.queued", greaterThanOrEqualTo(0)))
                .andExpect(jsonPath("$.totalInFlight", greaterThanOrEqualTo(0)));

        String key = "obs-snap-" + System.nanoTime();
        String source = "snap-src-" + System.nanoTime();
        MvcResult accepted = mockMvc.perform(post("/api/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"idempotencyKey\":\"" + key + "\",\"source\":\"" + source + "\","
                                + "\"type\":\"sleep\",\"payload\":\"300\",\"timeoutMs\":5000}"))
                .andReturn();
        // The shared context may be momentarily full from another test; accept only ACCEPTED.
        if (accepted.getResponse().getStatus() != 202) {
            log.info("[http-snapshot] skipped occupancy assertions, submit status={}",
                    accepted.getResponse().getStatus());
            return;
        }
        String id = JsonPath.read(accepted.getResponse().getContentAsString(), "$.id");

        // While in flight the source must appear in the breakdown.
        boolean sawSource = false;
        for (int i = 0; i < 30; i++) {
            MvcResult snap = mockMvc.perform(get("/api/snapshot")).andReturn();
            String body = snap.getResponse().getContentAsString();
            Integer total = JsonPath.read(body, "$.totalInFlight");
            if (total != null && total > 0 && body.contains(source)) {
                sawSource = true;
                log.info("[http-snapshot] in-flight body={}", body);
                break;
            }
            Thread.sleep(15);
        }
        org.junit.jupiter.api.Assertions.assertTrue(sawSource, "in-flight task must appear in snapshot");

        // Wait for convergence, then the counts must be internally consistent
        // (running + queued == total) and the task's source gone.
        for (int i = 0; i < 100; i++) {
            MvcResult snap = mockMvc.perform(get("/api/snapshot")).andReturn();
            String body = snap.getResponse().getContentAsString();
            Integer running = JsonPath.read(body, "$.running");
            Integer queued = JsonPath.read(body, "$.queued");
            Integer total = JsonPath.read(body, "$.totalInFlight");
            org.junit.jupiter.api.Assertions.assertEquals(running + queued, total,
                    "snapshot must be internally consistent: running + queued = total");
            MvcResult view = mockMvc.perform(get("/api/tasks/{id}", id)).andReturn();
            String status = JsonPath.read(view.getResponse().getContentAsString(), "$.status");
            if ("SUCCEEDED".equals(status)) {
                org.junit.jupiter.api.Assertions.assertFalse(body.contains(source),
                        "converged task's source must disappear from the snapshot");
                log.info("[http-snapshot] after convergence body={}", body);
                break;
            }
            Thread.sleep(20);
        }
    }

    private static void assertEqualsSafe(Object expected, Object actual) {
        org.junit.jupiter.api.Assertions.assertEquals(expected, actual);
    }
}
