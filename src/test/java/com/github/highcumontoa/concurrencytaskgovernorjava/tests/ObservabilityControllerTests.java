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
 * HTTP tests for the lifecycle record and the occupancy snapshot endpoint.
 */
@SpringBootTest(properties = {
        "task-governor.max-concurrency=1",
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
    void snapshotEndpoint_reportsTotalsAndReturnsToZero() throws Exception {
        // empty snapshot
        MvcResult before = mockMvc.perform(get("/api/snapshot"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.running", is(0)))
                .andExpect(jsonPath("$.queued", is(0)))
                .andExpect(jsonPath("$.inFlight", is(0)))
                .andExpect(jsonPath("$.globalCapacity", is(3)))
                .andReturn();
        log.info("[http-snapshot] empty: {}", before.getResponse().getContentAsString());

        // hold the only worker with a sleep task
        mockMvc.perform(post("/api/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"idempotencyKey\":\"snap-slow\",\"source\":\"snap-src\","
                                + "\"type\":\"sleep\",\"payload\":\"1500\",\"timeoutMs\":10000}"))
                .andExpect(status().isAccepted());

        // wait until the running count is observable
        int running = 0;
        for (int i = 0; i < 50; i++) {
            MvcResult res = mockMvc.perform(get("/api/snapshot")).andReturn();
            running = JsonPath.read(res.getResponse().getContentAsString(), "$.running");
            if (running == 1) {
                mockMvc.perform(get("/api/snapshot"))
                        .andExpect(status().isOk())
                        .andExpect(jsonPath("$.running", is(1)))
                        .andExpect(jsonPath("$.inFlight", greaterThanOrEqualTo(1)))
                        .andExpect(jsonPath("$.sources[0].source", is("snap-src")))
                        .andExpect(jsonPath("$.sources[0].inFlight", greaterThanOrEqualTo(1)));
                break;
            }
            Thread.sleep(20);
        }
        org.junit.jupiter.api.Assertions.assertEquals(1, running, "sleep task should show as running");

        // wait for natural completion and verify exact zero again
        for (int i = 0; i < 100; i++) {
            MvcResult res = mockMvc.perform(get("/api/snapshot")).andReturn();
            int inFlight = JsonPath.read(res.getResponse().getContentAsString(), "$.inFlight");
            if (inFlight == 0) {
                break;
            }
            Thread.sleep(20);
        }
        mockMvc.perform(get("/api/snapshot"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.running", is(0)))
                .andExpect(jsonPath("$.queued", is(0)))
                .andExpect(jsonPath("$.inFlight", is(0)))
                .andExpect(jsonPath("$.sources").isArray());
        log.info("[http-snapshot] judgment: returned to zero after completion");
    }

    @Test
    void taskView_containsOrderedLifecycleEventsEndingAtTerminal() throws Exception {
        MvcResult accepted = mockMvc.perform(post("/api/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"idempotencyKey\":\"snap-echo\",\"source\":\"snap-src\","
                                + "\"type\":\"echo\",\"payload\":\"hi\"}"))
                .andExpect(status().is2xxSuccessful())
                .andExpect(jsonPath("$.events[0].to", is("QUEUED")))
                .andReturn();
        String id = JsonPath.read(accepted.getResponse().getContentAsString(), "$.id");

        String body = null;
        for (int i = 0; i < 100; i++) {
            MvcResult res = mockMvc.perform(get("/api/tasks/{id}", id)).andReturn();
            body = res.getResponse().getContentAsString();
            String state = JsonPath.read(body, "$.status");
            if ("SUCCEEDED".equals(state)) {
                break;
            }
            Thread.sleep(10);
        }
        log.info("[http-events] final view: {}", body);
        org.junit.jupiter.api.Assertions.assertTrue(body.contains("\"status\":\"SUCCEEDED\""));
        org.junit.jupiter.api.Assertions.assertTrue(body.matches("(?s).*\"events\":\\[.*"),
                "view must carry the events array");
        // last event ends at the same terminal state
        org.junit.jupiter.api.Assertions.assertTrue(body.contains("\"to\":\"SUCCEEDED\""),
                "terminal lifecycle event must be SUCCEEDED");

        // duplicate submission returns the same id and therefore same record
        mockMvc.perform(post("/api/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"idempotencyKey\":\"snap-echo\",\"source\":\"snap-src\","
                                + "\"type\":\"echo\",\"payload\":\"hi\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id", is(id)));
    }
}
