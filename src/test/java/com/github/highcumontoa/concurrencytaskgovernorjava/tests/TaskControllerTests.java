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

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * HTTP-level tests: stable ids, distinguishable submission outcomes mapped to
 * distinct status codes, and idempotent cancellation.
 */
@SpringBootTest(properties = {
        "task-governor.max-concurrency=1",
        "task-governor.max-queue-size=0",
        "task-governor.per-source-max-in-flight=10",
        "task-governor.default-timeout-ms=5000"
})
@AutoConfigureMockMvc
class TaskControllerTests {

    private static final Logger log = LoggerFactory.getLogger(TaskControllerTests.class);

    @Autowired
    private MockMvc mockMvc;

    @Test
    void submitReturnsStableIdAndDuplicateReturnsSameTask() throws Exception {
        MvcResult first = mockMvc.perform(post("/api/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"idempotencyKey\":\"http-dup\",\"source\":\"http-test\","
                                + "\"type\":\"echo\",\"payload\":\"hello\"}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.id").isNotEmpty())
                .andReturn();
        String id = JsonPath.read(first.getResponse().getContentAsString(), "$.id");
        log.info("[http] accepted task id={}", id);

        MvcResult duplicate = mockMvc.perform(post("/api/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"idempotencyKey\":\"http-dup\",\"source\":\"http-test\","
                                + "\"type\":\"echo\",\"payload\":\"hello\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id", is(id)))
                .andReturn();
        log.info("[http] duplicate returned same id={} (judgment: 200 + identical id)",
                (String) JsonPath.read(duplicate.getResponse().getContentAsString(), "$.id"));

        mockMvc.perform(get("/api/tasks/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id", is(id)))
                .andExpect(jsonPath("$.status", anyOf(is("QUEUED"), is("RUNNING"), is("SUCCEEDED"))));
    }

    @Test
    void invalidSubmissionYields400() throws Exception {
        mockMvc.perform(post("/api/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"idempotencyKey\":\" \",\"source\":\"s\",\"type\":\"echo\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.outcome", is("REJECTED_INVALID")));
        log.info("[http] judgment: blank idempotencyKey -> 400 REJECTED_INVALID");
    }

    @Test
    void capacityExhaustionYields429() throws Exception {
        // occupy the single worker with a slow sleep task
        mockMvc.perform(post("/api/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"idempotencyKey\":\"http-slow\",\"source\":\"cap-src\","
                                + "\"type\":\"sleep\",\"payload\":\"800\",\"timeoutMs\":5000}"))
                .andExpect(status().isAccepted());

        // wait until it is running so the only slot is certainly taken
        String slowId = null;
        for (int i = 0; i < 50; i++) {
            MvcResult res = mockMvc.perform(post("/api/tasks")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"idempotencyKey\":\"http-slow\",\"source\":\"cap-src\","
                                    + "\"type\":\"sleep\"}"))
                    .andReturn();
            slowId = JsonPath.read(res.getResponse().getContentAsString(), "$.id");
            MvcResult view = mockMvc.perform(get("/api/tasks/{id}", slowId)).andReturn();
            String state = JsonPath.read(view.getResponse().getContentAsString(), "$.status");
            if ("RUNNING".equals(state)) {
                break;
            }
            Thread.sleep(20);
        }

        mockMvc.perform(post("/api/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"idempotencyKey\":\"http-overflow\",\"source\":\"cap-src\","
                                + "\"type\":\"echo\"}"))
                .andExpect(status().isTooManyRequests())
                .andExpect(jsonPath("$.outcome", is("REJECTED_CAPACITY")));
        log.info("[http] judgment: slot occupied -> 429 REJECTED_CAPACITY (slow task id={})", slowId);
    }

    @Test
    void cancelIsIdempotentAndUnknownIdsAre404() throws Exception {
        // capacity is 1 slot shared across tests in this context; retry until free
        String id = null;
        for (int i = 0; i < 100; i++) {
            MvcResult attempt = mockMvc.perform(post("/api/tasks")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"idempotencyKey\":\"http-cancel\",\"source\":\"http-test\","
                                    + "\"type\":\"sleep\",\"payload\":\"2000\",\"timeoutMs\":10000}"))
                    .andReturn();
            if (attempt.getResponse().getStatus() == 202) {
                id = JsonPath.read(attempt.getResponse().getContentAsString(), "$.id");
                break;
            }
            if (attempt.getResponse().getStatus() == 200) {
                // leftover from a previous run of this test: reuse it
                id = JsonPath.read(attempt.getResponse().getContentAsString(), "$.id");
                break;
            }
            Thread.sleep(100);
        }
        log.info("[http] cancel target task id={}", id);

        mockMvc.perform(post("/api/tasks/{id}/cancel", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("CANCELLED")))
                .andExpect(jsonPath("$.failureReason", is("CANCELLED")));
        // second cancel: still CANCELLED, no error
        mockMvc.perform(post("/api/tasks/{id}/cancel", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("CANCELLED")));
        log.info("[http] judgment: task={} cancel is idempotent, terminal state stable", id);

        mockMvc.perform(get("/api/tasks/{id}", "no-such-id"))
                .andExpect(status().isNotFound());
        mockMvc.perform(post("/api/tasks/{id}/cancel", "no-such-id"))
                .andExpect(status().isNotFound());
        log.info("[http] judgment: unknown id -> 404 for both query and cancel");
    }
}
