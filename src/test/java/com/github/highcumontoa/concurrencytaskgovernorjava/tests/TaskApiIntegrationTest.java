package com.github.highcumontoa.concurrencytaskgovernorjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static com.github.highcumontoa.concurrencytaskgovernorjava.tests.support.GovernorTestSupport.sleepQuietly;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 端到端 HTTP 测试：使用小容量配置（并发 2、单源队列 1、全局队列 2），
 * 验证受理、重复提交、非法输入、容量拒绝、查询、取消与失败归一化的完整契约。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
        "governor.max-concurrency=2",
        "governor.max-queued-per-source=1",
        "governor.max-queued-global=2",
        "governor.default-timeout-millis=600"
})
class TaskApiIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(TaskApiIntegrationTest.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @Test
    void fullLifecycle_overHttp_accept_duplicate_query_cancel() throws Exception {
        String base = "http://localhost:" + port + "/api/tasks";

        // 1) 受理一个 delay 任务
        JsonNode created = post(base, req("web-A", "web-key-1", "delay", "400", null),
                HttpStatus.CREATED);
        String taskId = created.path("task").path("taskId").asText();
        assertEquals("ACCEPTED", created.path("outcome").asText());
        assertTrue(taskId.startsWith("task-"));
        log.info("[http] accepted taskId={}", taskId);

        // 2) 重复提交：200 + DUPLICATE + 同一 taskId
        JsonNode dup = post(base, req("web-A", "web-key-1", "delay", "400", null),
                HttpStatus.OK);
        assertEquals("DUPLICATE", dup.path("outcome").asText());
        assertEquals(taskId, dup.path("task").path("taskId").asText());
        log.info("[http] duplicate -> outcome=DUPLICATE sameTaskId=true");

        // 3) 查询：可按 id
        JsonNode byId = get(base + "/" + taskId, HttpStatus.OK);
        assertEquals(taskId, byId.path("taskId").asText());
        // 可按幂等键
        JsonNode byKey = get(base + "/web-key-1", HttpStatus.OK);
        assertEquals(taskId, byKey.path("taskId").asText());

        // 4) 取消并等待收敛
        JsonNode cancelled = exchange(base + "/web-key-1/cancel", HttpMethod.POST, null, HttpStatus.OK);
        log.info("[http] cancel immediate status={}", cancelled.path("status").asText());
        JsonNode fin = awaitTerminalOverHttp(base + "/web-key-1", Duration.ofSeconds(5));
        assertEquals("CANCELLED", fin.path("status").asText());
        assertEquals("CANCELLED", fin.path("failureReason").asText());
        log.info("[http] converged CANCELLED over HTTP (executionCount={})",
                fin.path("executionCount").asInt());
    }

    @Test
    void invalidAndUnknownRequests_haveDistinguishableShapes() throws Exception {
        String base = "http://localhost:" + port + "/api/tasks";

        // 缺少幂等键 -> 400 INVALID_INPUT
        Map<String, Object> bad = req("web-B", null, "echo", "hi", null);
        JsonNode err = post(base, bad, HttpStatus.BAD_REQUEST);
        assertEquals("INVALID_INPUT", err.path("reason").asText());
        log.info("[http] missing key -> 400 INVALID_INPUT");

        // 未知任务类型 -> 400 INVALID_INPUT（原因在消息中可解释）
        JsonNode unknownType = post(base, req("web-B", "web-unk-type", "nope", "x", null),
                HttpStatus.BAD_REQUEST);
        assertEquals("INVALID_INPUT", unknownType.path("reason").asText());
        assertTrue(unknownType.path("message").asText().contains("unknown taskType"));
        log.info("[http] unknown type -> 400 INVALID_INPUT: {}", unknownType.path("message").asText());

        // 非法 JSON -> 400 INVALID_INPUT
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<String> malformed = rest.exchange(base, HttpMethod.POST,
                new HttpEntity<>("{not-json", headers), String.class);
        assertEquals(HttpStatus.BAD_REQUEST, malformed.getStatusCode());
        assertTrue(malformed.getBody().contains("INVALID_INPUT"));
        log.info("[http] malformed json -> 400 INVALID_INPUT");

        // 查询/取消未知标识 -> 404 NOT_FOUND（与非法输入区分）
        JsonNode nf = get(base + "/no-such-key", HttpStatus.NOT_FOUND);
        assertEquals("NOT_FOUND", nf.path("reason").asText());
        JsonNode nfCancel = exchange(base + "/no-such-key/cancel", HttpMethod.POST, null,
                HttpStatus.NOT_FOUND);
        assertEquals("NOT_FOUND", nfCancel.path("reason").asText());
        log.info("[http] unknown identifier -> 404 NOT_FOUND");
    }

    @Test
    void capacitySaturation_returns429_andFailingTaskIsNormalized() throws Exception {
        String base = "http://localhost:" + port + "/api/tasks";

        // 并发 2 + 单源队列 1：两个长任务占槽，第三个排队，第四个必被 429 拒绝
        post(base, req("web-C", "web-cap-1", "delay", "1000", null), HttpStatus.CREATED);
        post(base, req("web-C", "web-cap-2", "delay", "1000", null), HttpStatus.CREATED);
        sleepQuietly(80);
        post(base, req("web-C", "web-cap-3", "delay", "1000", null), HttpStatus.CREATED);
        JsonNode rejected = post(base, req("web-C", "web-cap-4", "delay", "1000", null),
                HttpStatus.TOO_MANY_REQUESTS);
        assertEquals("CAPACITY_EXHAUSTED", rejected.path("reason").asText());
        log.info("[http] saturated -> 429 CAPACITY_EXHAUSTED: {}", rejected.path("message").asText());

        // 全部取消以释放资源
        for (String k : new String[]{"web-cap-1", "web-cap-2", "web-cap-3"}) {
            exchange(base + "/" + k + "/cancel", HttpMethod.POST, null, HttpStatus.OK);
            awaitTerminalOverHttp(base + "/" + k, Duration.ofSeconds(5));
        }

        // 失败归一化：failing 任务 -> FAILED/EXECUTION_ERROR，底层异常不裸露堆栈
        post(base, req("web-D", "web-fail-1", "failing", "20:boom", 30000L), HttpStatus.CREATED);
        JsonNode fin = awaitTerminalOverHttp(base + "/web-fail-1", Duration.ofSeconds(5));
        assertEquals("FAILED", fin.path("status").asText());
        assertEquals("EXECUTION_ERROR", fin.path("failureReason").asText());
        assertTrue(fin.path("failureDetail").asText().contains("IllegalStateException"));
        log.info("[http] failing task -> FAILED/EXECUTION_ERROR detail='{}' (no raw stacktrace)",
                fin.path("failureDetail").asText());
    }

    // ---------------- helpers ----------------

    private Map<String, Object> req(String source, String key, String type, String payload,
                                    Long timeoutMillis) {
        Map<String, Object> m = new HashMap<>();
        m.put("source", source);
        m.put("idempotencyKey", key);
        m.put("taskType", type);
        m.put("payload", payload);
        if (timeoutMillis != null) {
            m.put("timeoutMillis", timeoutMillis);
        }
        return m;
    }

    private JsonNode post(String url, Map<String, Object> body, HttpStatus expected) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<String> resp = rest.exchange(url, HttpMethod.POST,
                new HttpEntity<>(body, headers), String.class);
        assertEquals(expected, resp.getStatusCode(),
                "unexpected status: " + resp.getStatusCode() + " body=" + resp.getBody());
        return parse(resp.getBody());
    }

    private JsonNode get(String url, HttpStatus expected) {
        return exchange(url, HttpMethod.GET, null, expected);
    }

    private JsonNode exchange(String url, HttpMethod method, Object body, HttpStatus expected) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<String> resp = rest.exchange(url, method,
                new HttpEntity<>(body == null ? "" : body, headers), String.class);
        assertEquals(expected, resp.getStatusCode(),
                "unexpected status: " + resp.getStatusCode() + " body=" + resp.getBody());
        return parse(resp.getBody());
    }

    private JsonNode parse(String body) {
        try {
            return MAPPER.readTree(body);
        } catch (Exception e) {
            throw new AssertionError("invalid JSON response: " + body, e);
        }
    }

    private JsonNode awaitTerminalOverHttp(String url, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        JsonNode last = null;
        while (System.nanoTime() < deadline) {
            ResponseEntity<String> resp = rest.getForEntity(url, String.class);
            last = parse(resp.getBody());
            String status = last.path("status").asText();
            if (status.equals("SUCCEEDED") || status.equals("FAILED")
                    || status.equals("CANCELLED") || status.equals("TIMED_OUT")) {
                return last;
            }
            Thread.sleep(10L);
        }
        throw new AssertionError("did not converge over HTTP: " + (last == null ? "?" : last));
    }
}
