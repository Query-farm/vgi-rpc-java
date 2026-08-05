// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The parts of the access-log contract the JSON schema cannot express.
 *
 * <p>{@code vgi-rpc-test --access-log} checks record <em>shape</em>; nothing in
 * a schema can say that two records of one stream shared a sampling decision,
 * that a dropped record was reported, or that a redactor which threw dropped the
 * claims instead of leaking them.
 */
final class AccessLogHookTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    // ---- sampling --------------------------------------------------------

    /**
     * Rule 2 of §5bb: the decision is a function of the call, not of the record.
     * Random per-record sampling shreds a multi-record stream into fragments
     * indistinguishable from data loss.
     */
    @Test
    void sampling_decides_once_per_stream_not_per_record() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        AccessLogHook hook = AccessLogHook.builder(out).sampleRate(0.5).build();

        // Twenty stream ids, ten records each. Whatever the coin said for a
        // stream, it must have said for every record of it.
        Map<String, Integer> kept = new LinkedHashMap<>();
        for (int s = 0; s < 20; s++) {
            String streamId = String.format("%032x", s);
            for (int r = 0; r < 10; r++) emit(hook, streamRecord(streamId), null);
            kept.put(streamId, 0);
        }
        for (JsonNode rec : records(out)) {
            kept.merge(rec.get("stream_id").asText(), 1, Integer::sum);
        }
        for (Map.Entry<String, Integer> e : kept.entrySet()) {
            assertTrue(e.getValue() == 0 || e.getValue() == 10,
                    "stream " + e.getKey() + " was split: " + e.getValue() + " of 10 records kept");
        }
        assertTrue(kept.values().stream().anyMatch(v -> v == 10), "sampling kept nothing at all at rate 0.5");
        assertTrue(kept.values().stream().anyMatch(v -> v == 0), "sampling dropped nothing at all at rate 0.5");
    }

    /** The same identifier must survive a restart with the same decision. */
    @Test
    void sampling_is_stable_across_hook_instances() throws Exception {
        List<String> ids = new ArrayList<>();
        for (int s = 0; s < 40; s++) ids.add(String.format("%032x", s));

        assertEquals(sampleOnce(ids), sampleOnce(ids),
                "the same stream ids must survive two independent hooks with the same fate");
    }

    /**
     * Rule 1 of §5bb. A rate below 1 exists because successful calls repeat,
     * which is exactly what failures do not.
     */
    @Test
    void errors_are_never_sampled_out() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // Low enough that no successful call survives, so anything in the log
        // got there by bypassing the decision rather than by luck.
        AccessLogHook hook = AccessLogHook.builder(out).sampleRate(0.000001).build();

        for (int s = 0; s < 50; s++) emit(hook, streamRecord(String.format("%032x", s)), null);
        for (int s = 50; s < 100; s++) {
            emit(hook, streamRecord(String.format("%032x", s)), new IllegalStateException("boom"));
        }

        List<JsonNode> records = records(out);
        assertEquals(50, records.size(), "every error and no success should have been kept");
        for (JsonNode rec : records) assertEquals("error", rec.get("status").asText());
    }

    /** Rule 3 of §5bb: a consumer counting calls has to divide by the rate. */
    @Test
    void every_sampled_in_record_carries_the_rate() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        AccessLogHook hook = AccessLogHook.builder(out).sampleRate(0.5).build();
        for (int s = 0; s < 40; s++) emit(hook, streamRecord(String.format("%032x", s)), null);

        List<JsonNode> records = records(out);
        assertFalse(records.isEmpty());
        for (JsonNode rec : records) {
            assertEquals(0.5, rec.get("sample_rate").asDouble(), 1e-9);
        }
    }

    /** Unsampled servers must not emit the field at all. */
    @Test
    void rate_one_emits_no_sample_rate() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        AccessLogHook hook = AccessLogHook.builder(out).build();
        emit(hook, unaryRecord(), null);
        assertFalse(records(out).get(0).has("sample_rate"));
    }

    /** A rate of {@code 100} meaning "100%" would otherwise silently log everything. */
    @Test
    void an_out_of_range_rate_fails_at_startup() {
        AccessLogHook.Builder b = AccessLogHook.builder(new ByteArrayOutputStream());
        assertThrows(IllegalArgumentException.class, () -> b.sampleRate(100));
        assertThrows(IllegalArgumentException.class, () -> b.sampleRate(0.0));
        assertThrows(IllegalArgumentException.class, () -> b.sampleRate(-0.5));
        assertThrows(IllegalArgumentException.class, () -> b.sampleRate(Double.NaN));
    }

    // ---- truncation markers ----------------------------------------------

    /**
     * §5b: {@code "payload_omitted"} means nothing was lost to a cap. Sharing the
     * size-driven {@code true} made the marker fire on nearly every record and
     * left a consumer scanning for real data loss with nothing to filter on.
     */
    @Test
    void omitting_payloads_is_not_reported_as_truncation() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        emit(AccessLogHook.builder(out).logPayloads(false).build(), unaryRecord(), null);

        JsonNode rec = records(out).get(0);
        assertEquals("payload_omitted", rec.get("truncated").asText());
        assertFalse(rec.get("truncated").isBoolean(), "payload omission must not read as size-driven shedding");
        assertFalse(rec.has("request_data"));
        assertTrue(rec.get("original_request_bytes").asInt() > 0);
    }

    /** The default logs payloads, and a record that lost nothing carries no marker. */
    @Test
    void logging_payloads_emits_no_truncation_marker() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        emit(AccessLogHook.builder(out).build(), unaryRecord(), null);

        JsonNode rec = records(out).get(0);
        assertFalse(rec.has("truncated"));
        assertEquals("AQIDBA==", rec.get("request_data").asText());
    }

    // ---- claim redaction -------------------------------------------------

    /** Key-based, and keys survive: "did this token carry an email claim" stays answerable. */
    @Test
    void claims_are_redacted_by_key_and_keys_are_kept() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DispatchInfo info = unaryRecord();
        info.claims = new LinkedHashMap<>(Map.of(
                "sub", "user-42",
                "email", "alice@example.com",
                "api_key", "sk-live-abcdef",
                "role", "admin",
                "context", "reached alice@example.com"));

        emit(AccessLogHook.builder(out).build(), info, null);
        JsonNode claims = records(out).get(0).get("claims");

        assertEquals("[redacted]", claims.get("email").asText());
        assertEquals("[redacted]", claims.get("api_key").asText());
        assertEquals("user-42", claims.get("sub").asText());
        assertEquals("admin", claims.get("role").asText());
        // The stated boundary: content is never inspected, only names.
        assertEquals("reached alice@example.com", claims.get("context").asText());
    }

    /** Fail closed: unredacted claims on disk cannot be recalled. */
    @Test
    void a_redactor_that_throws_drops_the_claims() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DispatchInfo info = unaryRecord();
        info.claims = new LinkedHashMap<>(Map.of("email", "alice@example.com"));

        emit(AccessLogHook.builder(out)
                .claimRedactor(claims -> { throw new IllegalStateException("policy lookup failed"); })
                .build(), info, null);

        String line = out.toString();
        assertFalse(records(out).get(0).has("claims"), "a broken redactor must fail closed, not open");
        assertFalse(line.contains("alice@example.com"));
    }

    /** The opt-out, for services that own their logs end to end. */
    @Test
    void redaction_can_be_disabled() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DispatchInfo info = unaryRecord();
        info.claims = new LinkedHashMap<>(Map.of("email", "alice@example.com"));

        emit(AccessLogHook.builder(out).claimRedactor(ClaimRedactor.none()).build(), info, null);
        assertEquals("alice@example.com", records(out).get(0).get("claims").get("email").asText());
    }

    // ---- trace correlation -----------------------------------------------

    /** Both or neither — a record carrying one half joins nothing. */
    @Test
    void trace_ids_are_emitted_as_a_pair() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String traceId = "4bf92f3577b34da6a3ce929d0e0e4736";
        String spanId = "00f067aa0ba902b7";
        emit(AccessLogHook.builder(out)
                .traceCorrelator(() -> new String[] {traceId, spanId})
                .build(), unaryRecord(), null);

        JsonNode rec = records(out).get(0);
        assertEquals(traceId, rec.get("trace_id").asText());
        assertEquals(spanId, rec.get("span_id").asText());
    }

    /** A dashed UUID would fail schema validation for every record the server writes. */
    @Test
    void a_malformed_trace_id_is_dropped_rather_than_emitted() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        emit(AccessLogHook.builder(out)
                .traceCorrelator(() -> new String[] {"4bf92f35-77b3-4da6-a3ce-929d0e0e4736", "00f067aa0ba902b7"})
                .build(), unaryRecord(), null);

        JsonNode rec = records(out).get(0);
        assertFalse(rec.has("trace_id"));
        assertFalse(rec.has("span_id"), "half a trace context is worse than none");
    }

    /** A correlator that throws must not fail the call. */
    @Test
    void a_throwing_correlator_is_survivable() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        emit(AccessLogHook.builder(out)
                .traceCorrelator(() -> { throw new IllegalStateException("no context"); })
                .build(), unaryRecord(), null);
        assertFalse(records(out).get(0).has("trace_id"));
    }

    // ---- asynchronous emission -------------------------------------------

    /**
     * §5bc: full means drop, and the drop is reported. A log that loses records
     * without saying so is worse than a slow one, because a consumer cannot tell
     * a quiet period from a lossy one.
     */
    @Test
    void a_full_queue_drops_and_reports_what_it_dropped() throws Exception {
        GateStream gate = new GateStream();
        AccessLogHook hook = AccessLogHook.builder(gate).asyncQueueSize(1).build();
        try {
            // The writer thread takes this one and parks inside write(), leaving
            // the queue empty and the drain deterministically stalled.
            emit(hook, named("first"), null);
            assertTrue(gate.entered.await(10, TimeUnit.SECONDS), "writer thread never reached the sink");

            emit(hook, named("second"));    // fills the one-deep queue
            emit(hook, named("dropped-a")); // no room
            emit(hook, named("dropped-b")); // no room

            gate.release.countDown();
            awaitLines(gate, 2);

            emit(hook, named("after"));
        } finally {
            hook.close();
        }

        List<JsonNode> records = records(gate.sink);
        assertEquals(3, records.size(), "two records should have been dropped, not queued");
        assertEquals(List.of("first", "second", "after"),
                records.stream().map(r -> r.get("method").asText()).toList());
        assertFalse(records.get(0).has("dropped_records"));
        assertFalse(records.get(1).has("dropped_records"));
        assertEquals(2, records.get(2).get("dropped_records").asInt(),
                "the first record through after a drop must carry the count");
    }

    /** A queue that never fills reports nothing, and still writes everything. */
    @Test
    void async_emission_without_pressure_reports_no_drops() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        AccessLogHook hook = AccessLogHook.builder(out).asyncQueueSize(1024).build();
        for (int i = 0; i < 100; i++) emit(hook, named("call-" + i), null);
        hook.close();

        List<JsonNode> records = records(out);
        assertEquals(100, records.size());
        for (JsonNode rec : records) assertFalse(rec.has("dropped_records"));
    }

    // ---- helpers ---------------------------------------------------------

    private static void emit(AccessLogHook hook, DispatchInfo info) {
        emit(hook, info, null);
    }

    private static void emit(AccessLogHook hook, DispatchInfo info, Throwable error) {
        hook.onDispatchEnd(hook.onDispatchStart(info), info, null, error);
    }

    /** Which of {@code ids} a fresh hook at rate 0.5 keeps. */
    private static List<String> sampleOnce(List<String> ids) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        AccessLogHook hook = AccessLogHook.builder(out).sampleRate(0.5).build();
        for (String id : ids) emit(hook, streamRecord(id), null);
        return records(out).stream().map(r -> r.get("stream_id").asText()).toList();
    }

    private static DispatchInfo unaryRecord() {
        DispatchInfo info = new DispatchInfo();
        info.method = "echo";
        info.methodType = "unary";
        info.serverId = "0123456789ab";
        info.protocol = "TestService";
        info.protocolHash = "0".repeat(64);
        info.requestData = new byte[] {1, 2, 3, 4};
        return info;
    }

    private static DispatchInfo named(String method) {
        DispatchInfo info = unaryRecord();
        info.method = method;
        return info;
    }

    private static DispatchInfo streamRecord(String streamId) {
        DispatchInfo info = unaryRecord();
        info.methodType = "stream";
        info.streamId = streamId;
        return info;
    }

    private static List<JsonNode> records(ByteArrayOutputStream out) throws IOException {
        List<JsonNode> parsed = new ArrayList<>();
        for (String line : out.toString().split("\n")) {
            if (!line.isBlank()) parsed.add(JSON.readTree(line));
        }
        return parsed;
    }

    /** Spin until the sink holds {@code n} complete lines, or give up. */
    private static void awaitLines(GateStream gate, int n) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (gate.lines() >= n) return;
            Thread.sleep(5);
        }
        throw new AssertionError("sink never reached " + n + " lines (saw " + gate.lines() + ")");
    }

    /**
     * A sink that parks the writer thread inside its first write, so the async
     * queue can be filled and overflowed with no timing assumptions.
     */
    private static final class GateStream extends OutputStream {
        final ByteArrayOutputStream sink = new ByteArrayOutputStream();
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        private boolean armed = true;

        @Override public void write(int b) {
            gate();
            synchronized (sink) { sink.write(b); }
        }

        @Override public void write(byte[] b, int off, int len) {
            gate();
            synchronized (sink) { sink.write(b, off, len); }
        }

        int lines() {
            synchronized (sink) {
                return (int) sink.toString().chars().filter(ch -> ch == '\n').count();
            }
        }

        private void gate() {
            if (!armed) return;
            armed = false;
            entered.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
