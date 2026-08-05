// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * {@link DispatchHook} that writes one JSONL access-log record per RPC call to
 * an {@link OutputStream}.
 *
 * <p>The record shape conforms to the cross-language vgi-rpc access-log
 * specification (see {@code docs/access-log-spec.md} and
 * {@code vgi_rpc/access_log.schema.json} in the Python reference repo).
 *
 * <p>Records are written synchronously by default. {@link Builder} adds the
 * optional behaviours the spec permits: deterministic sampling, background
 * emission, payload omission, and a replaceable claim-redaction policy.
 */
public final class AccessLogHook implements DispatchHook, AutoCloseable {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(AccessLogHook.class);

    private static final DateTimeFormatter ISO =
            DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    /** W3C ids are lowercase hex of fixed width; anything else fails schema validation downstream. */
    private static final Pattern TRACE_ID = Pattern.compile("[0-9a-f]{32}");
    private static final Pattern SPAN_ID = Pattern.compile("[0-9a-f]{16}");

    private final OutputStream out;
    private final String serverVersion;
    private final double sampleRate;
    private final long sampleThreshold;
    private final boolean logPayloads;
    private final ClaimRedactor claimRedactor;
    private final TraceCorrelator traceCorrelator;
    private final AsyncEmitter async;
    private final Object writeLock = new Object();

    /**
     * Creates a hook that appends one JSONL record per dispatch to {@code out}.
     * The stream is not closed by this class; the caller retains ownership.
     *
     * @param out destination for one JSONL record per dispatch (writes are synchronized)
     * @param serverVersion server-version string included in each record; {@code null} becomes {@code ""}
     */
    public AccessLogHook(OutputStream out, String serverVersion) {
        this(builder(out).serverVersion(serverVersion));
    }

    private AccessLogHook(Builder b) {
        this.out = b.out;
        this.serverVersion = b.serverVersion == null ? "" : b.serverVersion;
        this.sampleRate = b.sampleRate;
        this.sampleThreshold = (long) (b.sampleRate * 0xFFFFFFFFL);
        this.logPayloads = b.logPayloads;
        this.claimRedactor = b.claimRedactor;
        this.traceCorrelator = b.traceCorrelator;
        this.async = b.asyncQueueSize > 0 ? new AsyncEmitter(this, b.asyncQueueSize) : null;
    }

    /**
     * Start configuring a hook writing to {@code out}.
     *
     * @param out destination for one JSONL record per dispatch
     * @return a builder seeded with the conformant defaults: log everything,
     *         synchronously, with payloads, redacting claims by key
     */
    public static Builder builder(OutputStream out) {
        return new Builder(out);
    }

    /** Configuration for {@link AccessLogHook}. */
    public static final class Builder {
        private final OutputStream out;
        private String serverVersion = "";
        private double sampleRate = 1.0;
        private boolean logPayloads = true;
        private ClaimRedactor claimRedactor = ClaimRedactor.byKeyName();
        private TraceCorrelator traceCorrelator = TraceCorrelator.openTelemetry();
        private int asyncQueueSize;

        private Builder(OutputStream out) {
            this.out = out;
        }

        /**
         * Server build version stamped on every record.
         *
         * @param version free-form build identifier; {@code null} becomes {@code ""}
         * @return this builder
         */
        public Builder serverVersion(String version) {
            this.serverVersion = version;
            return this;
        }

        /**
         * Log only a fraction of successful calls.
         *
         * <p>Errors are never sampled out, and the decision is deterministic per
         * call rather than per record, so every record of one stream shares its
         * init's fate — see {@link AccessLogHook#sampledIn}.
         *
         * @param rate fraction of non-error calls to keep, {@code 0 < rate <= 1};
         *             {@code 1.0} (the default) keeps everything
         * @return this builder
         * @throws IllegalArgumentException if {@code rate} is outside the range.
         *         Rejected here rather than at the first request because
         *         {@code 100} meaning "100%" would otherwise silently log everything.
         */
        public Builder sampleRate(double rate) {
            if (!(rate > 0.0) || rate > 1.0 || Double.isNaN(rate)) {
                throw new IllegalArgumentException(
                        "access-log sample rate must be in (0.0, 1.0], got " + rate);
            }
            this.sampleRate = rate;
            return this;
        }

        /**
         * Whether request payloads are logged.
         *
         * @param enabled {@code false} drops {@code request_data} and marks the
         *                record {@code truncated: "payload_omitted"} — distinct from
         *                the size-driven {@code truncated: true}, so a consumer
         *                scanning for real data loss has something to filter on
         * @return this builder
         */
        public Builder logPayloads(boolean enabled) {
            this.logPayloads = enabled;
            return this;
        }

        /**
         * Policy applied to authentication claims before they reach a record.
         *
         * @param redactor the policy; {@link ClaimRedactor#none()} opts a service
         *                 that owns its logs end to end out of redaction
         * @return this builder
         */
        public Builder claimRedactor(ClaimRedactor redactor) {
            this.claimRedactor = redactor;
            return this;
        }

        /**
         * Source of the {@code trace_id}/{@code span_id} pair.
         *
         * @param correlator the source; defaults to {@link TraceCorrelator#openTelemetry()}
         * @return this builder
         */
        public Builder traceCorrelator(TraceCorrelator correlator) {
            this.traceCorrelator = correlator;
            return this;
        }

        /**
         * Hand records to a background writer so disk latency stays out of the
         * request path.
         *
         * <p>The queue is bounded and the enqueue never blocks: an unbounded queue
         * turns a stalled disk into an OOM, and a blocking put reintroduces exactly
         * the latency the thread was meant to remove. Full therefore means drop,
         * and the next record through carries {@code dropped_records}.
         *
         * <p>Opt-in because it trades durability — with a synchronous writer a
         * record on disk means the call completed; here a crash loses whatever is
         * still queued. That is the wrong trade for an audit log.
         *
         * @param queueSize bounded queue depth; {@code 0} (the default) keeps
         *                  emission synchronous
         * @return this builder
         * @throws IllegalArgumentException if {@code queueSize} is negative
         */
        public Builder asyncQueueSize(int queueSize) {
            if (queueSize < 0) throw new IllegalArgumentException("queue size must be >= 0, got " + queueSize);
            this.asyncQueueSize = queueSize;
            return this;
        }

        /**
         * Build the hook.
         *
         * @return a configured {@link AccessLogHook}
         */
        public AccessLogHook build() {
            return new AccessLogHook(this);
        }
    }

    /** Mint a 32-char lowercase hex stream identifier. The dispatcher assigns
     *  one per streaming call so every record of a stream's lifecycle can be
     *  correlated in the access log.
     *  @return a freshly generated 32-character lowercase hex string */
    public static String randomStreamId() {
        byte[] b = new byte[16];
        new SecureRandom().nextBytes(b);
        StringBuilder sb = new StringBuilder(32);
        for (byte v : b) sb.append(String.format("%02x", v));
        return sb.toString();
    }

    /** {@inheritDoc} Returns a nanosecond start timestamp used to compute duration. */
    @Override
    public Object onDispatchStart(DispatchInfo info) {
        return System.nanoTime();
    }

    /** {@inheritDoc} Writes one JSONL access-log record describing the completed dispatch. */
    @Override
    public void onDispatchEnd(Object token, DispatchInfo info, CallStatistics stats, Throwable error) {
        long startNs = token instanceof Long ? (Long) token : System.nanoTime();
        double durationMs = Math.round((System.nanoTime() - startNs) / 10_000.0) / 100.0;

        // A method that raised does not propagate: the dispatcher serializes the
        // exception into the response and returns normally, so `error` is null
        // for exactly the calls an operator most wants to find. CallOutcome is
        // what the error was recorded on as it went onto the wire.
        Throwable failure = error != null ? error : CallOutcome.currentError();

        String status = failure == null ? "ok" : "error";
        String errorType = "";
        String errorMessage = "";
        if (failure != null) {
            if (failure instanceof RpcError re) {
                errorType = re.errorType();
                errorMessage = re.getMessage() == null ? "" : re.getMessage();
            } else {
                errorType = failure.getClass().getSimpleName();
                errorMessage = failure.getMessage() == null ? failure.toString() : failure.getMessage();
            }
            // The schema requires a non-empty error_message on every error
            // record, and an exception carrying no message is not a reason to
            // emit an unreadable one.
            if (errorMessage.isEmpty()) errorMessage = failure.toString();
            if (errorType.isEmpty()) errorType = failure.getClass().getSimpleName();
        }

        AccessLogScope scope = AccessLogScope.current();

        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("timestamp", ISO.format(Instant.now()));
        rec.put("level", "INFO");
        rec.put("logger", "vgi_rpc.access");
        rec.put("message", info.protocol + "." + info.method + " " + status);
        rec.put("server_id", info.serverId);
        rec.put("protocol", info.protocol);
        rec.put("protocol_hash", info.protocolHash);
        rec.put("method", info.method);
        rec.put("method_type", info.methodType);
        rec.put("principal", info.principal);
        rec.put("auth_domain", info.authDomain);
        rec.put("authenticated", info.authenticated);
        rec.put("remote_addr", remoteAddr(info));
        rec.put("duration_ms", durationMs);
        rec.put("status", status);
        rec.put("error_type", errorType);

        if (!errorMessage.isEmpty()) rec.put("error_message", errorMessage);
        if (!serverVersion.isEmpty()) rec.put("server_version", serverVersion);
        if (info.protocolVersion != null && !info.protocolVersion.isEmpty()) {
            rec.put("protocol_version", info.protocolVersion);
        }
        String requestId = info.requestId != null && !info.requestId.isEmpty()
                ? info.requestId
                : (scope != null ? scope.requestId() : "");
        if (!requestId.isEmpty()) rec.put("request_id", requestId);
        // Trace correlation. request_id only joins records within this service;
        // these join them to the surrounding distributed trace.
        putTraceContext(rec);
        if (info.httpStatus > 0) rec.put("http_status", info.httpStatus);
        if (info.requestData != null && info.requestData.length > 0) {
            String encoded = Base64.getEncoder().encodeToString(info.requestData);
            if (logPayloads) {
                rec.put("request_data", encoded);
            } else {
                // Nothing was lost to a size cap here — this deployment simply
                // does not log payloads. Sharing the size-driven `true` made the
                // marker fire on essentially every record and stop meaning
                // anything to a consumer looking for real data loss.
                rec.put("original_request_bytes", encoded.length());
                rec.put("truncated", "payload_omitted");
            }
        }
        if ("stream".equals(info.methodType)) {
            rec.put("stream_id", info.streamId == null || info.streamId.isEmpty()
                    ? "00000000000000000000000000000000" : info.streamId);
        }
        // Stream state, decrypted. The token on the wire is an opaque AEAD
        // ciphertext; a log reader holding the server's token_key is not a
        // situation to design for, so the plaintext is what gets logged.
        // Gated with request_data: these are the same kind of payload and a
        // deployment that opted out of one did not ask for the other.
        if (logPayloads) {
            putStateBytes(rec, "request_state", info.requestState);
            putStateBytes(rec, "response_state", info.responseState);
        }
        if (info.cancelled) rec.put("cancelled", true);
        if (info.sessionId != null && !info.sessionId.isEmpty()) {
            rec.put("session_id", info.sessionId);
        }
        if (info.sessionAction != null && !info.sessionAction.isEmpty()) {
            rec.put("session_action", info.sessionAction);
        }
        putClaims(rec, info.claims);
        // Egress accounting. The stats below measure logical Arrow buffers —
        // what the worker processed. These measure what actually crossed the
        // network, which differs in both directions: compression shrinks the
        // body, and externalised payloads leave it entirely.
        if (scope != null) {
            if (scope.requestBytes() >= 0) rec.put("request_bytes", scope.requestBytes());
            if (scope.externalizedBytes() > 0) rec.put("externalized_bytes", scope.externalizedBytes());
        }
        if (stats != null && stats.nonZero()) {
            rec.put("input_batches", stats.inputBatches);
            rec.put("output_batches", stats.outputBatches);
            rec.put("input_rows", stats.inputRows);
            rec.put("output_rows", stats.outputRows);
            rec.put("input_bytes", stats.inputBytes);
            rec.put("output_bytes", stats.outputBytes);
        }

        if (scope != null) {
            // Deferred: response_bytes is not known until the body has been
            // compressed, which happens after this hook has run.
            scope.defer(this, rec);
        } else {
            write(rec);
        }
    }

    /** Base64 a decrypted state payload under {@code key}, skipping empties —
     *  the schema's base64 pattern admits no zero-length string. */
    private static void putStateBytes(Map<String, Object> rec, String key, byte[] state) {
        if (state == null || state.length == 0) return;
        rec.put(key, Base64.getEncoder().encodeToString(state));
    }

    /** HTTP fills {@code remote_addr} into the transport metadata, not the dispatch info. */
    private static String remoteAddr(DispatchInfo info) {
        if (info.remoteAddr != null && !info.remoteAddr.isEmpty()) return info.remoteAddr;
        if (info.transportMetadata != null) {
            Object addr = info.transportMetadata.get("remote_addr");
            if (addr instanceof String s) return s;
        }
        return "";
    }

    private void putTraceContext(Map<String, Object> rec) {
        String[] ids;
        try {
            ids = traceCorrelator.current();
        } catch (RuntimeException e) {
            return;
        }
        // Both or neither: a record carrying one half joins nothing, and a
        // malformed id (a dashed UUID, say) fails schema validation for every
        // record the server writes.
        if (ids == null || ids.length != 2 || ids[0] == null || ids[1] == null) return;
        if (!TRACE_ID.matcher(ids[0]).matches() || !SPAN_ID.matcher(ids[1]).matches()) return;
        rec.put("trace_id", ids[0]);
        rec.put("span_id", ids[1]);
    }

    private void putClaims(Map<String, Object> rec, Map<String, Object> claims) {
        if (claims == null || claims.isEmpty()) return;
        Map<String, Object> redacted;
        try {
            redacted = claimRedactor.redact(claims);
        } catch (RuntimeException e) {
            // Fail closed. A redactor that throws must not take the request down
            // with it, but it must not fail *open* either — an unredacted claim
            // on disk cannot be recalled.
            LOG.warn("claim redactor raised; dropping claims from the access-log record", e);
            return;
        }
        if (redacted != null && !redacted.isEmpty()) rec.put("claims", redacted);
    }

    /**
     * Decide whether to keep {@code rec}, stamping {@code sample_rate} when kept.
     *
     * <p>Errors are never sampled out: a rate below 1 exists because successful
     * calls are repetitive, which is exactly what failures are not, and a
     * consumer must be able to read a fall in error count as a fix landing
     * rather than as the dice going the other way.
     *
     * <p>The decision is a function of a stable identifier for the <em>call</em> —
     * {@code stream_id} when present, else {@code request_id} — so every record
     * of one stream shares its init's fate. Random per-record sampling shreds a
     * multi-record call into fragments indistinguishable from data loss, and the
     * calls likeliest to be split are the long streams most worth studying.
     */
    private boolean sampledIn(Map<String, Object> rec) {
        if (sampleRate >= 1.0) return true;
        if ("error".equals(rec.get("status"))) return true;
        Object key = rec.get("stream_id");
        if (!(key instanceof String s) || s.isEmpty()) key = rec.get("request_id");
        // A record with neither identifier degrades to a per-record decision
        // rather than being dropped on the floor.
        String keyed = key instanceof String s2 && !s2.isEmpty()
                ? s2
                : System.nanoTime() + ":" + Thread.currentThread().threadId();
        if (hash32(keyed) > sampleThreshold) return false;
        rec.put("sample_rate", sampleRate);
        return true;
    }

    /** FNV-1a with a murmur3 finalizer: the sample decision reads the low bits,
     *  which FNV alone leaves poorly distributed for short hex keys. */
    private static long hash32(String key) {
        long h = 0xcbf29ce484222325L;
        for (int i = 0; i < key.length(); i++) {
            h ^= key.charAt(i);
            h *= 0x100000001b3L;
        }
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= h >>> 33;
        return h >>> 32;
    }

    /** Apply sampling, then emit — synchronously or via the background writer. */
    void write(Map<String, Object> rec) {
        if (!sampledIn(rec)) return;
        if (async != null) {
            async.submit(rec);
        } else {
            writeLine(rec);
        }
    }

    /** Serialize and append one record. Called on the caller's thread when
     *  synchronous, on the writer thread when asynchronous. */
    void writeLine(Map<String, Object> rec) {
        String line = JsonWriter.toJsonLine(rec);
        try {
            synchronized (writeLock) {
                out.write(line.getBytes(StandardCharsets.UTF_8));
                out.write('\n');
                out.flush();
            }
        } catch (IOException ignored) {
            // best-effort observability
        }
    }

    /**
     * Stop the background writer, draining what it still holds. Synchronous
     * hooks need no shutdown; calling this on one is a no-op. The output stream
     * is not closed — the caller retains ownership.
     */
    @Override
    public void close() {
        if (async != null) async.close();
    }

    /**
     * Non-blocking background writer that reports what it dropped.
     *
     * <p>What makes dropping acceptable rather than silent corruption is that it
     * is reported: the next record to get through carries {@code dropped_records},
     * so the loss is visible in the log itself rather than only in a metric
     * nobody exports. A log that loses records without saying so is worse than a
     * slow one, because a consumer cannot tell a quiet period from a lossy one.
     */
    private static final class AsyncEmitter implements AutoCloseable {

        /** Sentinel telling the writer thread to stop; identity-compared. */
        private static final Map<String, Object> POISON = Map.of();

        private final AccessLogHook hook;
        private final BlockingQueue<Map<String, Object>> queue;
        private final Thread worker;
        private final Object dropLock = new Object();
        private long dropped;

        AsyncEmitter(AccessLogHook hook, int capacity) {
            this.hook = hook;
            this.queue = new ArrayBlockingQueue<>(capacity);
            this.worker = new Thread(this::drain, "vgi-rpc-access-log");
            this.worker.setDaemon(true);
            this.worker.start();
        }

        void submit(Map<String, Object> rec) {
            long seen;
            synchronized (dropLock) {
                seen = dropped;
                // Attribute the loss to the first record that gets through after
                // it, so the count reaches the same file the losses would have.
                if (seen > 0) rec.put("dropped_records", seen);
            }
            if (!queue.offer(rec)) {
                synchronized (dropLock) {
                    dropped++;
                }
                return;
            }
            if (seen > 0) {
                synchronized (dropLock) {
                    dropped -= seen;
                }
            }
        }

        private void drain() {
            try {
                while (true) {
                    Map<String, Object> rec = queue.take();
                    if (rec == POISON) return;
                    hook.writeLine(rec);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void close() {
            // A full queue would drop the sentinel and leave the thread parked;
            // it is a daemon, so the JVM still exits, but the drain would stall.
            while (!queue.offer(POISON)) {
                Thread.onSpinWait();
            }
            try {
                worker.join(TimeUnit.SECONDS.toMillis(5));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** Tiny JSON serializer for the record types used here (no external deps). */
    private static final class JsonWriter {
        static String toJsonLine(Map<String, Object> rec) {
            StringBuilder sb = new StringBuilder(256);
            writeObject(sb, rec);
            return sb.toString();
        }

        static void writeObject(StringBuilder sb, Map<?, ?> rec) {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> e : rec.entrySet()) {
                if (!first) sb.append(',');
                first = false;
                writeString(sb, String.valueOf(e.getKey()));
                sb.append(':');
                writeValue(sb, e.getValue());
            }
            sb.append('}');
        }

        static void writeValue(StringBuilder sb, Object v) {
            if (v == null) {
                sb.append("null");
            } else if (v instanceof String s) {
                writeString(sb, s);
            } else if (v instanceof Boolean b) {
                sb.append(b ? "true" : "false");
            } else if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte) {
                sb.append(v);
            } else if (v instanceof Double || v instanceof Float) {
                double d = ((Number) v).doubleValue();
                if (Double.isNaN(d) || Double.isInfinite(d)) sb.append("null");
                else sb.append(d);
            } else if (v instanceof Map<?, ?> m) {
                // Claims arrive as arbitrary JSON-shaped values; a toString()
                // fallback would emit Java map syntax and fail every consumer.
                writeObject(sb, m);
            } else if (v instanceof Collection<?> c) {
                sb.append('[');
                boolean first = true;
                for (Object item : c) {
                    if (!first) sb.append(',');
                    first = false;
                    writeValue(sb, item);
                }
                sb.append(']');
            } else {
                writeString(sb, v.toString());
            }
        }

        static void writeString(StringBuilder sb, String s) {
            sb.append('"');
            for (int i = 0; i < s.length(); i++) {
                char ch = s.charAt(i);
                switch (ch) {
                    case '"' -> sb.append("\\\"");
                    case '\\' -> sb.append("\\\\");
                    case '\n' -> sb.append("\\n");
                    case '\r' -> sb.append("\\r");
                    case '\t' -> sb.append("\\t");
                    case '\b' -> sb.append("\\b");
                    case '\f' -> sb.append("\\f");
                    default -> {
                        if (ch < 0x20) sb.append(String.format("\\u%04x", (int) ch));
                        else sb.append(ch);
                    }
                }
            }
            sb.append('"');
        }
    }
}
