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
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@link DispatchHook} that writes one JSONL access-log record per RPC call to
 * an {@link OutputStream}.
 *
 * <p>The record shape conforms to the cross-language vgi-rpc access-log
 * specification (see {@code docs/access-log-spec.md} and
 * {@code vgi_rpc/access_log.schema.json} in the Python reference repo).
 */
public final class AccessLogHook implements DispatchHook {

    private static final DateTimeFormatter ISO =
            DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    private final OutputStream out;
    private final String serverVersion;
    private final Object writeLock = new Object();

    public AccessLogHook(OutputStream out, String serverVersion) {
        this.out = out;
        this.serverVersion = serverVersion == null ? "" : serverVersion;
    }

    /** Mint a 32-char lowercase hex stream identifier. */
    public static String randomStreamId() {
        byte[] b = new byte[16];
        new SecureRandom().nextBytes(b);
        StringBuilder sb = new StringBuilder(32);
        for (byte v : b) sb.append(String.format("%02x", v));
        return sb.toString();
    }

    @Override
    public Object onDispatchStart(DispatchInfo info) {
        return System.nanoTime();
    }

    @Override
    public void onDispatchEnd(Object token, DispatchInfo info, CallStatistics stats, Throwable error) {
        long startNs = token instanceof Long ? (Long) token : System.nanoTime();
        double durationMs = Math.round((System.nanoTime() - startNs) / 10_000.0) / 100.0;

        String status = error == null ? "ok" : "error";
        String errorType = "";
        String errorMessage = "";
        if (error != null) {
            if (error instanceof RpcError re) {
                errorType = re.errorType();
                errorMessage = re.getMessage() == null ? "" : re.getMessage();
            } else {
                errorType = error.getClass().getSimpleName();
                errorMessage = error.getMessage() == null ? error.toString() : error.getMessage();
            }
        }

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
        rec.put("remote_addr", info.remoteAddr);
        rec.put("duration_ms", durationMs);
        rec.put("status", status);
        rec.put("error_type", errorType);

        if (!errorMessage.isEmpty()) rec.put("error_message", errorMessage);
        if (!serverVersion.isEmpty()) rec.put("server_version", serverVersion);
        if (info.protocolVersion != null && !info.protocolVersion.isEmpty()) {
            rec.put("protocol_version", info.protocolVersion);
        }
        if (info.requestId != null && !info.requestId.isEmpty()) rec.put("request_id", info.requestId);
        if (info.httpStatus > 0) rec.put("http_status", info.httpStatus);
        if (info.requestData != null && info.requestData.length > 0) {
            rec.put("request_data", Base64.getEncoder().encodeToString(info.requestData));
        }
        if ("stream".equals(info.methodType)) {
            rec.put("stream_id", info.streamId == null || info.streamId.isEmpty()
                    ? "00000000000000000000000000000000" : info.streamId);
        }
        if (info.cancelled) rec.put("cancelled", true);
        if (stats != null && stats.nonZero()) {
            rec.put("input_batches", stats.inputBatches);
            rec.put("output_batches", stats.outputBatches);
            rec.put("input_rows", stats.inputRows);
            rec.put("output_rows", stats.outputRows);
            rec.put("input_bytes", stats.inputBytes);
            rec.put("output_bytes", stats.outputBytes);
        }

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

    /** Tiny JSON serializer for the record types used here (no external deps). */
    private static final class JsonWriter {
        static String toJsonLine(Map<String, Object> rec) {
            StringBuilder sb = new StringBuilder(256);
            sb.append('{');
            boolean first = true;
            for (Map.Entry<String, Object> e : rec.entrySet()) {
                if (!first) sb.append(',');
                first = false;
                writeString(sb, e.getKey());
                sb.append(':');
                writeValue(sb, e.getValue());
            }
            sb.append('}');
            return sb.toString();
        }

        static void writeValue(StringBuilder sb, Object v) {
            if (v == null) {
                sb.append("null");
            } else if (v instanceof String s) {
                writeString(sb, s);
            } else if (v instanceof Boolean b) {
                sb.append(b ? "true" : "false");
            } else if (v instanceof Long || v instanceof Integer) {
                sb.append(v);
            } else if (v instanceof Double d) {
                if (d.isNaN() || d.isInfinite()) sb.append("null");
                else sb.append(d);
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
