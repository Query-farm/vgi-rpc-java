// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.conformance.driver;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.Introspect;
import farm.query.vgirpc.MethodType;
import farm.query.vgirpc.RawStream;
import farm.query.vgirpc.RpcConnection;
import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.external.ExternalLocationConfig;
import farm.query.vgirpc.http.HttpCapabilities;
import farm.query.vgirpc.http.HttpRpcConnection;
import farm.query.vgirpc.http.UploadUrl;
import farm.query.vgirpc.log.Message;
import farm.query.vgirpc.transport.RpcTransport;
import farm.query.vgirpc.transport.SubprocessTransport;
import farm.query.vgirpc.transport.TcpSocketTransport;
import farm.query.vgirpc.transport.UnixSocketTransport;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import farm.query.vgirpc.wire.Metadata;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Conformance client driver: a JSONL bridge that lets the Python conformance
 * suite drive this port's <em>client</em>.
 *
 * <p>The control protocol is specified in the reference repository's
 * {@code tools/cross-port/specs/CLIENT_DRIVER_PROTOCOL.md}; that document, not
 * this file, is the contract. One request object per line on stdin, one
 * response object per line on stdout, strict lockstep, one connection per
 * process.
 *
 * <p><strong>stdout is the control channel and nothing else.</strong> A single
 * stray line — a {@code println}, a Gradle progress bar, an SLF4J backend that
 * defaulted to {@code System.out} — desynchronises every later response from
 * its request, and the failure surfaces as a thousand unrelated test errors
 * rather than as itself. Diagnostics go to stderr; this class captures
 * {@link System#out} at startup and reassigns the global to stderr so that even
 * code which was never told the rule cannot break it.
 *
 * <p>The driver is a <em>relay</em>. It decodes nothing and repairs nothing:
 * the method name comes from the request batch's {@code vgi_rpc.method} (never
 * defaulted), the routing key comes from {@code connect} (never defaulted), the
 * stream kind comes from the harness (never guessed from a method name), and an
 * external-location pointer is resolved by the client under test or not at all.
 * Every one of those accommodations would turn a client defect into a passing
 * run, which is the failure mode the whole exercise exists to prevent.
 */
public final class Main {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Base64.Encoder B64E = Base64.getEncoder();
    private static final Base64.Decoder B64D = Base64.getDecoder();

    /** Default POSIX shared-memory segment size, matching the reference shim. */
    private static final int DEFAULT_SHM_SIZE = 4 * 1024 * 1024;
    /** How long a {@code unix} connect waits out a busy worker's full accept queue. */
    private static final java.time.Duration UNIX_CONNECT_TIMEOUT = java.time.Duration.ofSeconds(10);

    /** The real stdout, captured before {@link System#out} is redirected to stderr. */
    private final PrintStream control;
    private final List<Message> logs = Collections.synchronizedList(new ArrayList<>());

    private RpcTransport transport;
    private RpcConnection byteStream;
    private HttpRpcConnection http;
    private String protocol;
    private String protocolVersion;
    private RawStream stream;

    private Main(PrintStream control) {
        this.control = control;
    }

    /**
     * Run one driver process: read control requests until {@code shutdown} or EOF.
     *
     * @param args ignored; the driver is configured entirely over the control channel
     * @throws IOException if the control channel fails
     */
    public static void main(String[] args) throws IOException {
        PrintStream control = new PrintStream(new java.io.FileOutputStream(java.io.FileDescriptor.out),
                false, StandardCharsets.UTF_8);
        // Anything that reaches for System.out from here on lands on stderr
        // instead of corrupting the control channel.
        System.setOut(new PrintStream(new java.io.FileOutputStream(java.io.FileDescriptor.err),
                true, StandardCharsets.UTF_8));
        new Main(control).run();
    }

    private void run() throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        String line;
        while ((line = in.readLine()) != null) {
            String trimmed = line.trim();
            if (trimmed.isEmpty()) continue;
            JsonNode request;
            try {
                request = JSON.readTree(trimmed);
            } catch (Exception e) {
                write(refusal("bad json: " + e.getMessage()));
                continue;
            }
            String op = text(request, "op", "");
            if ("shutdown".equals(op)) {
                teardown();
                write(JSON.createObjectNode().put("ok", true));
                return;
            }
            try {
                write(dispatch(op, request));
            } catch (RuntimeException e) {
                // A driver bug must be reported as a driver failure, not as a
                // peer error: `ok` describes the driver, `error` describes the call.
                // The control channel carries one line, so the stack that would
                // say *where* goes to stderr — without it the harness reports a
                // one-line exception message with no way to locate it.
                e.printStackTrace();
                write(refusal(e.getClass().getSimpleName() + ": " + e.getMessage()));
            }
        }
        // EOF on stdin is a shutdown with no response: the harness kills a
        // driver that has not exited five seconds after asking it to.
        teardown();
    }

    private ObjectNode dispatch(String op, JsonNode request) {
        return switch (op) {
            case "connect" -> connect(request);
            case "unary" -> unary(request);
            case "describe" -> describe();
            case "stream_open" -> streamOpen(request);
            case "tick" -> tick(request);
            case "next_with_token" -> nextWithToken();
            case "exchange" -> exchange(request);
            case "cancel" -> cancel();
            case "close" -> closeStream();
            case "capabilities", "request_upload_urls", "session_begin", "session_token",
                 "session_echo_headers", "session_detach", "session_end" -> httpAdmin(op, request);
            default -> refusal("unknown op: " + op);
        };
    }

    // ------------------------------------------------------------------
    // Connection
    // ------------------------------------------------------------------

    private ObjectNode connect(JsonNode request) {
        if (byteStream != null || http != null) return refusal("already connected");
        String kind = text(request, "transport", "");
        JsonNode target = request.get("target");
        // The routing key, bound once. Never defaulted: every request names the
        // protocol it addresses, and a driver that substitutes a hardcoded name
        // when the field is absent hides exactly the class of defect this
        // exercise exists to find.
        JsonNode protocolNode = request.get("protocol");
        if (protocolNode == null || !protocolNode.isTextual() || protocolNode.asText().isBlank()) {
            return refusal("connect carries no 'protocol' routing key");
        }
        protocol = protocolNode.asText();
        // The conformance service declares its surface version; the harness
        // stamps the same key on every request batch, which is layered over
        // whatever the client would have sent, so this only has to be present.
        protocolVersion = null;
        try {
            switch (kind) {
                case "stdio" -> {
                    transport = new SubprocessTransport(argv(target));
                    byteStream = new RpcConnection(transport, logs::add, externalConfig(request));
                }
                case "shm" -> {
                    // Java implements the shared-memory side-channel on the
                    // server only; there is no client half to drive. Reported
                    // rather than silently degraded to a pipe, because a lane
                    // that quietly tested something else is worse than a lane
                    // that says it cannot run.
                    return refusal("shm transport has no client implementation in this port");
                }
                case "unix" -> {
                    if (target == null || !target.isTextual()) {
                        return refusal("unix target must be a path string");
                    }
                    // Waits out a busy worker's full accept queue rather than failing on it.
                    transport = UnixSocketTransport.connect(Path.of(target.asText()), UNIX_CONNECT_TIMEOUT);
                    byteStream = new RpcConnection(transport, logs::add, externalConfig(request));
                }
                case "tcp" -> {
                    if (target == null || !target.isTextual()) {
                        return refusal("tcp target must be a host:port string");
                    }
                    String address = target.asText();
                    int colon = address.lastIndexOf(':');
                    String host = colon < 0 ? "127.0.0.1" : address.substring(0, colon);
                    if (host.isEmpty()) host = "127.0.0.1";
                    int port = Integer.parseInt(colon < 0 ? address : address.substring(colon + 1));
                    transport = TcpSocketTransport.connect(host, port);
                    byteStream = new RpcConnection(transport, logs::add, externalConfig(request));
                }
                case "http" -> {
                    if (target == null || !target.isTextual()) {
                        return refusal("http target must be a url string");
                    }
                    http = buildHttp(target.asText(), request);
                }
                default -> {
                    return refusal("unknown transport: " + kind);
                }
            }
        } catch (Exception e) {
            return refusal("connect failed: " + e);
        }
        return JSON.createObjectNode().put("ok", true);
    }

    private HttpRpcConnection buildHttp(String url, JsonNode request) {
        HttpRpcConnection.Builder builder = HttpRpcConnection.builder(url).onLog(logs::add);
        JsonNode headers = request.get("headers");
        if (headers != null && headers.isObject()) {
            headers.fields().forEachRemaining(entry -> {
                if (entry.getValue().isTextual()) builder.header(entry.getKey(), entry.getValue().asText());
            });
        }
        // compression_level is tri-state: absent means "the client's default",
        // an explicit null disables request compression, an integer is a zstd
        // level. Reading absence as null would silently disable a codec the
        // suite means to exercise.
        if (request.has("compression_level")) {
            JsonNode level = request.get("compression_level");
            builder.compressionLevel(level.isNull() ? null : level.asInt());
        }
        ExternalLocationConfig external = externalConfig(request);
        if (external != null) builder.externalLocation(external);
        return builder.build();
    }

    /**
     * The external-location configuration, when the harness asked for the
     * client under test to resolve pointer batches itself.
     *
     * <p>The fixture storage vends {@code http://127.0.0.1} download URLs, so
     * the HTTPS-only validator has to come off — that is a property of the
     * fixture, not a relaxation of the client: resolution still happens in the
     * client, which is the whole point of the flag.
     */
    private static ExternalLocationConfig externalConfig(JsonNode request) {
        JsonNode external = request.get("external");
        if (external == null || !external.asBoolean(false)) return null;
        return ExternalLocationConfig.builder()
                .urlValidator(ExternalLocationConfig.permissiveValidator())
                .build();
    }

    private static List<String> argv(JsonNode target) {
        if (target == null || !target.isArray()) {
            throw new IllegalArgumentException("target must be an argv array");
        }
        List<String> argv = new ArrayList<>(target.size());
        target.forEach(item -> argv.add(item.asText()));
        return argv;
    }

    private void teardown() {
        if (stream != null) {
            try { stream.close(); } catch (Exception ignore) { /* best-effort */ }
            stream = null;
        }
        // Closing the connection is part of the contract, not an optimisation:
        // the harness runs thousands of connections, and a driver that leaks a
        // subprocess or a socket per connection exhausts the runner rather than
        // failing a test.
        if (http != null) {
            try { http.close(); } catch (Exception ignore) { /* best-effort */ }
            http = null;
        }
        if (byteStream != null) {
            try { byteStream.close(); } catch (Exception ignore) { /* best-effort */ }
            byteStream = null;
            transport = null;
        }
    }

    // ------------------------------------------------------------------
    // Calls
    // ------------------------------------------------------------------

    private ObjectNode unary(JsonNode request) {
        if (byteStream == null && http == null) return refusal("not connected");
        byte[] bytes;
        try {
            bytes = decodeB64(request, "request_b64");
        } catch (RuntimeException e) {
            return refusal(e.getMessage());
        }
        try (IpcStreamReader reader = new IpcStreamReader(new ByteArrayInputStream(bytes), Allocators.root())) {
            Map<String, String> md = reader.readNextBatch();
            if (md == null) return refusal("empty IPC stream (no batch)");
            String method = md.get(Metadata.RPC_METHOD);
            if (method == null || method.isBlank()) {
                // Never defaulted. An earlier driver generation defaulted to
                // __describe__, and every lost-metadata bug was reported as a
                // retired-method error instead of as itself.
                return refusal("request metadata names no " + Metadata.RPC_METHOD);
            }
            AnnotatedBatch params = new AnnotatedBatch(reader.root(), md, reader.dictionaryProvider(), null);
            byte[] reply;
            try {
                reply = byteStream != null
                        ? byteStream.callUnaryRaw(protocol, protocolVersion, method, params)
                        : http.callUnaryRaw(protocol, protocolVersion, method, params);
            } catch (RpcError e) {
                ObjectNode response = JSON.createObjectNode().put("ok", true);
                response.putNull("result_b64");
                response.set("logs", drainLogs());
                response.set("error", errorNode(e));
                return response;
            }
            ObjectNode response = JSON.createObjectNode().put("ok", true);
            if (reply == null) response.putNull("result_b64");
            else response.put("result_b64", B64E.encodeToString(reply));
            response.set("logs", drainLogs());
            response.putNull("error");
            return response;
        } catch (IOException e) {
            return refusal("could not read request batch: " + e);
        }
    }

    private ObjectNode describe() {
        if (byteStream == null && http == null) return refusal("not connected");
        Introspect.RawUnaryCaller caller = (target, method, batch) -> byteStream != null
                ? byteStream.callUnaryRaw(target, null, method, batch)
                : http.callUnaryRaw(target, null, method, batch);
        ObjectNode response = JSON.createObjectNode().put("ok", true);
        try {
            Introspect.ServiceDescription described = Introspect.describe(caller);
            response.set("describe", describeNode(described));
            response.set("logs", drainLogs());
            response.putNull("error");
        } catch (RpcError e) {
            response.putNull("describe");
            response.set("logs", drainLogs());
            response.set("error", errorNode(e));
        }
        return response;
    }

    private ObjectNode describeNode(Introspect.ServiceDescription described) {
        ObjectNode node = JSON.createObjectNode();
        node.put("protocol_name", described.protocolName());
        node.put("request_version", described.requestVersion());
        node.put("describe_version", described.describeVersion());
        node.put("protocol_hash", described.protocolHash());
        node.put("server_id", described.serverId());
        node.put("protocol_version", described.protocolVersion());
        ArrayNode methods = node.putArray("methods");
        for (Introspect.MethodDescription method : described.methods().values()) {
            ObjectNode entry = methods.addObject();
            entry.put("name", method.name());
            entry.put("method_type", method.methodType() == MethodType.STREAM ? "stream" : "unary");
            entry.put("has_return", method.hasReturn());
            entry.put("has_header", method.hasHeader());
            if (method.isExchange() == null) entry.putNull("is_exchange");
            else entry.put("is_exchange", method.isExchange());
            putSchema(entry, "params_schema_b64", method.paramsSchemaIpc());
            putSchema(entry, "result_schema_b64", method.resultSchemaIpc());
            putSchema(entry, "header_schema_b64", method.headerSchemaIpc());
        }
        return node;
    }

    /**
     * Relay a schema exactly as the peer serialised it.
     *
     * <p>Re-encoding a decoded schema would be conforming too, but relaying the
     * peer's own bytes has one fewer place for the two to disagree. An absent
     * schema is {@code null}, which the harness reads as the empty schema.
     */
    private static void putSchema(ObjectNode entry, String field, byte[] schemaIpc) {
        if (schemaIpc == null || schemaIpc.length == 0) entry.putNull(field);
        else entry.put(field, B64E.encodeToString(schemaIpc));
    }

    // ------------------------------------------------------------------
    // Streams
    // ------------------------------------------------------------------

    private ObjectNode streamOpen(JsonNode request) {
        if (byteStream == null && http == null) return refusal("not connected");
        if (stream != null) return refusal("a stream is already open on this connection");
        byte[] bytes;
        try {
            bytes = decodeB64(request, "request_b64");
        } catch (RuntimeException e) {
            return refusal(e.getMessage());
        }
        // Authoritative, from the harness's own protocol declaration. Inferring
        // it from the method name is a fixture-shaped accident that does not
        // survive the next method added to the suite.
        boolean isExchange = request.path("is_exchange").asBoolean(false);
        boolean hasHeader = request.path("has_header").asBoolean(false);
        try (IpcStreamReader reader = new IpcStreamReader(new ByteArrayInputStream(bytes), Allocators.root())) {
            Map<String, String> md = reader.readNextBatch();
            if (md == null) return refusal("empty IPC stream (no batch)");
            String method = md.get(Metadata.RPC_METHOD);
            if (method == null || method.isBlank()) {
                return refusal("request metadata names no " + Metadata.RPC_METHOD);
            }
            AnnotatedBatch params = new AnnotatedBatch(reader.root(), md, reader.dictionaryProvider(), null);
            RawStream opened;
            try {
                opened = byteStream != null
                        ? byteStream.openStreamRaw(protocol, protocolVersion, method, params, hasHeader)
                        : http.openStreamRaw(protocol, protocolVersion, method, params, hasHeader, isExchange);
            } catch (RpcError e) {
                // The driver carried out the op; the peer refused. `ok: false`
                // would mix the two channels and hand the harness an object
                // where it documents a string.
                ObjectNode response = JSON.createObjectNode().put("ok", true);
                response.set("logs", drainLogs());
                response.set("error", errorNode(e));
                return response;
            }
            stream = opened;
            ObjectNode response = JSON.createObjectNode().put("ok", true);
            byte[] header = opened.header();
            if (header == null) response.putNull("header_b64");
            else response.put("header_b64", B64E.encodeToString(header));
            response.set("logs", drainLogs());
            return response;
        } catch (IOException e) {
            return refusal("could not read request batch: " + e);
        }
    }

    private ObjectNode tick(JsonNode request) {
        if (stream == null) return refusal("no stream is open");
        Map<String, String> metadata = null;
        if (request.has("input_b64")) {
            // The stream carries an empty batch whose custom metadata is the
            // per-tick metadata: read the metadata, ignore the batch.
            try {
                metadata = metadataOf(decodeB64(request, "input_b64"));
            } catch (RuntimeException | IOException e) {
                return refusal("could not read tick metadata: " + e.getMessage());
            }
        }
        Map<String, String> perTick = perTickOf(metadata);
        return streamItem(() -> stream.tick(perTick), false);
    }

    private ObjectNode nextWithToken() {
        if (stream == null) return refusal("no stream is open");
        return streamItem(() -> stream.tick(null), true);
    }

    private ObjectNode exchange(JsonNode request) {
        if (stream == null) return refusal("no stream is open");
        byte[] bytes;
        try {
            bytes = decodeB64(request, "input_b64");
        } catch (RuntimeException e) {
            return refusal(e.getMessage());
        }
        try (IpcStreamReader reader = new IpcStreamReader(new ByteArrayInputStream(bytes), Allocators.root())) {
            Map<String, String> md = reader.readNextBatch();
            if (md == null) return refusal("empty IPC stream (no batch)");
            AnnotatedBatch input = new AnnotatedBatch(reader.root(), md, reader.dictionaryProvider(), null);
            return streamItem(() -> stream.exchange(input), false);
        } catch (IOException e) {
            return refusal("could not read exchange input: " + e);
        }
    }

    /** One producer/exchange turn, with the terminal bookkeeping every turn shares. */
    private ObjectNode streamItem(StreamTurn turn, boolean withToken) {
        ObjectNode response = JSON.createObjectNode().put("ok", true);
        byte[] batch;
        try {
            batch = turn.run();
        } catch (RpcError e) {
            // An error terminates the stream too.
            stream = null;
            response.put("done", true);
            response.putNull("batch_b64");
            if (withToken) response.putNull("token");
            response.set("logs", drainLogs());
            response.set("error", errorNode(e));
            return response;
        }
        if (batch == null) {
            stream = null;
            response.put("done", true);
            response.putNull("batch_b64");
            if (withToken) response.putNull("token");
            response.set("logs", drainLogs());
            response.putNull("error");
            return response;
        }
        response.put("done", false);
        response.put("batch_b64", B64E.encodeToString(batch));
        if (withToken) {
            String token = stream.stateToken();
            if (token == null) response.putNull("token");
            else response.put("token", token);
        }
        response.set("logs", drainLogs());
        response.putNull("error");
        return response;
    }

    private ObjectNode cancel() {
        ObjectNode response = JSON.createObjectNode().put("ok", true);
        if (stream != null) {
            try { stream.cancel(); } catch (RuntimeException ignore) { /* best-effort */ }
            stream = null;
        }
        response.set("logs", drainLogs());
        return response;
    }

    private ObjectNode closeStream() {
        if (stream != null) {
            try { stream.close(); } catch (RuntimeException ignore) { /* best-effort */ }
            stream = null;
        }
        return JSON.createObjectNode().put("ok", true);
    }

    /** One turn of a stream, so the terminal bookkeeping can be written once. */
    @FunctionalInterface
    private interface StreamTurn {
        byte[] run();
    }

    private static Map<String, String> perTickOf(Map<String, String> md) {
        return md == null || md.isEmpty() ? null : md;
    }

    private static Map<String, String> metadataOf(byte[] ipc) throws IOException {
        try (IpcStreamReader reader = new IpcStreamReader(new ByteArrayInputStream(ipc), Allocators.root())) {
            Map<String, String> md = reader.readNextBatch();
            return md == null ? Map.of() : new LinkedHashMap<>(md);
        }
    }

    // ------------------------------------------------------------------
    // HTTP-only ops
    // ------------------------------------------------------------------

    private ObjectNode httpAdmin(String op, JsonNode request) {
        if (byteStream == null && http == null) return refusal("not connected");
        if (http == null) return refusal("op requires http transport");
        try {
            return switch (op) {
                case "capabilities" -> capabilities();
                case "request_upload_urls" -> uploadUrls(request.path("count").asInt(1));
                case "session_begin" -> {
                    JsonNode token = request.get("token");
                    http.beginSession(token == null || token.isNull() ? null : token.asText());
                    yield JSON.createObjectNode().put("ok", true);
                }
                case "session_token" -> nullableText("token", http.currentSessionToken());
                case "session_echo_headers" -> {
                    ObjectNode response = JSON.createObjectNode().put("ok", true);
                    ObjectNode headers = response.putObject("headers");
                    http.currentEchoHeaders().forEach(headers::put);
                    yield response;
                }
                case "session_detach" -> nullableText("token", http.detachSession());
                case "session_end" -> {
                    http.endSession();
                    yield JSON.createObjectNode().put("ok", true);
                }
                default -> refusal("unknown admin op: " + op);
            };
        } catch (RpcError e) {
            // A transport failure the client library reports is a call error,
            // not a driver failure.
            ObjectNode response = JSON.createObjectNode().put("ok", true);
            response.set("error", errorNode(e));
            return response;
        } catch (RuntimeException e) {
            return refusal(op + ": " + e);
        }
    }

    private ObjectNode capabilities() {
        HttpCapabilities caps = http.capabilities();
        ObjectNode response = JSON.createObjectNode().put("ok", true);
        ObjectNode node = response.putObject("caps");
        node.put("sticky_enabled", caps.stickyEnabled());
        if (caps.stickyDefaultTtl() == null) node.putNull("sticky_default_ttl");
        else node.put("sticky_default_ttl", caps.stickyDefaultTtl().intValue());
        ArrayNode echo = node.putArray("sticky_echo_headers");
        caps.stickyEchoHeaders().forEach(echo::add);
        node.put("upload_url_support", caps.uploadUrlSupport());
        putNullableLong(node, "max_request_bytes", caps.maxRequestBytes());
        putNullableLong(node, "max_response_bytes", caps.maxResponseBytes());
        putNullableLong(node, "max_externalized_response_bytes", caps.maxExternalizedResponseBytes());
        node.put("externalization_enabled", caps.externalizationEnabled());
        putNullableLong(node, "max_upload_bytes", caps.maxUploadBytes());
        ArrayNode encodings = node.putArray("supported_encodings");
        caps.supportedEncodings().forEach(encodings::add);
        return response;
    }

    private ObjectNode uploadUrls(int count) {
        ObjectNode response = JSON.createObjectNode().put("ok", true);
        ArrayNode urls = response.putArray("urls");
        for (UploadUrl url : http.requestUploadUrls(count)) {
            ObjectNode entry = urls.addObject();
            entry.put("upload_url", url.uploadUrl());
            entry.put("download_url", url.downloadUrl());
            entry.put("expires_at", url.expiresAtUnixSeconds());
        }
        return response;
    }

    private static void putNullableLong(ObjectNode node, String field, Long value) {
        if (value == null) node.putNull(field);
        else node.put(field, value);
    }

    private ObjectNode nullableText(String field, String value) {
        ObjectNode response = JSON.createObjectNode().put("ok", true);
        if (value == null) response.putNull(field);
        else response.put(field, value);
        return response;
    }

    // ------------------------------------------------------------------
    // Control channel
    // ------------------------------------------------------------------

    private void write(ObjectNode response) {
        control.print(response.toString());
        control.print('\n');
        control.flush();
    }

    private static ObjectNode refusal(String reason) {
        return JSON.createObjectNode().put("ok", false).put("error", reason);
    }

    /**
     * Render a peer error.
     *
     * <p>{@code error_type} is the peer's own class name, relayed verbatim
     * because tests assert on the string. Translating it into a Java exception
     * name would make every such assertion a test of this driver's dictionary.
     */
    private static ObjectNode errorNode(RpcError error) {
        ObjectNode node = JSON.createObjectNode();
        node.put("error_type", error.errorType());
        node.put("error_message", error.errorMessage());
        node.put("traceback", error.remoteTraceback() == null ? "" : error.remoteTraceback());
        return node;
    }

    /**
     * Hand over the log records that arrived during this op, exactly once.
     *
     * <p>Records belong to the op during which they arrived; a test that asserts
     * on a server log fails if the driver forgets to attach them to the response
     * that produced them.
     */
    private ArrayNode drainLogs() {
        ArrayNode array = JSON.createArrayNode();
        synchronized (logs) {
            for (Message message : logs) {
                ObjectNode node = array.addObject();
                node.put("level", message.level().name());
                node.put("message", message.message());
                ObjectNode extra = node.putObject("extra");
                Map<String, Object> source = message.extra();
                if (source != null) {
                    source.forEach((key, value) -> extra.put(key, value == null ? "" : value.toString()));
                }
            }
            logs.clear();
        }
        return array;
    }

    private static byte[] decodeB64(JsonNode request, String field) {
        JsonNode node = request.get(field);
        if (node == null || !node.isTextual()) {
            throw new IllegalArgumentException(field + " is missing or not a string");
        }
        try {
            return B64D.decode(node.asText());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("base64 decode of " + field + ": " + e.getMessage());
        }
    }

    private static String text(JsonNode node, String field, String fallback) {
        JsonNode value = node.get(field);
        return value != null && value.isTextual() ? value.asText() : fallback;
    }
}
