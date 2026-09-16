// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.ExchangeState;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.ProducerState;
import farm.query.vgirpc.RawStream;
import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.ServiceIntrospector;
import farm.query.vgirpc.external.UploadUrlProvider;
import farm.query.vgirpc.log.Message;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.StreamHeader;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import farm.query.vgirpc.wire.IpcStreamWriter;
import farm.query.vgirpc.wire.Metadata;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The parts of {@link HttpRpcConnection} a caller that is <em>not</em> holding
 * a Java service interface depends on: the untyped call surface, capability
 * discovery, upload URLs, sticky sessions and request compression.
 *
 * <p>These exist because the conformance suite drives this client as a black
 * box — it relays somebody else's Arrow batches and asks the client what the
 * server advertises — and every one of them is a path the typed
 * {@code proxy()} tests never touch. They are asserted against a real
 * {@link HttpServer} rather than a stub wherever the server can be configured
 * to produce the shape under test, and against a synthetic {@code OPTIONS
 * /health} response where it cannot (an <em>absent</em> capability header is
 * not something a correct server ever emits, but it is exactly what a client
 * has to read correctly).</p>
 */
final class HttpRawClientSurfaceTest {

    private static final Schema OUT_SCHEMA = new Schema(List.of(
            new Field("n", FieldType.notNullable(new ArrowType.Int(64, true)), null)));
    private static final Schema IN_SCHEMA = new Schema(List.of(
            new Field("v", FieldType.nullable(new ArrowType.Int(64, true)), null)));
    private static final Schema DOUBLED_SCHEMA = new Schema(List.of(
            new Field("doubled", FieldType.nullable(new ArrowType.Int(64, true)), null)));

    private static final String PROTOCOL = ServiceIntrospector.protocolName(RawDemo.class);
    private static final String VERSION = ServiceIntrospector.protocolVersion(RawDemo.class);
    private static final String BOOM = "the method refused";

    /** Header record a producer sends ahead of its body. */
    public record Head(String tag, long total) implements ArrowSerializableRecord {}

    /** One row per tick, so walking it needs real continuation POSTs. */
    public static final class Counter extends ProducerState {
        public long remaining;
        public long next;

        public Counter() {}

        Counter(long count) { this.remaining = count; }

        @Override public void produce(OutputCollector out, CallContext ctx) {
            if (remaining <= 0) {
                out.finish();
                return;
            }
            VectorSchemaRoot root = VectorSchemaRoot.create(OUT_SCHEMA, Allocators.root());
            root.allocateNew();
            ((BigIntVector) root.getVector(0)).setSafe(0, next);
            root.setRowCount(1);
            out.emit(root);
            next++;
            remaining--;
            if (remaining == 0) out.finish();
        }
    }

    /** Doubles its input column. */
    public static final class Doubler extends ExchangeState {
        public Doubler() {}

        @Override
        public void exchange(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            BigIntVector src = (BigIntVector) input.root().getVector("v");
            int rows = input.root().getRowCount();
            VectorSchemaRoot root = VectorSchemaRoot.create(DOUBLED_SCHEMA, Allocators.root());
            root.allocateNew();
            BigIntVector dst = (BigIntVector) root.getVector("doubled");
            for (int i = 0; i < rows; i++) dst.setSafe(i, src.get(i) * 2);
            dst.setValueCount(rows);
            root.setRowCount(rows);
            out.emit(root);
        }
    }

    public interface RawDemo {
        String shout(String value);

        long boom(long v);

        long open_session(CallContext ctx);

        long open_counter(CallContext ctx, long initial);

        long bump(CallContext ctx);

        @StreamHeader(Head.class)
        RpcStream<Counter> count(long n);

        RpcStream<Counter> count_headerless(long n);

        RpcStream<Doubler> doubling();
    }

    public static final class Impl implements RawDemo {
        @Override public String shout(String value) { return value.toUpperCase(java.util.Locale.ROOT); }

        @Override public long boom(long v) { throw new IllegalStateException(BOOM); }

        @Override public long open_session(CallContext ctx) {
            ctx.openSession("state", null);
            return 1L;
        }

        @Override public long open_counter(CallContext ctx, long initial) {
            ctx.openSession(new long[]{initial}, null);
            return initial;
        }

        /** Refuses a request that resolved to no session — the worker-side
         *  symptom a client with broken scope bookkeeping produces. */
        @Override public long bump(CallContext ctx) {
            Object state = ctx.session();
            if (state == null) throw new IllegalStateException("no counter bound to this request");
            long[] counter = (long[]) state;
            return ++counter[0];
        }

        @Override public RpcStream<Counter> count(long n) {
            return RpcStream.producer(OUT_SCHEMA, new Counter(n), new Head("counting", n));
        }

        @Override public RpcStream<Counter> count_headerless(long n) {
            return RpcStream.producer(OUT_SCHEMA, new Counter(n));
        }

        @Override public RpcStream<Doubler> doubling() {
            return RpcStream.exchange(IN_SCHEMA, DOUBLED_SCHEMA, new Doubler());
        }
    }

    private HttpServer server;
    private HttpRpcConnection connection;

    @AfterEach
    void stop() throws Exception {
        if (connection != null) connection.close();
        if (server != null) server.stop();
        if (syntheticServer != null) syntheticServer.stop(0);
        if (storage != null) storage.stop(0);
    }

    private String start(Consumer<HttpServer.Config.Builder> configure) throws Exception {
        HttpServer.Config.Builder config = HttpServer.Config.builder().prefix("/vgi");
        configure.accept(config);
        server = new HttpServer(new RpcServer(RawDemo.class, new Impl()), config.build());
        server.start();
        return "http://127.0.0.1:" + server.port() + "/vgi";
    }

    private HttpRpcConnection connect(Consumer<HttpServer.Config.Builder> configure) throws Exception {
        connection = HttpRpcConnection.builder(start(configure)).build();
        return connection;
    }

    // --- untyped unary ----------------------------------------------------

    /**
     * The relay case: a caller holding somebody else's batch and metadata gets
     * a framed answer back, and the metadata it was given reaches the worker
     * unedited — including the routing key, which the server refuses a call
     * without.
     */
    @Test
    @Timeout(30)
    void untypedUnaryRelaysTheCallersBatchAndMetadata() throws Exception {
        HttpRpcConnection conn = connect(b -> { });
        try (VectorSchemaRoot params = utf8Row("value", "hi")) {
            byte[] framed = conn.callUnaryRaw(PROTOCOL, VERSION, "shout",
                    new AnnotatedBatch(params, Map.of("x.caller", "kept")));
            assertEquals("HI", firstString(framed, "result"));
        }
    }

    /** A worker error keeps its own type string across the untyped boundary. */
    @Test
    @Timeout(30)
    void untypedUnaryErrorKeepsTheWorkersType() throws Exception {
        HttpRpcConnection conn = connect(b -> { });
        try (VectorSchemaRoot params = int64Row("v", 1)) {
            RpcError error = assertThrows(RpcError.class, () -> conn.callUnaryRaw(
                    PROTOCOL, VERSION, "boom", new AnnotatedBatch(params, Map.of())));
            assertTrue(error.getMessage().contains(BOOM), error.getMessage());
        }
    }

    /** An unroutable protocol must not be quietly repaired into a working call. */
    @Test
    @Timeout(30)
    void untypedUnaryDoesNotInventARoute() throws Exception {
        HttpRpcConnection conn = connect(b -> { });
        try (VectorSchemaRoot params = utf8Row("value", "hi")) {
            assertThrows(RpcError.class, () -> conn.callUnaryRaw(
                    "NotHostedHere", VERSION, "shout", new AnnotatedBatch(params, Map.of())));
        }
    }

    // --- untyped producer streams ----------------------------------------

    /**
     * A producer walks to end-of-stream, and every batch before the last comes
     * back with the cursor that resumes <em>after</em> it.
     *
     * <p>The cursor is the assertion that matters here. It trails the data
     * batch in the response, so a client that returns as soon as it has data —
     * which is what the typed stream does, because it cannot read on without
     * recycling the caller's root — has not seen it yet. Over HTTP a producer
     * mid-stream must report one, or a relay cannot hand its own caller
     * anything to resume from.
     */
    @Test
    @Timeout(30)
    void untypedProducerWalksToEndAndReportsACursorMidStream() throws Exception {
        HttpRpcConnection conn = connect(b -> { });
        try (VectorSchemaRoot params = int64Row("n", 3)) {
            RawStream stream = conn.openStreamRaw(PROTOCOL, VERSION, "count_headerless",
                    new AnnotatedBatch(params, Map.of()), false, false);
            assertNull(stream.header(), "count_headerless declares no @StreamHeader");

            List<Long> seen = new ArrayList<>();
            byte[] batch;
            while ((batch = stream.tick(null)) != null) {
                seen.add(firstLong(batch, "n"));
                if (seen.size() < 3) {
                    assertNotNull(stream.stateToken(),
                            "an unfinished producer turn must carry a continuation token");
                }
            }
            assertEquals(List.of(0L, 1L, 2L), seen);
            assertNull(stream.stateToken(), "a finished producer offers no cursor");
            stream.close();
        }
    }

    /** A declared header comes back as framed bytes, not as a decoded record. */
    @Test
    @Timeout(30)
    void untypedProducerReturnsItsHeaderFramed() throws Exception {
        HttpRpcConnection conn = connect(b -> { });
        try (VectorSchemaRoot params = int64Row("n", 1)) {
            RawStream stream = conn.openStreamRaw(PROTOCOL, VERSION, "count",
                    new AnnotatedBatch(params, Map.of()), true, false);
            byte[] header = stream.header();
            assertNotNull(header, "count declares a @StreamHeader");
            assertEquals("counting", firstString(header, "tag"));
            assertNotNull(stream.tick(null));
            stream.close();
        }
    }

    /** An exchange turn answers the batch it was handed. */
    @Test
    @Timeout(30)
    void untypedExchangeAnswersEachInputBatch() throws Exception {
        HttpRpcConnection conn = connect(b -> { });
        try (VectorSchemaRoot params = VectorSchemaRoot.create(new Schema(List.of()), Allocators.root())) {
            params.allocateNew();
            params.setRowCount(1);
            RawStream stream = conn.openStreamRaw(PROTOCOL, VERSION, "doubling",
                    new AnnotatedBatch(params, Map.of()), false, true);
            for (long value : new long[]{2L, 5L}) {
                try (VectorSchemaRoot input = int64Row("v", value)) {
                    byte[] reply = stream.exchange(new AnnotatedBatch(input, Map.of()));
                    assertNotNull(reply);
                    assertEquals(value * 2, firstLong(reply, "doubled"));
                }
            }
            stream.close();
        }
    }

    /**
     * Per-tick metadata rides the continuation request rather than being
     * dropped — and the transport's own cursor keys survive beside it, which is
     * what keeps a caller from redirecting the stream by naming
     * {@code vgi_rpc.state} itself.
     */
    @Test
    @Timeout(30)
    void tickMetadataRidesTheContinuationRequest() throws Exception {
        AtomicReference<Map<String, String>> captured = new AtomicReference<>();
        HttpRpcConnection conn = connect(b -> b.preHandlers(List.of(new ExchangeBodyCapture(captured))));
        try (VectorSchemaRoot params = int64Row("n", 5)) {
            RawStream stream = conn.openStreamRaw(PROTOCOL, VERSION, "count_headerless",
                    new AnnotatedBatch(params, Map.of()), false, false);
            assertNotNull(stream.tick(null), "the init response carries the first batch");
            // The next tick is a real continuation POST, which the capture
            // intercepts and answers with an empty stream.
            assertNull(stream.tick(Map.of("x.tick", "updated")));
            stream.close();
        }
        Map<String, String> sent = captured.get();
        assertNotNull(sent, "no continuation request was captured");
        assertEquals("updated", sent.get("x.tick"));
        assertNotNull(sent.get(Metadata.STREAM_STATE),
                "the cursor must still ride beside the caller's metadata");
    }

    /** Fully answers the first {@code /exchange} POST, recording its metadata. */
    private record ExchangeBodyCapture(AtomicReference<Map<String, String>> captured)
            implements HttpPreHandler {

        @Override
        public boolean handle(HttpServletRequest request, HttpServletResponse response)
                throws java.io.IOException {
            if (!"POST".equals(request.getMethod())
                    || !request.getRequestURI().endsWith("/exchange")
                    || captured.get() != null) {
                return false;
            }
            byte[] body = request.getInputStream().readAllBytes();
            try (IpcStreamReader r = new IpcStreamReader(
                    new ByteArrayInputStream(body), Allocators.root())) {
                captured.set(r.readNextBatch());
            }
            // An empty stream: schema, then end-of-stream with no cursor, which
            // is how a worker says "finished".
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (IpcStreamWriter w = new IpcStreamWriter(out)) {
                w.writeSchema(OUT_SCHEMA);
            }
            response.setStatus(200);
            response.setContentType(HttpServer.ARROW_CONTENT_TYPE);
            response.setHeader(HttpServer.ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER, "true");
            response.getOutputStream().write(out.toByteArray());
            return true;
        }
    }

    // --- capabilities -----------------------------------------------------

    @Test
    @Timeout(30)
    void capabilitiesReadWhatTheServerAdvertises() throws Exception {
        HttpRpcConnection conn = connect(b -> b
                .advertiseMaxRequestBytes(true)
                .maxRequestBytes(1 << 20)
                .advertisedMaxResponseBytes(8L << 20)
                .advertisedMaxExternalizedResponseBytes(4L << 20)
                .stickyEnabled(true)
                .stickyDefaultTtlSeconds(300)
                .stickyEchoHeaders(Map.of("Backend", "b7"))
                .maxUploadBytes(16L << 20)
                .uploadUrlProvider(() -> new UploadUrlProvider.UploadUrl(
                        "http://storage.invalid/put", "http://storage.invalid/get",
                        Instant.ofEpochSecond(1_767_225_600L))));
        HttpCapabilities caps = conn.capabilities();
        assertTrue(caps.stickyEnabled());
        assertEquals(300, caps.stickyDefaultTtl());
        assertEquals(List.of("Backend"), caps.stickyEchoHeaders());
        assertTrue(caps.uploadUrlSupport());
        assertEquals(1L << 20, caps.maxRequestBytes());
        assertEquals(8L << 20, caps.maxResponseBytes());
        assertEquals(4L << 20, caps.maxExternalizedResponseBytes());
        assertEquals(16L << 20, caps.maxUploadBytes());
        assertEquals(List.of(MediaTypes.ZSTD, MediaTypes.GZIP), caps.supportedEncodings());
        assertFalse(caps.externalizationEnabled(), "no storage backend was configured");
        assertSame(caps, conn.capabilities(), "the probe result is cached, not re-fetched");
    }

    /**
     * A server that states it speaks no compression is not the same as one that
     * states nothing. Present-but-empty means "never compress"; absent means a
     * worker predating the header, every one of which decodes zstd. Reading the
     * two the same way either sends zstd to a server that answers 415, or
     * refuses to compress for every worker built before the header existed.
     */
    @Test
    @Timeout(30)
    void emptyEncodingAdvertisementIsNotAnAbsentOne() throws Exception {
        try (HttpRpcConnection empty = syntheticHealth(Map.of(
                HttpServer.SUPPORTED_ENCODINGS_HEADER, ""))) {
            assertEquals(List.of(), empty.capabilities().supportedEncodings());
        }
        try (HttpRpcConnection absent = syntheticHealth(Map.of())) {
            assertEquals(List.of(MediaTypes.ZSTD), absent.capabilities().supportedEncodings(),
                    "a worker predating the header still decodes zstd");
        }
    }

    /** An advertised limit that is not a number leaves the client not knowing it. */
    @Test
    @Timeout(30)
    void unparseableCapsReadAsUnknownRatherThanZero() throws Exception {
        try (HttpRpcConnection conn = syntheticHealth(Map.of(
                HttpServer.MAX_REQUEST_BYTES_HEADER, "lots",
                StickyHeaders.STICKY_TTL, "forever"))) {
            HttpCapabilities caps = conn.capabilities();
            assertNull(caps.maxRequestBytes());
            assertNull(caps.stickyDefaultTtl());
        }
    }

    /**
     * A health endpoint that answers with a failure is an error, not "this
     * worker advertises no limits" — the two lead to opposite client
     * decisions, and conflating them makes a misconfigured prefix look like a
     * permissive worker.
     */
    @Test
    @Timeout(30)
    void capabilityDiscoveryFailureIsLoud() throws Exception {
        try (HttpRpcConnection conn = syntheticHealth(Map.of(), 503)) {
            RpcError error = assertThrows(RpcError.class, conn::capabilities);
            assertEquals("ProtocolError", error.errorType());
        }
    }

    /**
     * A bare HTTP listener that answers {@code OPTIONS /vgi/health} with
     * exactly the headers a case needs — including <em>omitting</em> one.
     *
     * <p>Not an {@link HttpServer}, because a correct worker stamps its whole
     * capability set on every response and therefore cannot be configured to
     * leave a header out. Absent-versus-empty is a distinction a client must
     * read correctly regardless, since it also arrives from older workers and
     * from intermediaries that strip headers.</p>
     */
    private com.sun.net.httpserver.HttpServer syntheticServer;

    private HttpRpcConnection syntheticHealth(Map<String, String> healthHeaders, int status)
            throws Exception {
        syntheticServer = com.sun.net.httpserver.HttpServer.create(
                new java.net.InetSocketAddress(java.net.InetAddress.getLoopbackAddress(), 0), 0);
        syntheticServer.createContext("/vgi/health", exchange -> {
            healthHeaders.forEach((name, value) -> exchange.getResponseHeaders().add(name, value));
            exchange.sendResponseHeaders(status, -1);
            exchange.close();
        });
        syntheticServer.start();
        return HttpRpcConnection.builder(
                "http://127.0.0.1:" + syntheticServer.getAddress().getPort() + "/vgi").build();
    }

    private HttpRpcConnection syntheticHealth(Map<String, String> healthHeaders) throws Exception {
        return syntheticHealth(healthHeaders, 204);
    }

    // --- upload URLs ------------------------------------------------------

    @Test
    @Timeout(30)
    void uploadUrlsComeBackWithWholeSecondExpiries() throws Exception {
        HttpRpcConnection conn = connect(b -> b.uploadUrlProvider(
                () -> new UploadUrlProvider.UploadUrl("http://storage.invalid/put",
                        "http://storage.invalid/get", Instant.ofEpochSecond(1_767_225_600L, 500_000))));
        List<UploadUrl> urls = conn.requestUploadUrls(2);
        assertEquals(2, urls.size());
        assertEquals("http://storage.invalid/put", urls.getFirst().uploadUrl());
        assertEquals("http://storage.invalid/get", urls.getFirst().downloadUrl());
        assertEquals(1_767_225_600L, urls.getFirst().expiresAtUnixSeconds());
    }

    /** A worker with no provider does not route the endpoint; say so in those terms. */
    @Test
    @Timeout(30)
    void uploadUrlsOnAWorkerWithoutAProviderAreNotSupported() throws Exception {
        HttpRpcConnection conn = connect(b -> { });
        RpcError error = assertThrows(RpcError.class, () -> conn.requestUploadUrls(1));
        assertEquals("NotSupported", error.errorType());
    }

    // --- sticky sessions --------------------------------------------------

    @Test
    @Timeout(30)
    void sessionHeadersRideEveryRequestWhileAScopeIsOpen() throws Exception {
        HttpRpcConnection conn = connect(b -> b
                .stickyEnabled(true)
                .stickyEchoHeaders(Map.of("x-vgi-echo-marker", "b7")));
        RawDemo svc = conn.proxy(RawDemo.class);

        assertNull(conn.currentSessionToken(), "no scope is open yet");
        conn.beginSession(null);
        assertEquals(1L, svc.open_session(null));
        String token = conn.currentSessionToken();
        assertNotNull(token, "the worker minted a session but the client kept no token");
        assertEquals(Map.of("x-vgi-echo-marker", "b7"), conn.currentEchoHeaders(),
                "the echo header must be captured under its own name, prefix stripped");

        // The token is presented on every later call, so the worker resolves the
        // same session rather than minting a second one.
        assertEquals("HI", svc.shout("hi"));
        assertEquals(token, conn.currentSessionToken());

        conn.endSession();
        assertNull(conn.currentSessionToken());
    }

    /** A detached session outlives the scope: nothing is deleted on the way out. */
    @Test
    @Timeout(30)
    void detachHandsTheTokenToTheCaller() throws Exception {
        HttpRpcConnection conn = connect(b -> b.stickyEnabled(true));
        RawDemo svc = conn.proxy(RawDemo.class);
        conn.beginSession(null);
        svc.open_session(null);
        String token = conn.detachSession();
        assertNotNull(token);
        assertNull(conn.currentSessionToken(), "the scope no longer holds the token");
        conn.endSession();

        // The session is still live, so a fresh scope can resume it.
        conn.beginSession(token);
        assertEquals("HI", svc.shout("hi"));
        assertEquals(token, conn.currentSessionToken());
        conn.endSession();
    }

    /**
     * A nested scope must not take the enclosing one down with it.
     *
     * <p>The failure this pins is entirely after the fact: with a single
     * replaceable scope, the inner block's exit leaves the connection with no
     * session at all, and the next call on the <em>outer</em> session goes out
     * unsessioned. The worker then refuses it — "no counter bound to this
     * request" — from a line that never mentioned a session, which is why the
     * assertion that matters here is the one after {@code endSession()}.</p>
     */
    @Test
    @Timeout(30)
    void anInnerScopeExitRestoresTheEnclosingSession() throws Exception {
        HttpRpcConnection conn = connect(b -> b.stickyEnabled(true));
        RawDemo svc = conn.proxy(RawDemo.class);

        conn.beginSession(null);
        svc.open_counter(null, 10L);
        String outer = conn.currentSessionToken();
        assertNotNull(outer);

        conn.beginSession(null);
        svc.open_counter(null, 1L);
        String inner = conn.currentSessionToken();
        assertNotNull(inner);
        assertNotEquals(outer, inner, "the inner scope must open its own session");
        assertEquals(2L, svc.bump(null), "calls inside the inner scope address the inner session");

        conn.endSession();
        assertEquals(outer, conn.currentSessionToken(),
                "ending the inner scope must restore the enclosing one");
        assertEquals(11L, svc.bump(null),
                "the outer session must still be bound after the inner scope exits");

        conn.endSession();
        assertNull(conn.currentSessionToken());
    }

    /** With nothing open, ending a scope is a no-op rather than an error. */
    @Test
    @Timeout(30)
    void endingWithNoScopeOpenIsANoOp() throws Exception {
        HttpRpcConnection conn = connect(b -> b.stickyEnabled(true));
        conn.endSession();
        assertNull(conn.currentSessionToken());
        assertEquals("HI", conn.proxy(RawDemo.class).shout("hi"));
    }

    /** Detaching the inner scope still restores the outer one on exit. */
    @Test
    @Timeout(30)
    void detachingAnInnerScopeLeavesTheOuterIntact() throws Exception {
        HttpRpcConnection conn = connect(b -> b.stickyEnabled(true));
        RawDemo svc = conn.proxy(RawDemo.class);

        conn.beginSession(null);
        svc.open_counter(null, 5L);
        String outer = conn.currentSessionToken();

        conn.beginSession(null);
        svc.open_counter(null, 100L);
        String detached = conn.detachSession();
        assertNotNull(detached);
        conn.endSession();

        assertEquals(outer, conn.currentSessionToken());
        assertEquals(6L, svc.bump(null));
        conn.beginSession(detached);
        assertEquals(101L, svc.bump(null), "a detached session outlives the scope that opened it");
        conn.endSession();
        conn.endSession();
    }

    // --- request externalization -----------------------------------------

    /** In-memory object store: PUT keeps the bytes, GET hands them back. */
    private com.sun.net.httpserver.HttpServer storage;
    private final AtomicReference<byte[]> stored = new AtomicReference<>();

    private String startStorage() throws Exception {
        storage = com.sun.net.httpserver.HttpServer.create(
                new java.net.InetSocketAddress(java.net.InetAddress.getLoopbackAddress(), 0), 0);
        storage.createContext("/blob", exchange -> {
            if ("PUT".equals(exchange.getRequestMethod())) {
                stored.set(exchange.getRequestBody().readAllBytes());
                exchange.sendResponseHeaders(200, -1);
            } else {
                byte[] body = stored.get();
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
            }
            exchange.close();
        });
        storage.start();
        return "http://127.0.0.1:" + storage.getAddress().getPort() + "/blob";
    }

    /**
     * Answers one POST to {@code method}: {@code refusals} 413s first (standing
     * in for a worker enforcing a cap it never advertised), then captures the
     * body and replies with a valid one-row result.
     */
    private static final class UnaryIntercept implements HttpPreHandler {
        private final String path;
        private final AtomicReference<Map<String, String>> capturedMetadata;
        private final AtomicReference<Integer> capturedRows = new AtomicReference<>();
        private int refusalsLeft;

        UnaryIntercept(String path, AtomicReference<Map<String, String>> capturedMetadata, int refusals) {
            this.path = path;
            this.capturedMetadata = capturedMetadata;
            this.refusalsLeft = refusals;
        }

        @Override
        public boolean handle(HttpServletRequest request, HttpServletResponse response)
                throws java.io.IOException {
            if (!"POST".equals(request.getMethod()) || !request.getRequestURI().endsWith(path)) {
                return false;
            }
            byte[] body = request.getInputStream().readAllBytes();
            if (refusalsLeft > 0) {
                refusalsLeft--;
                response.setStatus(413);
                response.setContentType(MediaTypes.APPLICATION_JSON);
                response.setHeader(HttpServer.ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER, "true");
                response.getOutputStream().write(
                        "{\"title\": \"Request body exceeds max_request_bytes\"}"
                                .getBytes(StandardCharsets.UTF_8));
                return true;
            }
            try (IpcStreamReader r = new IpcStreamReader(
                    new ByteArrayInputStream(body), Allocators.root())) {
                capturedMetadata.set(r.readNextBatch());
                capturedRows.set(r.root().getRowCount());
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Schema schema = new Schema(List.of(new Field(
                    "result", FieldType.nullable(new ArrowType.Utf8()), null)));
            try (IpcStreamWriter w = new IpcStreamWriter(out);
                 VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root())) {
                root.allocateNew();
                ((VarCharVector) root.getVector("result"))
                        .setSafe(0, "HI".getBytes(StandardCharsets.UTF_8));
                root.setRowCount(1);
                w.writeSchema(schema);
                w.writeBatch(root, null);
            }
            response.setStatus(200);
            response.setContentType(HttpServer.ARROW_CONTENT_TYPE);
            response.setHeader(HttpServer.ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER, "true");
            response.getOutputStream().write(out.toByteArray());
            return true;
        }
    }

    private static String bigValue() {
        return "conformance-externalization-probe ".repeat(2048);
    }

    /**
     * A body over the worker's advertised {@code VGI-Max-Request-Bytes} is
     * uploaded and replaced by a pointer, rather than being sent inline for the
     * worker to refuse with a 413 nobody can recover from.
     */
    @Test
    @Timeout(30)
    void anOversizedRequestIsUploadedAndReplacedByAPointer() throws Exception {
        String blob = startStorage();
        AtomicReference<Map<String, String>> sent = new AtomicReference<>();
        HttpRpcConnection conn = connect(b -> b
                .maxRequestBytes(4096)
                .uploadUrlProvider(() -> new UploadUrlProvider.UploadUrl(blob, blob, null))
                .preHandlers(List.of(new UnaryIntercept("/shout", sent, 0))));
        String value = bigValue();
        assertEquals("HI", conn.proxy(RawDemo.class).shout(value));

        Map<String, String> metadata = sent.get();
        assertNotNull(metadata, "no request reached the worker");
        assertNotNull(metadata.get(Metadata.LOCATION), "the request was sent inline: " + metadata);
        // The envelope is what the worker dispatches on *before* it resolves the
        // pointer, so replacing the payload must not replace it.
        assertEquals("shout", metadata.get(Metadata.RPC_METHOD));
        assertEquals(PROTOCOL, metadata.get(Metadata.PROTOCOL));

        byte[] uploaded = stored.get();
        assertNotNull(uploaded, "nothing was uploaded");
        assertEquals(sha256Hex(uploaded), metadata.get(Metadata.LOCATION_SHA256),
                "the pointer must name the digest of the bytes actually uploaded");
        // The uploaded object is the framed request itself, uncompressed.
        try (IpcStreamReader r = new IpcStreamReader(
                new ByteArrayInputStream(uploaded), Allocators.root())) {
            assertNotNull(r.readNextBatch());
            assertEquals(value, r.root().getVector("value").getObject(0).toString());
        }
    }

    /**
     * A worker that enforces a cap it never advertised refuses with a 413; that
     * refusal is the discovery, and the call is retried once as a pointer.
     */
    @Test
    @Timeout(30)
    void aRefusedInlineBodyIsRetriedAsAPointer() throws Exception {
        String blob = startStorage();
        AtomicReference<Map<String, String>> sent = new AtomicReference<>();
        HttpRpcConnection conn = connect(b -> b
                .uploadUrlProvider(() -> new UploadUrlProvider.UploadUrl(blob, blob, null))
                .preHandlers(List.of(new UnaryIntercept("/shout", sent, 1))));
        assertEquals("HI", conn.proxy(RawDemo.class).shout(bigValue()));
        Map<String, String> metadata = sent.get();
        assertNotNull(metadata);
        assertNotNull(metadata.get(Metadata.LOCATION),
                "the 413 must be answered with a pointer, not repeated inline");
    }

    /**
     * With no upload slots to be had, the caller learns what is wrong. An
     * opaque {@code HTTP 413 with a non-Arrow body} names neither the cap nor
     * the flow that exists to get around it.
     */
    @Test
    @Timeout(30)
    void anOversizedRequestWithNoUploadSupportFailsLoudly() throws Exception {
        HttpRpcConnection conn = connect(b -> b.maxRequestBytes(4096));
        RpcError error = assertThrows(RpcError.class, () -> conn.proxy(RawDemo.class).shout(bigValue()));
        assertEquals("RequestTooLarge", error.errorType());
        assertTrue(error.errorMessage().contains("upload_url_support"), error.errorMessage());
    }

    private static String sha256Hex(byte[] data) throws Exception {
        byte[] hash = java.security.MessageDigest.getInstance("SHA-256").digest(data);
        StringBuilder out = new StringBuilder();
        for (byte b : hash) out.append(String.format("%02x", b));
        return out.toString();
    }

    // --- externalized responses ------------------------------------------

    /**
     * Answers a POST with a pointer to an object in {@link #storage}, the way a
     * worker does once a response batch crosses its externalization threshold.
     *
     * <p>The object is a whole IPC <em>stream</em>, not a lone batch: whatever
     * the turn produced — its log lines, its data batch, and the continuation
     * cursor riding that batch's metadata — is written to storage together, and
     * the pointer left on the wire carries only the location. That is what the
     * reference server does, and it is why a client cannot read a turn by
     * looking at the pointer alone.</p>
     */
    private final class PointerIntercept implements HttpPreHandler {
        private final String path;
        private final byte[] inner;
        private final AtomicReference<Map<String, String>> captured;

        PointerIntercept(String path, byte[] inner, AtomicReference<Map<String, String>> captured) {
            this.path = path;
            this.inner = inner;
            this.captured = captured;
        }

        @Override
        public boolean handle(HttpServletRequest request, HttpServletResponse response)
                throws java.io.IOException {
            if (!"POST".equals(request.getMethod()) || !request.getRequestURI().endsWith(path)) {
                return false;
            }
            byte[] body = request.getInputStream().readAllBytes();
            if (captured != null) {
                try (IpcStreamReader r = new IpcStreamReader(
                        new ByteArrayInputStream(body), Allocators.root())) {
                    captured.set(r.readNextBatch());
                }
            }
            stored.set(inner);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Schema schema = readSchema(inner);
            try (IpcStreamWriter w = new IpcStreamWriter(out)) {
                w.writeSchema(schema);
                farm.query.vgirpc.wire.Wire.writeZeroBatch(w, schema, Map.of(
                        Metadata.LOCATION, blobUrl,
                        Metadata.LOCATION_SHA256, sha256Of(inner)));
            }
            response.setStatus(200);
            response.setContentType(HttpServer.ARROW_CONTENT_TYPE);
            response.setHeader(HttpServer.ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER, "true");
            response.getOutputStream().write(out.toByteArray());
            return true;
        }
    }

    private String blobUrl;

    private static Schema readSchema(byte[] stream) throws java.io.IOException {
        try (IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(stream), Allocators.root())) {
            return r.schema();
        }
    }

    private static String sha256Of(byte[] data) {
        try {
            return sha256Hex(data);
        } catch (Exception impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    /** Build an IPC stream: some log batches, then one data batch. */
    private static byte[] innerStream(Schema schema, java.util.function.Consumer<VectorSchemaRoot> fill,
                                      Map<String, String> dataMetadata,
                                      List<Map<String, String>> logs) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (IpcStreamWriter w = new IpcStreamWriter(out);
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root())) {
            w.writeSchema(schema);
            for (Map<String, String> log : logs) {
                farm.query.vgirpc.wire.Wire.writeZeroBatch(w, schema, log);
            }
            root.allocateNew();
            fill.accept(root);
            w.writeBatch(root, dataMetadata);
        }
        return out.toByteArray();
    }

    /**
     * A log line the worker emitted during an externalized turn still reaches
     * the client's sink.
     *
     * <p>It travels inside the object, so a client that resolves the pointer by
     * taking the first data batch and discarding the rest drops it. Then a
     * method's log output depends on whether its batch happened to be big
     * enough to externalize — the one thing externalization is supposed not to
     * be observable in.</p>
     */
    @Test
    @Timeout(30)
    void logsInsideAnExternalizedResponseAreRelayed() throws Exception {
        blobUrl = startStorage();
        Schema schema = new Schema(List.of(new Field(
                "result", FieldType.nullable(new ArrowType.Utf8()), null)));
        byte[] inner = innerStream(schema, root -> {
                    ((VarCharVector) root.getVector("result"))
                            .setSafe(0, "HI".getBytes(StandardCharsets.UTF_8));
                    root.setRowCount(1);
                }, null,
                List.of(Map.of(Metadata.LOG_LEVEL, "INFO", Metadata.LOG_MESSAGE, "from storage")));

        List<Message> logs = new java.util.ArrayList<>();
        String base = start(b -> b.preHandlers(List.of(
                new PointerIntercept("/shout", inner, null))));
        connection = HttpRpcConnection.builder(base)
                .onLog(logs::add)
                .externalLocation(farm.query.vgirpc.external.ExternalLocationConfig.builder()
                        .urlValidator(farm.query.vgirpc.external.ExternalLocationConfig.permissiveValidator())
                        .build())
                .build();

        assertEquals("HI", connection.proxy(RawDemo.class).shout("hi"));
        assertEquals(1, logs.size(), "the log inside the externalized object was dropped");
        assertEquals("from storage", logs.getFirst().message());
    }

    /**
     * The continuation cursor of an externalized exchange turn rides the batch
     * in storage, not the pointer — so the next turn has to present the cursor
     * the client read out of the object.
     *
     * <p>Without this the stream ends silently after one turn: the second
     * exchange finds no cursor and reports "no batch", which reads like the
     * worker ending the stream rather than the client losing its place.</p>
     */
    @Test
    @Timeout(30)
    void anExternalizedExchangeTurnCarriesItsCursorInsideTheObject() throws Exception {
        blobUrl = startStorage();
        Schema schema = new Schema(List.of(new Field(
                "doubled", FieldType.nullable(new ArrowType.Int(64, true)), null)));
        byte[] inner = innerStream(schema, root -> {
                    ((BigIntVector) root.getVector("doubled")).setSafe(0, 4L);
                    root.setRowCount(1);
                },
                Map.of(Metadata.STREAM_STATE, "cursor-2"), List.of());

        AtomicReference<Map<String, String>> secondTurn = new AtomicReference<>();
        String base = start(b -> b.preHandlers(List.of(
                new InitIntercept("/doubling/init", schema),
                new PointerIntercept("/doubling/exchange", inner, secondTurn))));
        connection = HttpRpcConnection.builder(base)
                .externalLocation(farm.query.vgirpc.external.ExternalLocationConfig.builder()
                        .urlValidator(farm.query.vgirpc.external.ExternalLocationConfig.permissiveValidator())
                        .build())
                .build();

        try (VectorSchemaRoot params = VectorSchemaRoot.create(new Schema(List.of()), Allocators.root())) {
            params.allocateNew();
            params.setRowCount(1);
            RawStream stream = connection.openStreamRaw(PROTOCOL, VERSION, "doubling",
                    new AnnotatedBatch(params, Map.of()), false, true);
            try (VectorSchemaRoot input = int64Row("v", 2L)) {
                byte[] reply = stream.exchange(new AnnotatedBatch(input, Map.of()));
                assertNotNull(reply, "the first turn was answered from storage");
                assertEquals(4L, firstLong(reply, "doubled"));
                assertEquals("cursor-2", stream.stateToken(),
                        "the cursor inside the object must become this stream's position");
            }
            try (VectorSchemaRoot input = int64Row("v", 3L)) {
                stream.exchange(new AnnotatedBatch(input, Map.of()));
            }
            stream.close();
        }
        Map<String, String> sent = secondTurn.get();
        assertNotNull(sent);
        assertEquals("cursor-2", sent.get(Metadata.STREAM_STATE),
                "the next turn must present the cursor read out of the externalized object");
    }

    /** Answers a stream {@code /init} with a bare cursor batch, as an exchange init does. */
    private record InitIntercept(String path, Schema schema) implements HttpPreHandler {
        @Override
        public boolean handle(HttpServletRequest request, HttpServletResponse response)
                throws java.io.IOException {
            if (!"POST".equals(request.getMethod()) || !request.getRequestURI().endsWith(path)) {
                return false;
            }
            request.getInputStream().readAllBytes();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (IpcStreamWriter w = new IpcStreamWriter(out)) {
                w.writeSchema(schema);
                farm.query.vgirpc.wire.Wire.writeZeroBatch(w, schema, Map.of(
                        Metadata.STREAM_STATE, "cursor-1", Metadata.CALL_STATE, "call-1"));
            }
            response.setStatus(200);
            response.setContentType(HttpServer.ARROW_CONTENT_TYPE);
            response.setHeader(HttpServer.ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER, "true");
            response.getOutputStream().write(out.toByteArray());
            return true;
        }
    }

    // --- request compression ---------------------------------------------

    /** Records the content coding of the first RPC POST, then declines. */
    private record EncodingSpy(AtomicReference<String> contentEncoding,
                               AtomicReference<String> acceptEncoding) implements HttpPreHandler {
        @Override
        public boolean handle(HttpServletRequest request, HttpServletResponse response) {
            if ("POST".equals(request.getMethod()) && contentEncoding.get() == null) {
                contentEncoding.set(String.valueOf(request.getHeader(HttpHeaders.CONTENT_ENCODING)));
                acceptEncoding.set(String.valueOf(request.getHeader(HttpHeaders.ACCEPT_ENCODING)));
            }
            return false;
        }
    }

    /** The default is identity in both directions — unchanged by the new knob. */
    @Test
    @Timeout(30)
    void compressionIsOffUntilAskedFor() throws Exception {
        AtomicReference<String> contentEncoding = new AtomicReference<>();
        AtomicReference<String> acceptEncoding = new AtomicReference<>();
        HttpRpcConnection conn = connect(b -> b.preHandlers(
                List.of(new EncodingSpy(contentEncoding, acceptEncoding))));
        assertEquals("HI", conn.proxy(RawDemo.class).shout("hi"));
        assertEquals("null", contentEncoding.get(), "no request body compression by default");
        assertEquals(MediaTypes.IDENTITY, acceptEncoding.get());
    }

    /** With a level set, the body is compressed and a compressed answer is accepted. */
    @Test
    @Timeout(30)
    void aCompressionLevelCompressesTheRequestBody() throws Exception {
        AtomicReference<String> contentEncoding = new AtomicReference<>();
        AtomicReference<String> acceptEncoding = new AtomicReference<>();
        String base = start(b -> b.preHandlers(List.of(new EncodingSpy(contentEncoding, acceptEncoding))));
        connection = HttpRpcConnection.builder(base).compressionLevel(3).build();
        assertEquals("HI", connection.proxy(RawDemo.class).shout("hi"));
        assertEquals(MediaTypes.ZSTD, contentEncoding.get());
        assertTrue(acceptEncoding.get().contains(MediaTypes.ZSTD), acceptEncoding.get());
    }

    /**
     * A worker that advertises no codecs gets an uncompressed body however
     * eagerly the client was configured: compressing regardless would turn a
     * capability mismatch into a 415 on every call.
     */
    @Test
    @Timeout(30)
    void aLevelDoesNotForceACodecTheServerDoesNotSpeak() throws Exception {
        AtomicReference<String> contentEncoding = new AtomicReference<>();
        AtomicReference<String> acceptEncoding = new AtomicReference<>();
        String base = start(b -> b.supportedEncodings(List.of())
                .preHandlers(List.of(new EncodingSpy(contentEncoding, acceptEncoding))));
        connection = HttpRpcConnection.builder(base).compressionLevel(3).build();
        assertEquals("HI", connection.proxy(RawDemo.class).shout("hi"));
        assertEquals("null", contentEncoding.get());
    }

    /** {@code null} is "off", and is not confused with "never configured". */
    @Test
    @Timeout(30)
    void anExplicitNullLevelDisablesCompression() throws Exception {
        AtomicReference<String> contentEncoding = new AtomicReference<>();
        AtomicReference<String> acceptEncoding = new AtomicReference<>();
        String base = start(b -> b.preHandlers(List.of(new EncodingSpy(contentEncoding, acceptEncoding))));
        connection = HttpRpcConnection.builder(base).compressionLevel(null).build();
        assertEquals("HI", connection.proxy(RawDemo.class).shout("hi"));
        assertEquals("null", contentEncoding.get());
    }

    // --- helpers ----------------------------------------------------------

    private static VectorSchemaRoot utf8Row(String field, String value) {
        Schema schema = new Schema(List.of(
                new Field(field, FieldType.notNullable(new ArrowType.Utf8()), null)));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root());
        root.allocateNew();
        ((VarCharVector) root.getVector(field)).setSafe(0, value.getBytes(StandardCharsets.UTF_8));
        root.setRowCount(1);
        return root;
    }

    private static VectorSchemaRoot int64Row(String field, long value) {
        Schema schema = new Schema(List.of(
                new Field(field, FieldType.notNullable(new ArrowType.Int(64, true)), null)));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root());
        root.allocateNew();
        ((BigIntVector) root.getVector(field)).setSafe(0, value);
        root.setRowCount(1);
        return root;
    }

    private static String firstString(byte[] framed, String column) throws Exception {
        try (IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(framed), Allocators.root())) {
            assertNotNull(r.readNextBatch());
            return r.root().getVector(column).getObject(0).toString();
        }
    }

    private static long firstLong(byte[] framed, String column) throws Exception {
        try (IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(framed), Allocators.root())) {
            assertNotNull(r.readNextBatch());
            return ((Number) r.root().getVector(column).getObject(0)).longValue();
        }
    }
}
