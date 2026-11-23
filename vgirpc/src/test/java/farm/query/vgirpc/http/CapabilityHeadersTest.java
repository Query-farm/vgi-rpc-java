// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import com.github.luben.zstd.Zstd;
import farm.query.vgirpc.RpcConnection;
import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.external.UploadUrlProvider;
import farm.query.vgirpc.transport.RpcTransport;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import farm.query.vgirpc.wire.IpcStreamWriter;
import farm.query.vgirpc.wire.Metadata;
import farm.query.vgirpc.wire.Wire;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * {@code VGI-Supported-Encodings} end to end: a live server advertises exactly
 * the codec set it was configured with, and behaves the way it advertises.
 *
 * <p>The empty set is the interesting case. It is the SDK's "never compress"
 * configuration — there is no separate on/off flag — and it is what makes the
 * shared conformance case
 * {@code TestHttpCompressionNegotiationConformance::test_empty_advertisement_means_never_compressed}
 * reachable in Java: a present-but-empty header (distinct from an absent one,
 * which means a legacy server and is read as "assume zstd") plus an
 * uncompressed body however eagerly the client asks.
 */
final class CapabilityHeadersTest {

    public interface EchoService {
        String echo(String value);
    }

    public static final class EchoImpl implements EchoService {
        @Override public String echo(String value) { return value; }
    }

    /** Large and highly compressible, mirroring the conformance suite's probe. */
    private static final String PAYLOAD = "conformance-compression-probe ".repeat(4096);

    /** Leading bytes of an Arrow IPC stream (the continuation marker). */
    private static final byte[] ARROW_MAGIC = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};

    private HttpServer server;
    private String base;

    @AfterEach
    void stop() throws Exception {
        if (server != null) server.stop();
    }

    /** Boot a server whose producible codec set is {@code encodings}
     *  ({@code null} = leave unset, i.e. the default set). */
    private void start(List<String> encodings) throws Exception {
        startWithConfig(HttpServer.Config.builder().prefix("/vgi").supportedEncodings(encodings));
    }

    private void startWithConfig(HttpServer.Config.Builder config) throws Exception {
        server = new HttpServer(new RpcServer(EchoService.class, new EchoImpl()), config.build());
        server.start();
        base = "http://127.0.0.1:" + server.port() + "/vgi";
    }

    // ---- advertisement ---------------------------------------------------

    /** The canonical capability probe carries the configured codec set. */
    @Test
    void options_health_advertises_the_configured_codecs() throws Exception {
        start(null);
        HttpResponse<Void> resp = options(base + "/health");
        assertEquals(200, resp.statusCode());

        String advertised = advertised(resp);
        assertEquals(String.join(", ", HttpServer.Config.defaultSupportedEncodings()), advertised);
        assertFalse(advertised.contains(MediaTypes.IDENTITY));
        assertEquals("true", resp.headers()
                .firstValue(HttpServer.ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER).orElseThrow());
    }

    @Test
    void options_health_rejects_invalid_present_response_budgets_as_arrow_value_errors()
            throws Exception {
        start(null);
        List<HttpRequest> requests = List.of(
                request(base + "/health")
                        .header(HttpServer.ACCEPT_MAX_RESPONSE_BYTES_HEADER, "invalid")
                        .method("OPTIONS", HttpRequest.BodyPublishers.noBody()).build(),
                request(base + "/health")
                        .header(HttpServer.ACCEPT_MAX_RESPONSE_BYTES_HEADER, "65535")
                        .method("OPTIONS", HttpRequest.BodyPublishers.noBody()).build(),
                request(base + "/health")
                        .header(HttpServer.ACCEPT_MAX_RESPONSE_BYTES_HEADER, "65536")
                        .header(HttpServer.ACCEPT_MAX_RESPONSE_BYTES_HEADER, "65537")
                        .method("OPTIONS", HttpRequest.BodyPublishers.noBody()).build());

        try (HttpClient client = newClient()) {
            for (HttpRequest request : requests) {
                HttpResponse<byte[]> resp = client.send(request,
                        HttpResponse.BodyHandlers.ofByteArray());
                assertEquals(400, resp.statusCode());
                assertEquals("true", resp.headers()
                        .firstValue(HttpServer.ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER)
                        .orElseThrow());
                assertEquals("true", resp.headers()
                        .firstValue(HttpServer.RPC_ERROR_HEADER).orElseThrow());
                assertTrue(resp.headers().firstValue("Content-Type").orElseThrow()
                        .startsWith("application/vnd.apache.arrow.stream"));
                try (IpcStreamReader reader = new IpcStreamReader(
                        new java.io.ByteArrayInputStream(resp.body()), Allocators.root())) {
                    RpcError error = Wire.errorFromMetadata(reader.readNextBatch());
                    assertEquals("ValueError", error.errorType());
                }
            }
        }
    }

    @Test
    void hosting_and_application_response_caps_advertise_the_tighter_limit() throws Exception {
        startWithConfig(HttpServer.Config.builder().prefix("/vgi")
                .maxResponseBytes(8L << 20)
                .hostingMaxResponseBytes(2L << 20)
                .preferredResponseBytes(4L << 20));
        HttpResponse<Void> resp = options(base + "/health");
        assertEquals(Long.toString(2L << 20), resp.headers()
                .firstValue(HttpServer.MAX_RESPONSE_BYTES_HEADER).orElseThrow());
    }

    @Test
    void response_budget_configuration_enforces_the_shared_numeric_range() {
        assertThrows(IllegalArgumentException.class, () -> HttpServer.Config.builder()
                .maxResponseBytes((64L << 10) - 1).build());
        assertThrows(IllegalArgumentException.class, () -> HttpServer.Config.builder()
                .hostingMaxResponseBytes((64L << 10) - 1).build());
        assertThrows(IllegalArgumentException.class, () -> HttpServer.Config.builder()
                .preferredResponseBytes((64L << 10) - 1).build());
        assertThrows(IllegalArgumentException.class, () -> HttpServer.Config.builder()
                .advertisedMaxResponseBytes(9_007_199_254_740_992L).build());
        assertThrows(IllegalArgumentException.class, () -> HttpRpcConnection.builder("http://example.invalid")
                .acceptedMaxResponseBytes((64L << 10) - 1));
    }

    /** A narrowed set is advertised verbatim — the same mechanism
     *  {@code VGI_HTTP_DISABLE_ZSTD} presets. */
    @Test
    void options_health_advertises_a_narrowed_set() throws Exception {
        start(List.of(MediaTypes.GZIP));
        assertEquals(MediaTypes.GZIP, advertised(options(base + "/health")));
    }

    /**
     * The empty case: the header must be <em>present and empty</em>, never
     * omitted. Absent means "legacy server, assume zstd" — the opposite claim.
     */
    @Test
    void options_health_advertises_an_empty_set_as_a_present_empty_header() throws Exception {
        start(List.of());
        HttpResponse<Void> resp = options(base + "/health");
        assertEquals(200, resp.statusCode());
        assertTrue(resp.headers().firstValue(HttpServer.SUPPORTED_ENCODINGS_HEADER).isPresent(),
                "an empty codec set must still advertise the header, or it reads as a legacy server");
        assertEquals("", advertised(resp));
    }

    /** Capability headers ride on every response, not just the probe. */
    @Test
    void supported_encodings_is_present_on_health_get() throws Exception {
        start(null);
        try (HttpClient client = newClient()) {
            HttpResponse<String> resp = client.send(
                    HttpRequest.newBuilder(URI.create(base + "/health"))
                            .timeout(Duration.ofSeconds(10)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, resp.statusCode());
            assertTrue(resp.headers().firstValue(HttpServer.SUPPORTED_ENCODINGS_HEADER).isPresent());
        }
    }

    // ---- behaviour matches the advertisement -----------------------------

    /** Control for the empty-set case below: with the default set the very same
     *  request really is answered with a compressed body. */
    @Test
    void advertised_codec_is_actually_used() throws Exception {
        // Configured explicitly, not left unset: VGI_HTTP_DISABLE_ZSTD in the
        // environment would otherwise move the default set out from under this.
        start(HttpServer.Config.DEFAULT_SUPPORTED_ENCODINGS);
        HttpResponse<byte[]> resp = post(zstdFirst());
        assertEquals(200, resp.statusCode());
        assertEquals(MediaTypes.ZSTD,
                resp.headers().firstValue(HttpHeaders.CONTENT_ENCODING).orElseThrow());

        byte[] body = resp.body();
        assertNotEquals(0, body.length);
        assertFalse(startsWith(body, ARROW_MAGIC), "body should be compressed, not raw Arrow IPC");
        byte[] plain = Zstd.decompress(body, (int) Zstd.getFrameContentSize(body));
        assertTrue(startsWith(plain, ARROW_MAGIC), "decompressed body should be an Arrow IPC stream");
    }

    /**
     * A server configured with no producible codecs compresses nothing, however
     * eagerly the client asks — on either accept header.
     */
    @Test
    void empty_set_never_compresses_a_response() throws Exception {
        start(List.of());
        HttpResponse<byte[]> resp = post(zstdFirst());
        assertEquals(200, resp.statusCode());
        assertEquals("", advertised(resp));
        assertTrue(resp.headers().firstValue(HttpHeaders.CONTENT_ENCODING).isEmpty(),
                "no codec is producible, so nothing may be claimed on Content-Encoding");
        assertTrue(resp.headers().firstValue(HttpHeaders.X_VGI_CONTENT_ENCODING).isEmpty(),
                "nor on the custom stamping header");
        assertTrue(startsWith(resp.body(), ARROW_MAGIC), "body must be raw Arrow IPC");
    }

    /** ...and the same server refuses a compressed <em>request</em> body: it
     *  advertises no codecs, so it must not quietly decode one. */
    @Test
    void empty_set_rejects_a_compressed_request_body() throws Exception {
        start(List.of());
        byte[] request = unaryRequest(PAYLOAD);
        HttpResponse<byte[]> resp = post(request(base + "/EchoService/echo")
                .header(HttpHeaders.CONTENT_ENCODING, MediaTypes.ZSTD)
                .POST(HttpRequest.BodyPublishers.ofByteArray(Zstd.compress(request))));

        assertEquals(415, resp.statusCode());
        assertEquals("", advertised(resp), "the 415 must carry the (empty) set so the client can retry");
    }

    /** The narrowed set is a decode gate too: zstd is no longer accepted, gzip is. */
    @Test
    void narrowed_set_rejects_the_dropped_codec_on_requests() throws Exception {
        start(List.of(MediaTypes.GZIP));
        byte[] request = unaryRequest("small");
        HttpResponse<byte[]> zstdResp = post(request(base + "/EchoService/echo")
                .header(HttpHeaders.CONTENT_ENCODING, MediaTypes.ZSTD)
                .POST(HttpRequest.BodyPublishers.ofByteArray(Zstd.compress(request))));
        assertEquals(415, zstdResp.statusCode());
        assertEquals(MediaTypes.GZIP, advertised(zstdResp));

        HttpResponse<byte[]> gzipResp = post(request(base + "/EchoService/echo")
                .header(HttpHeaders.CONTENT_ENCODING, MediaTypes.GZIP)
                .POST(HttpRequest.BodyPublishers.ofByteArray(gzip(request))));
        assertEquals(200, gzipResp.statusCode());
    }

    @Test
    void negotiated_response_overflow_returns_structured_rpc_error() throws Exception {
        startWithConfig(HttpServer.Config.builder()
                .prefix("/vgi")
                .supportedEncodings(List.of())
                .maxResponseBytes(64L << 10));

        HttpResponse<byte[]> resp = post(request(base + "/EchoService/echo")
                .header(HttpServer.ACCEPT_MAX_RESPONSE_BYTES_HEADER, Long.toString(64L << 10))
                .POST(HttpRequest.BodyPublishers.ofByteArray(unaryRequest(PAYLOAD))));

        assertEquals(200, resp.statusCode());
        assertEquals("true", resp.headers().firstValue(HttpServer.RPC_ERROR_HEADER).orElseThrow());
        assertTrue(resp.body().length < (64L << 10), "the structured refusal must fit the negotiated cap");

        try (HttpRpcConnection client = HttpRpcConnection.builder(base).build()) {
            RpcError error = assertThrows(RpcError.class,
                    () -> client.proxy(EchoService.class).echo(PAYLOAD));
            assertEquals("ResponseTooLargeError", error.errorType());
            assertTrue(error.errorMessage().contains("max_response_bytes"));
            assertTrue(error.errorMessage().contains("echo"));
        }
    }

    @Test
    void malformed_or_combined_client_budget_is_http_400_before_dispatch() throws Exception {
        start(List.of());
        byte[] body = unaryRequest("small");
        HttpResponse<byte[]> malformed = post(request(base + "/EchoService/echo")
                .header(HttpServer.ACCEPT_MAX_RESPONSE_BYTES_HEADER, "01")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body)));
        assertEquals(400, malformed.statusCode());

        HttpResponse<byte[]> combined = post(request(base + "/EchoService/echo")
                .header(HttpServer.ACCEPT_MAX_RESPONSE_BYTES_HEADER, "65536, 131072")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body)));
        assertEquals(400, combined.statusCode());
    }

    @Test
    void upload_url_authenticates_before_budget_and_body_parsing() throws Exception {
        startWithConfig(HttpServer.Config.builder().prefix("/vgi")
                .supportedEncodings(List.of())
                .authenticator(request -> { throw new InvalidCredentials("bad token"); })
                .uploadUrlProvider((size, sha256) -> new UploadUrlProvider.UploadUrl(
                        "https://upload.invalid", "https://download.invalid", Instant.now(), Map.of(), "obj")));

        HttpResponse<byte[]> resp = post(request(base + "/__upload_url__/init")
                .header(HttpServer.ACCEPT_MAX_RESPONSE_BYTES_HEADER, "1")
                .POST(HttpRequest.BodyPublishers.ofByteArray(new byte[0])));
        assertEquals(401, resp.statusCode());
        assertEquals("true", resp.headers()
                .firstValue(HttpServer.ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER).orElseThrow());
    }

    @Test
    void upload_url_response_obeys_the_negotiated_budget() throws Exception {
        String longPart = "x".repeat(700);
        startWithConfig(HttpServer.Config.builder().prefix("/vgi")
                .supportedEncodings(List.of())
                .uploadUrlProvider((size, sha256) -> new UploadUrlProvider.UploadUrl(
                        "https://upload.invalid/" + longPart,
                        "https://download.invalid/" + longPart, Instant.now(), Map.of(), "obj")));

        HttpResponse<byte[]> resp = post(request(base + "/__upload_url__/init")
                .header(HttpServer.ACCEPT_MAX_RESPONSE_BYTES_HEADER, Long.toString(64L << 10))
                .POST(HttpRequest.BodyPublishers.ofByteArray(uploadRequest(100))));
        assertEquals(200, resp.statusCode());
        assertEquals("true", resp.headers().firstValue(HttpServer.RPC_ERROR_HEADER).orElseThrow());
        try (IpcStreamReader reader = new IpcStreamReader(
                new java.io.ByteArrayInputStream(resp.body()), Allocators.root())) {
            RpcError error = Wire.errorFromMetadata(reader.readNextBatch());
            assertEquals("ResponseTooLargeError", error.errorType());
            assertTrue(error.errorMessage().contains("__upload_url__"));
            assertTrue(error.errorMessage().contains("max_response_bytes"));
        }
    }

    // ---- upload-URL protocol v2 -------------------------------------------

    private void startUploadServer(Long maxUploadBytes) throws Exception {
        HttpServer.Config.Builder b = HttpServer.Config.builder().prefix("/vgi")
                .supportedEncodings(List.of())
                .uploadUrlProvider((size, sha256) -> new UploadUrlProvider.UploadUrl(
                        "https://upload.invalid/" + size, "https://download.invalid/" + sha256, Instant.now(),
                        Map.of("x-amz-checksum-sha256", "Zm9v"), "obj-" + size));
        if (maxUploadBytes != null) b.maxUploadBytes(maxUploadBytes);
        startWithConfig(b);
    }

    private RpcError refusal(byte[] body) throws Exception {
        HttpResponse<byte[]> resp = post(request(base + "/__upload_url__/init")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body)));
        assertEquals(400, resp.statusCode());
        try (IpcStreamReader reader = new IpcStreamReader(
                new java.io.ByteArrayInputStream(resp.body()), Allocators.root())) {
            RpcError error = Wire.errorFromMetadata(reader.readNextBatch());
            assertEquals("ValueError", error.errorType());
            return error;
        }
    }

    @Test
    void upload_url_support_header_is_version_two() throws Exception {
        startUploadServer(null);
        HttpResponse<Void> resp = options(base + "/health");
        assertEquals(HttpServer.UPLOAD_URL_PROTOCOL_VERSION,
                resp.headers().firstValue(HttpServer.UPLOAD_URL_HEADER).orElseThrow());
        assertEquals("2", HttpServer.UPLOAD_URL_PROTOCOL_VERSION);
    }

    @Test
    void upload_url_v2_round_trip_returns_one_row_per_entry_with_headers() throws Exception {
        startUploadServer(null);
        HttpResponse<byte[]> resp = post(request(base + "/__upload_url__/init")
                .POST(HttpRequest.BodyPublishers.ofByteArray(UploadRequests.v2(3, 42, UploadRequests.SHA))));
        assertEquals(200, resp.statusCode());
        try (IpcStreamReader reader = new IpcStreamReader(
                new java.io.ByteArrayInputStream(resp.body()), Allocators.root())) {
            reader.readNextBatch();
            VectorSchemaRoot root = reader.root();
            assertEquals(HttpServer.UPLOAD_URL_RESPONSE_SCHEMA, root.getSchema());
            assertEquals(3, root.getRowCount());
            assertEquals("https://upload.invalid/42", root.getVector("upload_url").getObject(0).toString());
            assertEquals("https://download.invalid/" + UploadRequests.SHA,
                    root.getVector("download_url").getObject(2).toString());
            assertTrue(root.getVector("upload_headers").getObject(1).toString().contains("x-amz-checksum-sha256"));
        }
    }

    @Test
    void upload_url_retired_count_shape_is_refused() throws Exception {
        startUploadServer(null);
        RpcError error = refusal(UploadRequests.v1Count(1));
        assertTrue(error.errorMessage().contains("retired count shape"), error.errorMessage());
        assertTrue(error.errorMessage().contains("VGI-Upload-URL-Support: 2"), error.errorMessage());
    }

    @Test
    void upload_url_entry_count_bounds_are_refused() throws Exception {
        startUploadServer(null);
        assertTrue(refusal(UploadRequests.v2(0)).errorMessage().contains("must have 1-100 entries"));
        assertTrue(refusal(UploadRequests.v2(101)).errorMessage().contains("must have 1-100 entries"));
    }

    @Test
    void upload_url_bad_size_and_hash_are_refused() throws Exception {
        startUploadServer(null);
        assertTrue(refusal(UploadRequests.v2(1, 0, UploadRequests.SHA)).errorMessage()
                .contains("uploads[0].size must be an integer >= 1, got 0"));
        assertTrue(refusal(UploadRequests.v2(1, 5, "A".repeat(64))).errorMessage().contains("64 lowercase hex"));
        assertTrue(refusal(UploadRequests.v2(1, 5, "a".repeat(63))).errorMessage().contains("64 lowercase hex"));
    }

    @Test
    void upload_url_size_over_max_upload_bytes_is_refused() throws Exception {
        startUploadServer(1000L);
        RpcError error = refusal(UploadRequests.v2(1, 1001, UploadRequests.SHA));
        assertTrue(error.errorMessage().contains("exceeds VGI-Max-Upload-Bytes"), error.errorMessage());
    }

    // ---- helpers ---------------------------------------------------------

    private static HttpClient newClient() {
        return HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    private static HttpRequest.Builder request(String url) {
        return HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(30));
    }

    private static HttpResponse<Void> options(String url) throws Exception {
        try (HttpClient client = newClient()) {
            return client.send(request(url)
                            .method("OPTIONS", HttpRequest.BodyPublishers.noBody()).build(),
                    HttpResponse.BodyHandlers.discarding());
        }
    }

    private static String advertised(HttpResponse<?> resp) {
        return resp.headers().firstValue(HttpServer.SUPPORTED_ENCODINGS_HEADER).orElseThrow();
    }

    /** Ask for zstd on both accept headers — the eager client of the conformance case. */
    private HttpRequest.Builder zstdFirst() throws Exception {
        return request(base + "/EchoService/echo")
                .header(HttpHeaders.ACCEPT_ENCODING, "zstd, gzip")
                .header(HttpHeaders.X_VGI_ACCEPT_ENCODING, "zstd, gzip")
                .POST(HttpRequest.BodyPublishers.ofByteArray(unaryRequest(PAYLOAD)));
    }

    private static HttpResponse<byte[]> post(HttpRequest.Builder builder) throws Exception {
        try (HttpClient client = newClient()) {
            return client.send(builder.header("Content-Type", "application/vnd.apache.arrow.stream").build(),
                    HttpResponse.BodyHandlers.ofByteArray());
        }
    }

    private static boolean startsWith(byte[] body, byte[] prefix) {
        return body.length >= prefix.length
                && Arrays.equals(Arrays.copyOf(body, prefix.length), prefix);
    }

    private static byte[] gzip(byte[] data) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (java.util.zip.GZIPOutputStream gz = new java.util.zip.GZIPOutputStream(out)) {
            gz.write(data);
        }
        return out.toByteArray();
    }

    /**
     * Serialise one real unary {@code echo} request, by running the call over an
     * in-process pipe pair and teeing the client's outbound bytes. The HTTP
     * transport frames a unary call as exactly this IPC stream, so the captured
     * bytes are a valid POST body.
     */
    private static byte[] unaryRequest(String value) throws Exception {
        RpcServer server = new RpcServer(EchoService.class, new EchoImpl());
        PipedOutputStream clientOut = new PipedOutputStream();
        PipedInputStream serverIn = new PipedInputStream(clientOut, 1 << 20);
        PipedOutputStream serverOut = new PipedOutputStream();
        PipedInputStream clientIn = new PipedInputStream(serverOut, 1 << 20);
        ByteArrayOutputStream captured = new ByteArrayOutputStream();

        RpcTransport serverTransport = new InProcessTransport(serverIn, serverOut);
        RpcTransport clientTransport =
                new InProcessTransport(clientIn, new TeeOutputStream(clientOut, captured));

        Thread serverThread = new Thread(() -> server.serve(serverTransport), "capture-rpc-server");
        serverThread.setDaemon(true);
        serverThread.start();
        try (RpcConnection conn = new RpcConnection(clientTransport)) {
            assertEquals(value, conn.proxy(EchoService.class).echo(value));
        } finally {
            clientTransport.close();
            serverThread.join(2000);
            serverTransport.close();
        }
        return captured.toByteArray();
    }

    private static byte[] uploadRequest(int count) throws Exception {
        return UploadRequests.v2(count);
    }

    /** Writes through to the transport while keeping a copy of the request bytes. */
    private record TeeOutputStream(OutputStream target, ByteArrayOutputStream copy) {
        private OutputStream stream() {
            return new OutputStream() {
                @Override public void write(int b) throws IOException {
                    target.write(b);
                    copy.write(b);
                }
                @Override public void write(byte[] b, int off, int len) throws IOException {
                    target.write(b, off, len);
                    copy.write(b, off, len);
                }
                @Override public void flush() throws IOException { target.flush(); }
                @Override public void close() throws IOException { target.close(); }
            };
        }
    }

    private static final class InProcessTransport implements RpcTransport {
        private final InputStream in;
        private final OutputStream out;

        InProcessTransport(InputStream in, OutputStream out) {
            this.in = in;
            this.out = out;
        }

        InProcessTransport(InputStream in, TeeOutputStream tee) {
            this(in, tee.stream());
        }

        @Override public InputStream reader() { return in; }
        @Override public OutputStream writer() { return out; }

        @Override public void close() {
            try { out.flush(); } catch (Exception ignore) { /* best-effort */ }
            try { out.close(); } catch (Exception ignore) { /* best-effort */ }
            try { in.close(); } catch (Exception ignore) { /* best-effort */ }
        }
    }

    // ---- protocol 3.4: fix 1 and the grant columns --------------------------

    @Test
    void upload_cap_defaults_to_the_fetch_cap_and_is_always_advertised() throws Exception {
        startUploadServer(null);
        HttpResponse<Void> resp = options(base + "/health");
        assertEquals(Long.toString(farm.query.vgirpc.external.ExternalLocationConfig.DEFAULT_MAX_FETCH_BYTES),
                resp.headers().firstValue(HttpServer.MAX_UPLOAD_BYTES_HEADER).orElseThrow());
    }

    @Test
    void upload_cap_above_the_default_fetch_cap_raises_the_fetch_cap() throws Exception {
        RpcServer rpc = new RpcServer(EchoService.class, new EchoImpl());
        farm.query.vgirpc.external.ExternalLocationConfig cfg =
                farm.query.vgirpc.external.ExternalLocationConfig.builder().build();
        rpc.setExternalConfig(cfg);
        rpc.setLocationResolver(new farm.query.vgirpc.external.LocationResolver(cfg));
        server = new HttpServer(rpc, HttpServer.Config.builder().prefix("/vgi")
                .uploadUrlProvider((size, sha256) -> new UploadUrlProvider.UploadUrl(
                        "https://u.invalid", "https://d.invalid", Instant.now(), Map.of(), "obj"))
                .maxUploadBytes(4L << 30)
                .build());
        server.start();
        base = "http://127.0.0.1:" + server.port() + "/vgi";
        HttpResponse<Void> resp = options(base + "/health");
        assertEquals(Long.toString(4L << 30),
                resp.headers().firstValue(HttpServer.MAX_UPLOAD_BYTES_HEADER).orElseThrow());
        assertEquals(4L << 30, rpc.externalConfig().maxFetchBytes(), "the default fetch cap was raised");
        assertEquals(4L << 30, rpc.locationResolver().config().maxFetchBytes(), "and the resolver with it");
    }

    @Test
    void upload_cap_above_an_explicit_fetch_cap_refuses_startup() {
        RpcServer rpc = new RpcServer(EchoService.class, new EchoImpl());
        rpc.setExternalConfig(farm.query.vgirpc.external.ExternalLocationConfig.builder()
                .maxFetchBytes(1L << 20).build());
        HttpServer.Config config = HttpServer.Config.builder()
                .uploadUrlProvider((size, sha256) -> new UploadUrlProvider.UploadUrl(
                        "https://u.invalid", "https://d.invalid", Instant.now(), Map.of(), "obj"))
                .maxUploadBytes(2L << 20)
                .build();
        IllegalArgumentException e = org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class, () -> new HttpServer(rpc, config));
        assertTrue(e.getMessage().contains("fetch cap"), e.getMessage());
    }

    @Test
    void grant_rows_carry_the_object_id_and_a_sealed_ref() throws Exception {
        startUploadServer(null);
        HttpResponse<byte[]> resp = post(request(base + "/__upload_url__/init")
                .POST(HttpRequest.BodyPublishers.ofByteArray(UploadRequests.v2(2, 42, UploadRequests.SHA))));
        assertEquals(200, resp.statusCode());
        try (IpcStreamReader reader = new IpcStreamReader(
                new java.io.ByteArrayInputStream(resp.body()), Allocators.root())) {
            reader.readNextBatch();
            VectorSchemaRoot root = reader.root();
            assertEquals("obj-42", root.getVector("object_id").getObject(0).toString());
            byte[] ref = (byte[]) root.getVector("ref").getObject(1);
            assertEquals(0x01, ref[0], "a versioned sealed envelope");
        }
    }

    @Test
    void a_provider_without_an_object_id_is_refused() throws Exception {
        startWithConfig(HttpServer.Config.builder().prefix("/vgi").supportedEncodings(List.of())
                .uploadUrlProvider((size, sha256) -> new UploadUrlProvider.UploadUrl(
                        "https://u.invalid", "https://d.invalid", Instant.now())));
        HttpResponse<byte[]> resp = post(request(base + "/__upload_url__/init")
                .POST(HttpRequest.BodyPublishers.ofByteArray(UploadRequests.v2(1, 42, UploadRequests.SHA))));
        assertEquals(500, resp.statusCode());
    }

    // --- extraCapabilityHeaders ---------------------------------------------------------

    @Test
    void extraCapabilityHeadersRideEveryResponseIncludingDiscovery() throws Exception {
        startWithConfig(HttpServer.Config.builder().prefix("/vgi")
                .corsOrigins(List.of("https://app.example"))
                .extraCapabilityHeaders(Map.of("VGI-Arguments-Ref-Bytes", "4194304")));
        HttpClient client = HttpClient.newHttpClient();
        // Capability discovery: OPTIONS runs no pre-handlers, so a header added there is the test.
        HttpResponse<Void> options = client.send(HttpRequest.newBuilder(URI.create(base + "/health"))
                .method("OPTIONS", HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.discarding());
        assertEquals("4194304", options.headers().firstValue("VGI-Arguments-Ref-Bytes").orElse(null));
        // An ordinary GET, and a request that fails (404), carry it too.
        HttpResponse<Void> health = client.send(HttpRequest.newBuilder(URI.create(base + "/health")).GET().build(),
                HttpResponse.BodyHandlers.discarding());
        assertEquals("4194304", health.headers().firstValue("VGI-Arguments-Ref-Bytes").orElse(null));
        HttpResponse<Void> missing = client.send(HttpRequest.newBuilder(URI.create(base + "/no/such/route"))
                .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.discarding());
        assertEquals("4194304", missing.headers().firstValue("VGI-Arguments-Ref-Bytes").orElse(null));
        // A browser client may read it: it is in the CORS expose set.
        HttpResponse<Void> cors = client.send(HttpRequest.newBuilder(URI.create(base + "/health"))
                .header("Origin", "https://app.example").GET().build(), HttpResponse.BodyHandlers.discarding());
        String exposed = cors.headers().firstValue("Access-Control-Expose-Headers").orElse("");
        assertTrue(exposed.toLowerCase(java.util.Locale.ROOT).contains("vgi-arguments-ref-bytes"), exposed);
    }

    @Test
    void noExtraCapabilityHeadersByDefault() throws Exception {
        start(null);
        HttpResponse<Void> options = HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create(base + "/health"))
                .method("OPTIONS", HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.discarding());
        assertFalse(options.headers().firstValue("VGI-Arguments-Ref-Bytes").isPresent());
    }

    @Test
    void extraCapabilityHeadersAreValidated() {
        assertThrows(IllegalArgumentException.class, () -> HttpServer.Config.builder()
                .extraCapabilityHeaders(Map.of("Bad Name", "1")).build());
        assertThrows(IllegalArgumentException.class, () -> HttpServer.Config.builder()
                .extraCapabilityHeaders(Map.of("X-Ok", "line\r\nbreak")).build());
        // vgi-rpc's own capability headers cannot be overridden (case-insensitively).
        assertThrows(IllegalArgumentException.class, () -> HttpServer.Config.builder()
                .extraCapabilityHeaders(Map.of("vgi-upload-url-support", "9")).build());
        // A built config gains headers through its copy method.
        HttpServer.Config cfg = HttpServer.Config.builder().build()
                .withExtraCapabilityHeaders(Map.of("VGI-Arguments-Ref-Bytes", "1"));
        assertEquals(Map.of("VGI-Arguments-Ref-Bytes", "1"), cfg.extraCapabilityHeaders());
        assertTrue(HttpServer.Config.builder().build().extraCapabilityHeaders().isEmpty());
    }
}
