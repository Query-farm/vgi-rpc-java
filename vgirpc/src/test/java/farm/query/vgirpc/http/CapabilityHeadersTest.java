// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import com.github.luben.zstd.Zstd;
import farm.query.vgirpc.RpcConnection;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.transport.RpcTransport;
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
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        server = new HttpServer(new RpcServer(EchoService.class, new EchoImpl()),
                HttpServer.Config.builder().prefix("/vgi").supportedEncodings(encodings).build());
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
        HttpResponse<byte[]> resp = post(request(base + "/echo")
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
        HttpResponse<byte[]> zstdResp = post(request(base + "/echo")
                .header(HttpHeaders.CONTENT_ENCODING, MediaTypes.ZSTD)
                .POST(HttpRequest.BodyPublishers.ofByteArray(Zstd.compress(request))));
        assertEquals(415, zstdResp.statusCode());
        assertEquals(MediaTypes.GZIP, advertised(zstdResp));

        HttpResponse<byte[]> gzipResp = post(request(base + "/echo")
                .header(HttpHeaders.CONTENT_ENCODING, MediaTypes.GZIP)
                .POST(HttpRequest.BodyPublishers.ofByteArray(gzip(request))));
        assertEquals(200, gzipResp.statusCode());
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
        return request(base + "/echo")
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
}
