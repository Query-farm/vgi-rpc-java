// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.luben.zstd.Zstd;
import farm.query.vgirpc.AccessLogHook;
import farm.query.vgirpc.RpcConnection;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.external.ExternalLocationConfig;
import farm.query.vgirpc.external.ExternalStorage;
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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * §4.8: what crossed the wire, measured where it can actually be measured.
 *
 * <p>{@code response_bytes} cannot be read where the record is written — response
 * compression runs after the handler returns — so this drives a real HTTP server
 * and compares the number in the log against the bytes the client received.
 * A record emitted at dispatch time would report the uncompressed size, which
 * for a compressible Arrow batch is wrong by roughly three orders of magnitude.
 */
final class AccessLogEgressTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    /** Large and highly compressible, so a conflated figure cannot pass by accident. */
    private static final String PAYLOAD = "vgi-rpc-egress-accounting-probe ".repeat(8192);

    public interface EchoService {
        String echo(String value);
    }

    public static final class EchoImpl implements EchoService {
        @Override public String echo(String value) { return value; }
    }

    /** Swallows uploads and hands back a URL nobody fetches: only the size matters here. */
    private static final class CountingStorage implements ExternalStorage {
        private final java.util.concurrent.atomic.AtomicLong uploaded =
                new java.util.concurrent.atomic.AtomicLong();

        @Override public URI upload(byte[] body, String contentEncoding) {
            uploaded.addAndGet(body.length);
            return URI.create("https://storage.invalid/obj/" + uploaded.get());
        }
    }

    private HttpServer server;
    private final ByteArrayOutputStream accessLog = new ByteArrayOutputStream();

    @AfterEach
    void stop() throws Exception {
        if (server != null) server.stop();
    }

    @Test
    void response_bytes_is_the_compressed_size_not_the_arrow_size() throws Exception {
        RpcServer rpc = new RpcServer(EchoService.class, new EchoImpl());
        rpc.setDispatchHook(AccessLogHook.builder(accessLog).serverVersion("egress-test").build());
        server = new HttpServer(rpc, HttpServer.Config.builder()
                .prefix("/vgi")
                .supportedEncodings(List.of(MediaTypes.ZSTD, MediaTypes.GZIP))
                .build());
        server.start();

        byte[] request = unaryRequest(PAYLOAD);
        HttpResponse<byte[]> resp;
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()) {
            resp = client.send(HttpRequest.newBuilder(
                            URI.create("http://127.0.0.1:" + server.port() + "/vgi/echo"))
                    .timeout(Duration.ofSeconds(30))
                    .header("Content-Type", "application/vnd.apache.arrow.stream")
                    .header(HttpHeaders.ACCEPT_ENCODING, "zstd, gzip")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(request))
                    .build(), HttpResponse.BodyHandlers.ofByteArray());
        }
        assertEquals(200, resp.statusCode());
        assertEquals(MediaTypes.ZSTD, resp.headers().firstValue(HttpHeaders.CONTENT_ENCODING).orElseThrow());

        JsonNode rec = onlyRecord();
        assertEquals(resp.body().length, rec.get("response_bytes").asLong(),
                "response_bytes must be what the client received, i.e. post-compression");
        assertEquals(request.length, rec.get("request_bytes").asLong(),
                "request_bytes is the body as received, before any decoding");
        assertEquals(200, rec.get("http_status").asInt());
        assertEquals(resp.headers().firstValue(HttpHeaders.REQUEST_ID).orElseThrow(),
                rec.get("request_id").asText());

        // The point of the field: an emitter that ran before compression would
        // have reported this figure instead, and it is larger by two orders of
        // magnitude on a payload that compresses at all.
        byte[] uncompressed = Zstd.decompress(resp.body(), (int) Zstd.getFrameContentSize(resp.body()));
        assertTrue(uncompressed.length > rec.get("response_bytes").asLong() * 100,
                "the probe is meant to compress hard; uncompressed=" + uncompressed.length
                        + " response_bytes=" + rec.get("response_bytes").asLong());
    }

    /** An uncompressed exchange still reports the true on-wire figures. */
    @Test
    void identity_encoding_reports_the_raw_body_sizes() throws Exception {
        RpcServer rpc = new RpcServer(EchoService.class, new EchoImpl());
        rpc.setDispatchHook(AccessLogHook.builder(accessLog).build());
        server = new HttpServer(rpc, HttpServer.Config.builder()
                .prefix("/vgi").supportedEncodings(List.of()).build());
        server.start();

        byte[] request = unaryRequest("small");
        HttpResponse<byte[]> resp;
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()) {
            resp = client.send(HttpRequest.newBuilder(
                            URI.create("http://127.0.0.1:" + server.port() + "/vgi/echo"))
                    .timeout(Duration.ofSeconds(30))
                    .header("Content-Type", "application/vnd.apache.arrow.stream")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(request))
                    .build(), HttpResponse.BodyHandlers.ofByteArray());
        }
        assertEquals(200, resp.statusCode());

        JsonNode rec = onlyRecord();
        assertEquals(request.length, rec.get("request_bytes").asLong());
        assertEquals(resp.body().length, rec.get("response_bytes").asLong());
    }

    /**
     * The figure nothing at the transport can see: an externalised payload leaves
     * a pointer batch of a few hundred bytes in the body while the data itself
     * goes to object storage. Without this field it is invisible.
     */
    @Test
    void externalized_bytes_counts_what_never_touched_the_body() throws Exception {
        CountingStorage storage = new CountingStorage();
        RpcServer rpc = new RpcServer(EchoService.class, new EchoImpl());
        rpc.setDispatchHook(AccessLogHook.builder(accessLog).build());
        rpc.setExternalConfig(ExternalLocationConfig.builder()
                .storage(storage)
                .thresholdBytes(1024)
                .urlValidator(ExternalLocationConfig.permissiveValidator())
                .build());
        server = new HttpServer(rpc, HttpServer.Config.builder()
                .prefix("/vgi").supportedEncodings(List.of()).build());
        server.start();

        byte[] request = unaryRequest(PAYLOAD);
        HttpResponse<byte[]> resp;
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()) {
            resp = client.send(HttpRequest.newBuilder(
                            URI.create("http://127.0.0.1:" + server.port() + "/vgi/echo"))
                    .timeout(Duration.ofSeconds(30))
                    .header("Content-Type", "application/vnd.apache.arrow.stream")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(request))
                    .build(), HttpResponse.BodyHandlers.ofByteArray());
        }
        assertEquals(200, resp.statusCode());
        assertTrue(storage.uploaded.get() > 0, "the probe should have externalised");

        JsonNode rec = onlyRecord();
        assertEquals(storage.uploaded.get(), rec.get("externalized_bytes").asLong());
        assertTrue(rec.get("response_bytes").asLong() < rec.get("externalized_bytes").asLong(),
                "the body should hold only a pointer batch");
    }

    // ---- helpers ---------------------------------------------------------

    private JsonNode onlyRecord() throws IOException {
        String[] lines = accessLog.toString().split("\n");
        assertEquals(1, lines.length, "expected exactly one access-log record");
        return JSON.readTree(lines[0]);
    }

    /**
     * Serialise one real unary {@code echo} request by running the call over an
     * in-process pipe pair and teeing the client's outbound bytes. The HTTP
     * transport frames a unary call as exactly this IPC stream, so the captured
     * bytes are a valid POST body.
     */
    private static byte[] unaryRequest(String value) throws Exception {
        RpcServer server = new RpcServer(EchoService.class, new EchoImpl());
        PipedOutputStream clientOut = new PipedOutputStream();
        PipedInputStream serverIn = new PipedInputStream(clientOut, 1 << 22);
        PipedOutputStream serverOut = new PipedOutputStream();
        PipedInputStream clientIn = new PipedInputStream(serverOut, 1 << 22);
        ByteArrayOutputStream captured = new ByteArrayOutputStream();

        RpcTransport serverTransport = new InProcessTransport(serverIn, serverOut);
        RpcTransport clientTransport = new InProcessTransport(clientIn, tee(clientOut, captured));

        Thread serverThread = new Thread(() -> server.serve(serverTransport), "egress-capture-server");
        serverThread.setDaemon(true);
        serverThread.start();
        try (RpcConnection conn = new RpcConnection(clientTransport)) {
            assertEquals(value, conn.proxy(EchoService.class).echo(value));
        } finally {
            clientTransport.close();
            serverThread.join(5000);
            serverTransport.close();
        }
        return captured.toByteArray();
    }

    /** Writes through to the transport while keeping a copy of the request bytes. */
    private static OutputStream tee(OutputStream target, ByteArrayOutputStream copy) {
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

    private record InProcessTransport(InputStream in, OutputStream out) implements RpcTransport {
        @Override public InputStream reader() { return in; }
        @Override public OutputStream writer() { return out; }
        @Override public void close() {
            try {
                out.close();
                in.close();
            } catch (IOException ignored) {
                // Test pipes; nothing to recover.
            }
        }
    }
}
