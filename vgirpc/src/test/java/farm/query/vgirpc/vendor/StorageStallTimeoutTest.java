// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.vendor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import farm.query.vgirpc.external.ExternalFetcher;
import farm.query.vgirpc.external.ExternalLocationConfig;
import farm.query.vgirpc.external.StorageHttp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.net.ServerSocketFactory;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Storage HTTP timeouts are stall (idle) timeouts, never a whole-transfer
 * deadline: a slow transfer that keeps moving completes with exactly one
 * attempt, and a transfer that stops moving fails within the stall timeout.
 *
 * <p>The storage side is a raw socket server that throttles: it reads a PUT
 * body at a fixed rate (so TCP backpressure reaches the client), trickles a GET
 * body, or stops moving bytes on request.</p>
 */
class StorageStallTimeoutTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Duration STALL = Duration.ofSeconds(2);
    private static final long SLACK_MS = 2000;
    private static final int MIB = 1 << 20;

    private final List<AutoCloseable> closers = new ArrayList<>();

    @AfterEach
    void close() throws Exception {
        for (AutoCloseable c : closers) c.close();
    }

    /** One request the throttling server saw. */
    record Attempt(String method, AtomicLong bodyBytes) {}

    /** A throttling HTTP/1.1 storage server on a raw (optionally TLS) socket. */
    static final class SlowServer implements AutoCloseable {
        final ServerSocket socket;
        final List<Attempt> attempts = new CopyOnWriteArrayList<>();
        final List<Socket> open = new CopyOnWriteArrayList<>();
        final long rate;
        final long stallAfter;
        final long getSize;
        final boolean noResponse;
        final boolean tls;

        SlowServer(long rate, long stallAfter, long getSize, boolean noResponse, SSLContext ssl) throws IOException {
            this.rate = rate;
            this.stallAfter = stallAfter;
            this.getSize = getSize;
            this.noResponse = noResponse;
            this.tls = ssl != null;
            ServerSocketFactory f = ssl != null ? ssl.getServerSocketFactory() : ServerSocketFactory.getDefault();
            socket = f.createServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress());
            Thread t = new Thread(this::acceptLoop, "slow-storage");
            t.setDaemon(true);
            t.start();
        }

        String base() { return (tls ? "https" : "http") + "://127.0.0.1:" + socket.getLocalPort(); }

        String hostPort() { return "127.0.0.1:" + socket.getLocalPort(); }

        private void acceptLoop() {
            while (!socket.isClosed()) {
                try {
                    Socket s = socket.accept();
                    open.add(s);
                    Thread t = new Thread(() -> serve(s), "slow-storage-conn");
                    t.setDaemon(true);
                    t.start();
                } catch (IOException e) {
                    return;
                }
            }
        }

        private void serve(Socket s) {
            try (s) {
                s.setReceiveBufferSize(64 * 1024);
                InputStream in = s.getInputStream();
                OutputStream out = s.getOutputStream();
                String head = readHead(in);
                if (head == null) return;
                String method = head.substring(0, head.indexOf(' '));
                long contentLength = 0;
                for (String line : head.split("\r\n")) {
                    int c = line.indexOf(':');
                    if (c > 0 && line.substring(0, c).trim().equalsIgnoreCase("content-length")) {
                        contentLength = Long.parseLong(line.substring(c + 1).trim());
                    }
                }
                Attempt attempt = new Attempt(method, new AtomicLong());
                attempts.add(attempt);
                if (method.equals("GET")) {
                    if (noResponse) { Thread.sleep(600_000); return; }
                    out.write(("HTTP/1.1 200 OK\r\nContent-Length: " + getSize + "\r\nConnection: close\r\n\r\n")
                            .getBytes(StandardCharsets.US_ASCII));
                    byte[] block = new byte[(int) Math.max(1, rate / 20)];
                    long sent = 0;
                    while (sent < getSize) {
                        if (stallAfter >= 0 && sent >= stallAfter) { Thread.sleep(600_000); return; }
                        int n = (int) Math.min(block.length, getSize - sent);
                        out.write(block, 0, n);
                        out.flush();
                        sent += n;
                        attempt.bodyBytes().set(sent);
                        Thread.sleep(n * 1000L / rate);
                    }
                    return;
                }
                byte[] buf = new byte[(int) Math.max(1, Math.min(64 * 1024, rate / 20))];
                long got = 0;
                while (got < contentLength) {
                    if (stallAfter >= 0 && got >= stallAfter) { Thread.sleep(600_000); return; }
                    int n = in.read(buf, 0, (int) Math.min(buf.length, contentLength - got));
                    if (n < 0) return;
                    got += n;
                    attempt.bodyBytes().set(got);
                    Thread.sleep(n * 1000L / rate);
                }
                if (noResponse) { Thread.sleep(600_000); return; }
                out.write("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        .getBytes(StandardCharsets.US_ASCII));
                out.flush();
            } catch (IOException | InterruptedException ignored) {
                // closed by the test or the client
            }
        }

        private static String readHead(InputStream in) throws IOException {
            StringBuilder sb = new StringBuilder();
            int c;
            while ((c = in.read()) >= 0) {
                sb.append((char) c);
                if (sb.length() >= 4 && sb.substring(sb.length() - 4).equals("\r\n\r\n")) return sb.toString();
            }
            return null;
        }

        @Override
        public void close() throws IOException {
            socket.close();
            for (Socket s : open) s.close();
        }
    }

    private SlowServer slow(long rate, long stallAfter, long getSize, boolean noResponse) throws IOException {
        SlowServer s = new SlowServer(rate, stallAfter, getSize, noResponse, null);
        closers.add(s);
        return s;
    }

    /** A minimal blob vendor whose grants point at {@code storage}. */
    private String vendor(SlowServer storage, long objectSize) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/", ex -> vendorApi(ex, storage, objectSize));
        server.start();
        closers.add(() -> server.stop(0));
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private static void vendorApi(HttpExchange ex, SlowServer storage, long objectSize) throws IOException {
        String path = ex.getRequestURI().getPath();
        String expires = Instant.now().plusSeconds(3600).toString();
        String object = storage.base() + "/obj-1?X-Amz-Signature=secret";
        String body;
        if (path.equals("/v1/info")) {
            body = "{\"region\":\"test\",\"upload_hosts\":[\"" + storage.hostPort() + "\"],\"download_hosts\":[\""
                    + storage.hostPort() + "\"]}";
        } else if (path.equals("/v1/blobs/put")) {
            JsonNode req = JSON.readTree(ex.getRequestBody());
            body = "{\"object_id\":\"obj-1\",\"upload_url\":\"" + object + "\",\"required_headers\":{\"Content-Length\":\""
                    + req.get("size").asLong() + "\",\"x-amz-checksum-sha256\":\"" + req.get("sha256_b64").asText()
                    + "\"},\"download_url\":\"" + object + "\",\"expires_at\":\"" + expires + "\"}";
        } else {
            body = "{\"download_url\":\"" + object + "\",\"size\":" + objectSize
                    + ",\"sha256_b64\":\"AAAA\",\"content_encoding\":null,\"expires_at\":\"" + expires + "\"}";
        }
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(200, bytes.length);
        try (OutputStream out = ex.getResponseBody()) { out.write(bytes); }
    }

    private VendorStorage storageClient(String vendorUrl, int attempts) {
        VendorStorage v = new VendorStorage(vendorUrl, "token", "rpc", STALL, STALL, attempts, 0.01, 600, true);
        closers.add(v);
        return v;
    }

    private static long millisSince(long t0) { return (System.nanoTime() - t0) / 1_000_000; }

    private static long putCount(SlowServer s) {
        return s.attempts.stream().filter(a -> a.method().equals("PUT")).count();
    }

    @Test
    void stallTimeoutFromEnvironment() {
        assertEquals("VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS", StorageHttp.ENV_STALL_TIMEOUT);
        assertEquals(Duration.ofSeconds(30), StorageHttp.stallTimeoutFromEnv(Map.of()));
        assertEquals(Duration.ofSeconds(120), StorageHttp.stallTimeoutFromEnv(Map.of(StorageHttp.ENV_STALL_TIMEOUT, "120")));
        assertEquals(Duration.ofMillis(2500), StorageHttp.stallTimeoutFromEnv(Map.of(StorageHttp.ENV_STALL_TIMEOUT, "2.5")));
        for (String bad : List.of("0", "-1", "abc", "1e3x", "Infinity")) {
            assertThrows(IllegalArgumentException.class,
                    () -> StorageHttp.stallTimeoutFromEnv(Map.of(StorageHttp.ENV_STALL_TIMEOUT, bad)), bad);
        }
    }

    @Test
    void slowUploadThatKeepsMovingSucceedsWithOnePut() throws Exception {
        // 24 MiB at 2 MiB/s is ~12 s: six stall windows, and six times the
        // whole-request timeout (2 s) the vendor API calls use.
        SlowServer storage = slow(2L * MIB, -1, 0, false);
        VendorStorage client = storageClient(vendor(storage, 24L * MIB), 3);
        long t0 = System.nanoTime();
        client.upload(new byte[24 * MIB], null);
        assertTrue(millisSince(t0) > 4 * STALL.toMillis(), "upload ran " + millisSince(t0) + " ms");
        assertEquals(1, putCount(storage));
        assertEquals(24L * MIB, storage.attempts.get(0).bodyBytes().get());
    }

    @Test
    void uploadWhoseStorageStopsReadingFailsWithinTheStallTimeoutPerAttempt() throws Exception {
        SlowServer storage = slow(8L * MIB, 256 * 1024, 0, false);
        VendorStorage client = storageClient(vendor(storage, 32L * MIB), 2);
        long t0 = System.nanoTime();
        VendorError e = assertThrows(VendorError.class, () -> client.upload(new byte[32 * MIB], null));
        long ms = millisSince(t0);
        assertTrue(e.getMessage().contains("failed after 2 attempts (stalled)"), e.getMessage());
        assertTrue(!e.getMessage().contains("secret"), e.getMessage());
        assertEquals(2, putCount(storage));
        assertTrue(ms < 2 * STALL.toMillis() + 2 * SLACK_MS, "took " + ms + " ms");
    }

    @Test
    void slowDownloadThatKeepsMovingSucceedsWithOneGet() throws Exception {
        SlowServer storage = slow(512L * 1024, -1, 3L * MIB, false);
        VendorStorage client = storageClient(vendor(storage, 3L * MIB), 3);
        long t0 = System.nanoTime();
        byte[] got = client.get("obj-1", 8L * MIB);
        assertEquals(3 * MIB, got.length);
        assertTrue(millisSince(t0) > 2 * STALL.toMillis(), "download ran " + millisSince(t0) + " ms");
        assertEquals(List.of("GET"), storage.attempts.stream().map(Attempt::method).toList());
    }

    @Test
    void downloadThatStallsMidBodyIsRetriedThenFailsWithinTheStallTimeout() throws Exception {
        SlowServer storage = slow(4L * MIB, MIB, 3L * MIB, false);
        VendorStorage client = storageClient(vendor(storage, 3L * MIB), 2);
        long t0 = System.nanoTime();
        VendorError e = assertThrows(VendorError.class, () -> client.get("obj-1", 8L * MIB));
        long ms = millisSince(t0);
        assertTrue(e.getMessage().contains("(stalled)"), e.getMessage());
        assertEquals(List.of("GET", "GET"), storage.attempts.stream().map(Attempt::method).toList());
        assertTrue(ms < 2 * STALL.toMillis() + 2 * SLACK_MS, "took " + ms + " ms");
    }

    @Test
    void storageThatNeverAnswersFailsWithinTheStallTimeout() throws Exception {
        SlowServer storage = slow(MIB, -1, 10, true);
        VendorStorage client = storageClient(vendor(storage, 10), 1);
        long t0 = System.nanoTime();
        VendorError e = assertThrows(VendorError.class, () -> client.get("obj-1", MIB));
        assertTrue(e.getMessage().contains("(stalled)"), e.getMessage());
        assertTrue(millisSince(t0) < STALL.toMillis() + SLACK_MS, "took " + millisSince(t0) + " ms");
    }

    @Test
    void pointerFetchIsNotCutOffWhileMovingAndFailsWhenStalled() throws Exception {
        SlowServer moving = slow(256L * 1024, -1, 1536L * 1024, false);
        SlowServer stuck = slow(MIB, 64 * 1024, MIB, false);
        ExternalFetcher fetcher = new ExternalFetcher(ExternalLocationConfig.builder()
                .urlValidator(u -> { })
                .maxRetries(0)
                .stallTimeout(STALL)
                .build());
        long t0 = System.nanoTime();
        byte[] body = fetcher.fetch(URI.create(moving.base() + "/x"), null);
        assertEquals(1536 * 1024, body.length);
        assertTrue(millisSince(t0) > 2 * STALL.toMillis(), "fetch ran " + millisSince(t0) + " ms");

        long t1 = System.nanoTime();
        IOException e = assertThrows(IOException.class, () -> fetcher.fetch(URI.create(stuck.base() + "/x"), null));
        assertTrue(e.getMessage().contains("stalled"), e.getMessage());
        assertTrue(millisSince(t1) < STALL.toMillis() + SLACK_MS, "took " + millisSince(t1) + " ms");
    }

    /** Over TLS a single large write can get one deadline for the whole buffer;
     *  the body must go in chunks, each restarting the stall window. */
    @Test
    void slowPutOverTlsKeepsMovingPastManyStallWindows() throws Exception {
        Path dir = Files.createTempDirectory("vgi-stall-tls");
        Path ks = dir.resolve("ks.p12");
        Process p = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "keytool").toString(),
                "-genkeypair", "-keystore", ks.toString(), "-storetype", "PKCS12", "-storepass", "changeit",
                "-alias", "s", "-keyalg", "RSA", "-keysize", "2048", "-dname", "CN=127.0.0.1", "-validity", "1",
                "-ext", "SAN=ip:127.0.0.1").redirectErrorStream(true).start();
        p.getInputStream().readAllBytes();
        assertEquals(0, p.waitFor());
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(ks)) { store.load(in, "changeit".toCharArray()); }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(store, "changeit".toCharArray());
        SSLContext serverCtx = SSLContext.getInstance("TLS");
        serverCtx.init(kmf.getKeyManagers(), null, null);
        SSLContext trustAll = SSLContext.getInstance("TLS");
        trustAll.init(null, new TrustManager[] {new X509TrustManager() {
            @Override public void checkClientTrusted(X509Certificate[] c, String a) { }
            @Override public void checkServerTrusted(X509Certificate[] c, String a) { }
            @Override public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
        }}, null);

        SlowServer storage = new SlowServer(2L * MIB, -1, 0, false, serverCtx);
        closers.add(storage);
        HttpClient client = StorageHttp.newClientBuilder().sslContext(trustAll).build();
        closers.add(client::shutdownNow);
        long t0 = System.nanoTime();
        StorageHttp.Result r = StorageHttp.exchange(client, HttpRequest.newBuilder(URI.create(storage.base() + "/x")),
                "PUT", new byte[16 * MIB], 1024, STALL);
        assertEquals(200, r.status());
        assertTrue(millisSince(t0) > 3 * STALL.toMillis(), "PUT ran " + millisSince(t0) + " ms");
        assertEquals(1, putCount(storage));
        assertEquals(16L * MIB, storage.attempts.get(0).bodyBytes().get());
    }

    @Test
    void stallSurfacesAsAStallTimeoutException() throws Exception {
        SlowServer storage = slow(MIB, -1, 10, true);
        HttpClient client = StorageHttp.newClientBuilder().build();
        closers.add(client::shutdownNow);
        IOException e = assertThrows(IOException.class, () -> StorageHttp.exchange(client,
                HttpRequest.newBuilder(URI.create(storage.base() + "/x")), "GET", null, 1024, Duration.ofSeconds(1)));
        assertInstanceOf(StorageHttp.StallTimeoutException.class, e);
    }
}
