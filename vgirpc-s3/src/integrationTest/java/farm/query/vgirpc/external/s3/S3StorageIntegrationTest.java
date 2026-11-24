// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.external.s3;

import com.github.luben.zstd.Zstd;
import farm.query.vgirpc.external.ExternalFetcher;
import farm.query.vgirpc.external.ExternalLocationConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.containers.wait.strategy.Wait;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real S3-protocol integration tests. RustFS is deliberately used instead of a
 * request stub: the rejected requests below prove that the URL is actually
 * bound to its SigV4 signature and HTTP method.
 */
@Testcontainers
final class S3StorageIntegrationTest {

    // Multi-architecture manifest for the 1.0.0-beta.12-glibc release.
    private static final String IMAGE = "ghcr.io/rustfs/rustfs"
            + "@sha256:29c02251c085cb04edce556304a9ec0f8fba0c40300266cf4f3d953783fe2450";
    private static final String ACCESS_KEY = "vgi-integration-access";
    private static final String SECRET_KEY = "vgi-integration-secret-key";
    private static final String BUCKET = "vgi-rpc-integration";
    private static final String PREFIX = "java-integration/";
    private static final int S3_PORT = 9000;

    @Container
    private static final GenericContainer<?> RUSTFS = new GenericContainer<>(DockerImageName.parse(IMAGE))
            .withExposedPorts(S3_PORT)
            .withEnv("RUSTFS_ACCESS_KEY", ACCESS_KEY)
            .withEnv("RUSTFS_SECRET_KEY", SECRET_KEY)
            .withEnv("RUSTFS_CONSOLE_ENABLE", "false")
            .withEnv("RUSTFS_OBS_LOGGER_LEVEL", "error")
            .waitingFor(Wait.forHttp("/health").forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(2));

    private static final StaticCredentialsProvider CREDENTIALS =
            StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY));
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();
    private static URI endpoint;

    @BeforeAll
    static void createBucket() {
        endpoint = URI.create("http://" + RUSTFS.getHost() + ":" + RUSTFS.getMappedPort(S3_PORT));
        try (S3Client client = S3Client.builder()
                .endpointOverride(endpoint)
                .forcePathStyle(true)
                .credentialsProvider(CREDENTIALS)
                .region(Region.US_EAST_1)
                .build()) {
            client.createBucket(request -> request.bucket(BUCKET));
        }
    }

    @Test
    void uploadProducesCredentialFreeMethodBoundSigV4GetAndRecoversAfterRejections() throws Exception {
        byte[] expected = "real RustFS payload: \u0000\u0001\u0002"
                .getBytes(StandardCharsets.UTF_8);

        try (S3Storage storage = storage()) {
            URI signedGet = storage.upload(expected, null);
            assertSignedObjectPath(signedGet);

            HttpResponse<byte[]> fetched = get(signedGet);
            assertEquals(200, fetched.statusCode());
            assertArrayEquals(expected, fetched.body());
            assertEquals("application/vnd.apache.arrow.stream",
                    fetched.headers().firstValue("Content-Type").orElseThrow());
            assertEquals(Integer.toString(expected.length),
                    fetched.headers().firstValue("Content-Length").orElseThrow());
            assertTrue(fetched.headers().firstValue("Content-Encoding").isEmpty());

            URI mutated = mutateSignature(signedGet);
            assertEquals(403, get(mutated).statusCode(),
                    "RustFS must reject a mutated SigV4 signature");

            HttpResponse<byte[]> wrongMethod = HTTP.send(
                    HttpRequest.newBuilder(signedGet)
                            .timeout(Duration.ofSeconds(10))
                            .PUT(HttpRequest.BodyPublishers.noBody())
                            .build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(403, wrongMethod.statusCode(),
                    "a presigned GET must not authorize PUT");

            byte[] recoveryBody = "storage remains usable".getBytes(StandardCharsets.UTF_8);
            URI recoveryGet = storage.upload(recoveryBody, null);
            HttpResponse<byte[]> recovered = get(recoveryGet);
            assertEquals(200, recovered.statusCode());
            assertArrayEquals(recoveryBody, recovered.body());
        }
    }

    @Test
    void zstdObjectRetainsEncodingAndExternalFetcherReturnsDecodedBytes() throws Exception {
        byte[] decoded = "Arrow-compatible bytes should survive external zstd.\n"
                .repeat(256).getBytes(StandardCharsets.UTF_8);
        byte[] encoded = Zstd.compress(decoded, 3);

        try (S3Storage storage = storage()) {
            URI signedGet = storage.upload(encoded, "zstd");

            HttpResponse<byte[]> raw = get(signedGet);
            assertEquals(200, raw.statusCode());
            assertArrayEquals(encoded, raw.body());
            assertEquals("zstd", raw.headers().firstValue("Content-Encoding").orElseThrow());

            ExternalFetcher fetcher = new ExternalFetcher(ExternalLocationConfig.builder()
                    .urlValidator(ExternalLocationConfig.permissiveValidator())
                    .maxRetries(0)
                    .maxFetchBytes(1L << 20)
                    .maxDecompressedBytes(1L << 20)
                    .build());
            assertArrayEquals(decoded, fetcher.fetch(signedGet, sha256Hex(decoded)));
        }
    }

    /**
     * A TCP proxy to the S3 endpoint forwarding client bytes at {@code rate}
     * bytes/s (all of them, or only the first {@code stallAfter}, then nothing);
     * server bytes pass freely. Counts connections.
     */
    private static final class ThrottlingProxy implements AutoCloseable {
        final java.net.ServerSocket server;
        final java.util.concurrent.atomic.AtomicInteger connections = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.List<java.net.Socket> open = new java.util.concurrent.CopyOnWriteArrayList<>();

        ThrottlingProxy(URI target, long rate, long stallAfter) throws java.io.IOException {
            server = new java.net.ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress());
            Thread accept = new Thread(() -> {
                while (!server.isClosed()) {
                    try {
                        java.net.Socket client = server.accept();
                        connections.incrementAndGet();
                        java.net.Socket upstream = new java.net.Socket(target.getHost(), target.getPort());
                        client.setReceiveBufferSize(64 * 1024);
                        open.add(client);
                        open.add(upstream);
                        pump(upstream, client, 0, -1);
                        pump(client, upstream, rate, stallAfter);
                    } catch (java.io.IOException e) {
                        return;
                    }
                }
            }, "throttling-proxy");
            accept.setDaemon(true);
            accept.start();
        }

        private static void pump(java.net.Socket from, java.net.Socket to, long rate, long stallAfter) {
            Thread t = new Thread(() -> {
                byte[] buf = new byte[16 * 1024];
                long total = 0;
                try {
                    java.io.InputStream in = from.getInputStream();
                    java.io.OutputStream out = to.getOutputStream();
                    int n;
                    while ((n = in.read(buf)) >= 0) {
                        if (stallAfter >= 0 && total >= stallAfter) {
                            Thread.sleep(600_000);
                            return;
                        }
                        out.write(buf, 0, n);
                        out.flush();
                        total += n;
                        if (rate > 0) Thread.sleep(n * 1000L / rate);
                    }
                } catch (java.io.IOException | InterruptedException ignored) {
                    // closed
                } finally {
                    try { from.close(); } catch (java.io.IOException ignored) { }
                    try { to.close(); } catch (java.io.IOException ignored) { }
                }
            }, "throttling-proxy-pump");
            t.setDaemon(true);
            t.start();
        }

        URI uri() { return URI.create("http://127.0.0.1:" + server.getLocalPort()); }

        @Override
        public void close() throws java.io.IOException {
            server.close();
            for (java.net.Socket s : open) s.close();
        }
    }

    private static S3Storage throttledStorage(URI via, Duration stall, long minUploadRate) {
        return S3Storage.builder(BUCKET)
                .endpointOverride(via)
                .forcePathStyle(true)
                .credentials(CREDENTIALS)
                .region(Region.US_EAST_1)
                .keyPrefix(PREFIX)
                .connectTimeout(Duration.ofSeconds(1))
                .stallTimeout(stall)
                .minUploadRateBytesPerSecond(minUploadRate)
                .build();
    }

    @Test
    void slowPutObjectThatKeepsMovingSucceedsOnItsFirstAttempt() throws Exception {
        // 24 MiB at 2 MiB/s is ~12 s against a 3 s stall timeout.
        byte[] data = new byte[24 << 20];
        try (ThrottlingProxy proxy = new ThrottlingProxy(endpoint, 2L << 20, -1);
             S3Storage slow = throttledStorage(proxy.uri(), Duration.ofSeconds(3), 64 * 1024);
             S3Storage direct = storage()) {
            long t0 = System.nanoTime();
            String key = slow.putObject(data);
            long ms = (System.nanoTime() - t0) / 1_000_000;
            assertTrue(ms > 9_000, "putObject ran " + ms + " ms");
            assertEquals(1, proxy.connections.get());
            assertEquals(data.length, direct.get(key, 32L << 20).length);
        }
    }

    @Test
    void putObjectWhoseServerStopsReadingFailsWithinItsScaledDeadline() throws Exception {
        // 32 MiB at a 16 MiB/s floor: each attempt's deadline is connect (1 s)
        // + stall (3 s) + 2 s, where it would otherwise block forever.
        try (ThrottlingProxy proxy = new ThrottlingProxy(endpoint, 0, 512 * 1024);
             S3Storage stuck = throttledStorage(proxy.uri(), Duration.ofSeconds(3), 16L << 20)) {
            long t0 = System.nanoTime();
            org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
                    () -> stuck.putObject(new byte[32 << 20]));
            long ms = (System.nanoTime() - t0) / 1_000_000;
            // The SDK's standard retry policy makes up to 3 attempts, with backoff.
            assertTrue(ms < 3 * 6_000 + 12_000, "took " + ms + " ms");
        }
    }

    private static S3Storage storage() {
        return S3Storage.builder(BUCKET)
                .endpointOverride(endpoint)
                .forcePathStyle(true)
                .credentials(CREDENTIALS)
                .region(Region.US_EAST_1)
                .keyPrefix(PREFIX)
                .presignDuration(Duration.ofMinutes(5))
                .build();
    }

    private static HttpResponse<byte[]> get(URI uri) throws Exception {
        return HTTP.send(
                HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(10)).GET().build(),
                HttpResponse.BodyHandlers.ofByteArray());
    }

    @Test
    void uploadUrlAcceptsOnlyTheDeclaredSizeAndDigest() throws Exception {
        try (S3Storage storage = storage()) {
            byte[] body = new byte[100_000];
            new java.util.Random(7).nextBytes(body);
            String sha = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body));

            var corrupt = storage.generateUploadUrl(body.length, sha);
            byte[] bad = body.clone();
            bad[0] ^= (byte) 0xff;
            assertTrue(put(corrupt, bad) >= 400, "a corrupted body of the declared length must be refused");

            var truncated = storage.generateUploadUrl(body.length, sha);
            assertTrue(put(truncated, java.util.Arrays.copyOf(body, body.length - 1)) >= 400,
                    "a body of another length must be refused");

            var good = storage.generateUploadUrl(body.length, sha);
            assertTrue(good.objectId().startsWith(PREFIX), good.objectId());
            assertTrue(good.headers().containsKey("x-amz-checksum-sha256"), good.headers().toString());
            assertTrue(URI.create(good.uploadUrl()).getRawQuery().toLowerCase(Locale.ROOT).contains("content-length"),
                    "the PUT must sign Content-Length");
            assertEquals(200, put(good, body));
            assertArrayEquals(body, storage.get(good.objectId(), Long.MAX_VALUE));
            HttpResponse<byte[]> got = HTTP.send(HttpRequest.newBuilder(URI.create(good.downloadUrl())).GET().build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(200, got.statusCode());
            assertArrayEquals(body, got.body());
        }
    }

    private static int put(farm.query.vgirpc.external.UploadUrlProvider.UploadUrl url, byte[] body) throws Exception {
        HttpRequest.Builder req = HttpRequest.newBuilder(URI.create(url.uploadUrl()))
                .PUT(HttpRequest.BodyPublishers.ofByteArray(body));
        url.headers().forEach(req::header);
        return HTTP.send(req.build(), HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    private static void assertSignedObjectPath(URI uri) {
        assertEquals(endpoint.getScheme(), uri.getScheme());
        assertEquals(endpoint.getHost(), uri.getHost());
        assertEquals(endpoint.getPort(), uri.getPort());
        assertTrue(uri.getPath().startsWith("/" + BUCKET + "/" + PREFIX), uri::toString);
        assertTrue(uri.getPath().endsWith(".arrow"), uri::toString);
        String query = uri.getRawQuery().toLowerCase(Locale.ROOT);
        assertTrue(query.contains("x-amz-algorithm=aws4-hmac-sha256"), query);
        assertTrue(query.contains("x-amz-expires=300"), query);
        assertTrue(query.contains("x-amz-signature="), query);
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static URI mutateSignature(URI uri) {
        String mutated = uri.toString().replaceFirst(
                "(?i)(X-Amz-Signature=)[0-9a-f]+", "$1" + "0".repeat(64));
        assertNotEquals(uri.toString(), mutated, "test URL did not contain a SigV4 signature");
        return URI.create(mutated);
    }
}
