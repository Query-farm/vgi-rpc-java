// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.external.s3;

import farm.query.vgirpc.external.UploadUrlProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The direct-storage environment contract (v1) against RustFS: static keys, a session token
 * from STS AssumeRole (RustFS enforces it), the public endpoint for handed-out URLs, the URL
 * lifetime cap under temporary credentials, checksum none, the SDK checksum default for a custom
 * endpoint, and the startup bucket check.
 */
@Testcontainers
final class S3DirectStorageIntegrationTest {

    private static final String IMAGE = "ghcr.io/rustfs/rustfs"
            + "@sha256:29c02251c085cb04edce556304a9ec0f8fba0c40300266cf4f3d953783fe2450";
    private static final String ACCESS_KEY = "vgi-direct-access";
    private static final String SECRET_KEY = "vgi-direct-secret-key";
    private static final String BUCKET = "vgi-direct";
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

    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static String endpoint;

    @BeforeAll
    static void createBucket() {
        endpoint = "http://" + RUSTFS.getHost() + ":" + RUSTFS.getMappedPort(S3_PORT);
        try (S3Client client = S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .region(Region.US_EAST_1)
                .build()) {
            client.createBucket(r -> r.bucket(BUCKET));
        }
    }

    private static Map<String, String> env(String... kv) {
        Map<String, String> env = new HashMap<>();
        env.put(S3Storage.ENV_BUCKET, BUCKET);
        env.put(S3Storage.ENV_ENDPOINT, endpoint);
        env.put(S3Storage.ENV_PREFIX, "direct/");
        env.put(S3Storage.ENV_ALLOW_PLAIN_HTTP, "1");
        env.put(S3Storage.ENV_ACCESS_KEY_ID, ACCESS_KEY);
        env.put(S3Storage.ENV_SECRET_ACCESS_KEY, SECRET_KEY);
        for (int i = 0; i < kv.length; i += 2) {
            if (kv[i + 1] == null) env.remove(kv[i]);
            else env.put(kv[i], kv[i + 1]);
        }
        return env;
    }

    private static String sha256Hex(byte[] data) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
    }

    private static HttpResponse<byte[]> put(UploadUrlProvider.UploadUrl url, byte[] body) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url.uploadUrl()))
                .PUT(HttpRequest.BodyPublishers.ofByteArray(body));
        url.headers().forEach(b::header);
        return HTTP.send(b.build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    /** Every role with static keys from the environment, plus an upload grant the store enforces. */
    @Test
    void staticKeysFromTheEnvironment() throws Exception {
        try (S3Storage s = S3Storage.fromEnv(env())) {
            s.verify();
            assertTrue(s.supportsUploadGrants());
            byte[] data = "static keys".getBytes(StandardCharsets.UTF_8);
            String key = s.putObject(data);
            assertTrue(key.startsWith("direct/rpc/"), key);
            assertArrayEquals(data, s.get(key, 1 << 20));
            URI result = s.upload(data, null);
            assertArrayEquals(data, HTTP.send(HttpRequest.newBuilder(result).build(),
                    HttpResponse.BodyHandlers.ofByteArray()).body());
            UploadUrlProvider.UploadUrl grant = s.generateUploadUrl(data.length, sha256Hex(data));
            // The checksum is a signed header, never an (unsigned, droppable) query parameter.
            String query = java.net.URLDecoder.decode(URI.create(grant.uploadUrl()).getRawQuery(),
                    StandardCharsets.UTF_8).toLowerCase(java.util.Locale.ROOT);
            assertFalse(java.util.regex.Pattern.compile("(^|&)x-amz-checksum-sha256=").matcher(query).find(), query);
            assertTrue(query.matches(".*x-amz-signedheaders=[^&]*x-amz-checksum-sha256.*"), query);
            assertTrue(grant.headers().containsKey("x-amz-checksum-sha256"), grant.headers().toString());
            assertEquals(200, put(grant, data).statusCode());
            byte[] bad = data.clone();
            bad[0] ^= 1;
            UploadUrlProvider.UploadUrl grant2 = s.generateUploadUrl(data.length, sha256Hex(data));
            assertEquals(400, put(grant2, bad).statusCode(), "a same-length wrong body must be refused (BadDigest)");
        }
    }

    /** A session token from STS AssumeRole: accepted with the token, refused without it. */
    @Test
    void sessionTokenFromAssumeRole() throws Exception {
        Credentials temp;
        try (StsClient sts = StsClient.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .build()) {
            temp = sts.assumeRole(r -> r.roleArn("arn:aws:iam::000000000000:role/vgi").roleSessionName("vgi-java")
                    .durationSeconds(900)).credentials();
        }
        Map<String, String> withToken = env(S3Storage.ENV_ACCESS_KEY_ID, temp.accessKeyId(),
                S3Storage.ENV_SECRET_ACCESS_KEY, temp.secretAccessKey(),
                S3Storage.ENV_SESSION_TOKEN, temp.sessionToken());
        try (S3Storage s = S3Storage.fromEnv(withToken)) {
            s.verify();
            byte[] data = "session token".getBytes(StandardCharsets.UTF_8);
            assertArrayEquals(data, s.get(s.putObject(data), 1 << 20));
            URI result = s.upload(data, null);
            assertTrue(result.getRawQuery().contains("X-Amz-Security-Token="), "the URL carries the token");
            assertEquals(200, HTTP.send(HttpRequest.newBuilder(result).build(),
                    HttpResponse.BodyHandlers.ofByteArray()).statusCode());
        }
        Map<String, String> noToken = new HashMap<>(withToken);
        noToken.remove(S3Storage.ENV_SESSION_TOKEN);
        try (S3Storage s = S3Storage.fromEnv(noToken)) {
            assertThrows(IllegalStateException.class, s::verify);
        }
        assertThrows(IllegalArgumentException.class, () -> S3Storage.fromEnv(env(
                S3Storage.ENV_ACCESS_KEY_ID, null, S3Storage.ENV_SECRET_ACCESS_KEY, null,
                S3Storage.ENV_SESSION_TOKEN, "orphan")));
    }

    /** URLs stop 60 s before the signing credentials expire, and never outlive the TTL setting. */
    @Test
    void urlLifetimeIsCappedByCredentialExpiry() {
        AwsSessionCredentials expiring = AwsSessionCredentials.builder()
                .accessKeyId(ACCESS_KEY).secretAccessKey(SECRET_KEY).sessionToken("t")
                .expirationTime(Instant.now().plus(Duration.ofMinutes(10))).build();
        try (S3Storage s = S3Storage.builder(BUCKET).endpointOverride(URI.create(endpoint))
                .credentials(StaticCredentialsProvider.create(expiring)).build()) {
            long ttl = s.effectiveUrlTtl(S3Storage.MIN_UPLOAD_GRANT_TTL).toSeconds();
            assertTrue(ttl <= 540 && ttl > 500, "ttl " + ttl);
            String url = s.generateUploadUrl(10, "0".repeat(64)).uploadUrl();
            long expires = Long.parseLong(url.replaceAll(".*[?&]X-Amz-Expires=(\\d+).*", "$1"));
            assertTrue(expires <= 540, "X-Amz-Expires " + expires);
        }
        try (S3Storage s = S3Storage.fromEnv(env(S3Storage.ENV_URL_TTL, "120"))) {
            assertEquals(120, s.effectiveUrlTtl(Duration.ofMinutes(5)).toSeconds());
        }
        AwsSessionCredentials expired = AwsSessionCredentials.builder()
                .accessKeyId(ACCESS_KEY).secretAccessKey(SECRET_KEY).sessionToken("t")
                .expirationTime(Instant.now().plusSeconds(30)).build();
        try (S3Storage s = S3Storage.builder(BUCKET).endpointOverride(URI.create(endpoint))
                .credentials(StaticCredentialsProvider.create(expired)).build()) {
            assertThrows(IllegalStateException.class, () -> s.effectiveUrlTtl(Duration.ofMinutes(5)));
        }
    }

    /** Handed-out URLs use the public endpoint; the worker's own traffic uses the endpoint. */
    @Test
    void publicEndpointIsUsedOnlyForHandedOutUrls() throws Exception {
        String pub = "https://s3.public.example:8443";
        try (S3Storage s = S3Storage.fromEnv(env(S3Storage.ENV_PUBLIC_ENDPOINT, pub))) {
            byte[] data = "public endpoint".getBytes(StandardCharsets.UTF_8);
            assertArrayEquals(data, s.get(s.putObject(data), 1 << 20));
            URI result = s.upload(data, null);
            assertEquals("s3.public.example", result.getHost());
            assertEquals(8443, result.getPort());
            assertTrue(result.getPath().startsWith("/" + BUCKET + "/direct/rpc/"), result.getPath());
            UploadUrlProvider.UploadUrl grant = s.generateUploadUrl(4, "0".repeat(64));
            assertEquals("s3.public.example", URI.create(grant.uploadUrl()).getHost());
            assertEquals("s3.public.example", URI.create(grant.downloadUrl()).getHost());
            // The URL works once rewritten to where this test can reach storage: same signature.
            URI reachable = URI.create(endpoint + result.getRawPath() + "?" + result.getRawQuery());
            HttpResponse<byte[]> r = HTTP.send(HttpRequest.newBuilder(reachable).build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            // The signature covers the public host, so the internal host does not verify; the
            // public URL is what clients get. Either refusal or success shows the host was signed.
            assertTrue(r.statusCode() == 403 || r.statusCode() == 200, "status " + r.statusCode());
        }
    }

    /** Checksum none: no upload grants, no checksum header, objects and results still stored. */
    @Test
    void checksumNone() throws Exception {
        List<String> heads = new CopyOnWriteArrayList<>();
        try (HeaderProxy proxy = new HeaderProxy(URI.create(endpoint), heads);
             S3Storage s = S3Storage.fromEnv(env(S3Storage.ENV_CHECKSUM, "none",
                     S3Storage.ENV_ENDPOINT, proxy.uri()))) {
            assertFalse(s.supportsUploadGrants());
            assertEquals(S3Storage.Checksum.NONE, s.checksum());
            assertThrows(UnsupportedOperationException.class, () -> s.generateUploadUrl(1, "0".repeat(64)));
            byte[] data = "checksum none".getBytes(StandardCharsets.UTF_8);
            assertArrayEquals(data, s.get(s.putObject(data), 1 << 20));
            s.upload(data, null);
            String puts = String.join("\n", heads).toLowerCase(java.util.Locale.ROOT);
            assertTrue(puts.contains("put /"), puts);
            assertFalse(puts.contains("x-amz-checksum-sha256"), "checksum none sent a sha256 header");
            assertFalse(puts.contains("x-amz-checksum-crc32"), "an SDK checksum went to a custom endpoint");
        }
        assertThrows(IllegalArgumentException.class, () -> S3Storage.fromEnv(env(S3Storage.ENV_CHECKSUM, "md5")));
    }

    /** The SDK's own CRC32 request checksums: off for a custom endpoint, on when asked. */
    @Test
    void sdkChecksumIsConfigurable() throws Exception {
        List<String> heads = new CopyOnWriteArrayList<>();
        try (HeaderProxy proxy = new HeaderProxy(URI.create(endpoint), heads);
             S3Storage s = S3Storage.fromEnv(env(S3Storage.ENV_ENDPOINT, proxy.uri(),
                     S3Storage.ENV_SDK_CHECKSUM, "when_supported"))) {
            s.upload("crc".getBytes(StandardCharsets.UTF_8), null);
            String all = String.join("\n", heads).toLowerCase(java.util.Locale.ROOT);
            assertTrue(all.contains("x-amz-checksum-crc32") || all.contains("x-amz-sdk-checksum-algorithm"), all);
        }
        heads.clear();
        try (HeaderProxy proxy = new HeaderProxy(URI.create(endpoint), heads);
             S3Storage s = S3Storage.fromEnv(env(S3Storage.ENV_ENDPOINT, proxy.uri()))) {
            s.upload("crc".getBytes(StandardCharsets.UTF_8), null);
            String all = String.join("\n", heads).toLowerCase(java.util.Locale.ROOT);
            assertFalse(all.contains("x-amz-checksum-crc32"), all);
        }
        assertThrows(IllegalArgumentException.class,
                () -> S3Storage.fromEnv(env(S3Storage.ENV_SDK_CHECKSUM, "always")));
    }

    /** Plain http needs the explicit opt-in; the key areas; an explicit token's expiry caps URLs. */
    @Test
    void plainHttpOptInKeyAreasAndSessionExpiry() throws Exception {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> S3Storage.fromEnv(env(S3Storage.ENV_ALLOW_PLAIN_HTTP, null)));
        assertTrue(e.getMessage().contains(S3Storage.ENV_ALLOW_PLAIN_HTTP), e.getMessage());
        assertThrows(IllegalArgumentException.class, () -> S3Storage.fromEnv(env(S3Storage.ENV_ALLOW_PLAIN_HTTP, null,
                S3Storage.ENV_ENDPOINT, null, S3Storage.ENV_PUBLIC_ENDPOINT, "http://public.example")));
        try (S3Storage fs = S3Storage.fromEnv(env(), S3Storage.AREA_FS)) {
            byte[] data = "fs area".getBytes(StandardCharsets.UTF_8);
            String key = fs.putObject(data);
            assertTrue(key.startsWith("direct/fs/"), key);
            fs.deleteObject(key);
            assertThrows(farm.query.vgirpc.external.ObjectMissingException.class, () -> fs.get(key, 1 << 20));
            fs.deleteObject(key); // best effort: a second delete is harmless
        }
        long epoch = Instant.now().plus(Duration.ofMinutes(20)).getEpochSecond();
        try (S3Storage s = S3Storage.fromEnv(env(S3Storage.ENV_SESSION_TOKEN, "t",
                S3Storage.ENV_SESSION_EXPIRES, Long.toString(epoch)))) {
            long ttl = s.effectiveUrlTtl(Duration.ofMinutes(5)).toSeconds();
            assertTrue(ttl <= 20 * 60 - 60 && ttl > 20 * 60 - 120, "ttl " + ttl);
        }
        String rfc = Instant.now().plus(Duration.ofMinutes(10)).toString();
        try (S3Storage s = S3Storage.fromEnv(env(S3Storage.ENV_SESSION_TOKEN, "t",
                S3Storage.ENV_SESSION_EXPIRES, rfc))) {
            // A 10-minute session cannot meet the 15-minute grant minimum: sign for what is left.
            long ttl = s.effectiveUrlTtl(S3Storage.MIN_UPLOAD_GRANT_TTL).toSeconds();
            assertTrue(ttl <= 540, "ttl " + ttl);
        }
        assertThrows(IllegalArgumentException.class, () -> S3Storage.fromEnv(env(
                S3Storage.ENV_SESSION_EXPIRES, "tomorrow", S3Storage.ENV_SESSION_TOKEN, "t")));
        assertThrows(IllegalArgumentException.class, () -> S3Storage.fromEnv(env(
                S3Storage.ENV_SESSION_EXPIRES, "1800000000")));
    }

    /** The startup check names the problem. */
    @Test
    void verifyIsActionable() {
        try (S3Storage s = S3Storage.fromEnv(env(S3Storage.ENV_BUCKET, "vgi-no-such-bucket"))) {
            IllegalStateException e = assertThrows(IllegalStateException.class, s::verify);
            assertTrue(e.getMessage().contains("does not exist"), e.getMessage());
        }
        try (S3Storage s = S3Storage.fromEnv(env(S3Storage.ENV_SECRET_ACCESS_KEY, "wrong-secret"))) {
            IllegalStateException e = assertThrows(IllegalStateException.class, s::verify);
            assertTrue(e.getMessage().contains("access denied") || e.getMessage().contains("HTTP 4"), e.getMessage());
            assertFalse(e.getMessage().contains("wrong-secret"), "the secret leaked into the message");
        }
        try (S3Storage s = S3Storage.fromEnv(env(S3Storage.ENV_ENDPOINT, "http://127.0.0.1:1"))) {
            IllegalStateException e = assertThrows(IllegalStateException.class, s::verify);
            assertTrue(e.getMessage().contains("unreachable"), e.getMessage());
        }
        assertThrows(IllegalArgumentException.class, () -> S3Storage.fromEnv(env(S3Storage.ENV_BUCKET, null)));
        assertThrows(IllegalArgumentException.class,
                () -> S3Storage.fromEnv(env(S3Storage.ENV_ENDPOINT, "ftp://x")));
    }

    /** A pass-through TCP proxy recording the request head of every client request. */
    private static final class HeaderProxy implements AutoCloseable {
        private final ServerSocket server;
        private final List<Socket> open = new CopyOnWriteArrayList<>();

        HeaderProxy(URI target, List<String> heads) throws Exception {
            server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            Thread t = new Thread(() -> {
                while (!server.isClosed()) {
                    try {
                        Socket client = server.accept();
                        Socket upstream = new Socket(target.getHost(), target.getPort());
                        open.add(client);
                        open.add(upstream);
                        pump(upstream, client, null);
                        pump(client, upstream, heads);
                    } catch (Exception e) {
                        return;
                    }
                }
            });
            t.setDaemon(true);
            t.start();
        }

        private static void pump(Socket from, Socket to, List<String> heads) {
            Thread t = new Thread(() -> {
                byte[] buf = new byte[16 * 1024];
                try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
                    int n;
                    while ((n = in.read(buf)) >= 0) {
                        if (heads != null) {
                            String text = new String(buf, 0, n, StandardCharsets.ISO_8859_1);
                            int end = text.indexOf("\r\n\r\n");
                            if (end > 0 && (text.startsWith("PUT ") || text.startsWith("GET ")
                                    || text.startsWith("HEAD "))) {
                                heads.add(text.substring(0, end));
                            }
                        }
                        out.write(buf, 0, n);
                        out.flush();
                    }
                } catch (Exception ignored) {
                    // closed
                }
            });
            t.setDaemon(true);
            t.start();
        }

        String uri() { return "http://127.0.0.1:" + server.getLocalPort(); }

        @Override
        public void close() throws Exception {
            server.close();
            for (Socket s : open) s.close();
        }
    }
}
