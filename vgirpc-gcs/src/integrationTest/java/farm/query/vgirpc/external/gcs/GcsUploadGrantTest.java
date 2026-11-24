// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.external.gcs;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import farm.query.vgirpc.external.ExternalFetcher;
import farm.query.vgirpc.external.ExternalLocationConfig;
import farm.query.vgirpc.external.UploadUrlProvider.UploadUrl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * GCS upload grants (direct storage contract, Addendum C): the real V4 signer (a generated
 * service-account key) against an in-process stand-in for the GCS XML API that enforces what
 * GCS enforces on these URLs: every signed header is sent and {@code x-goog-content-length-range}
 * holds. GCS cannot enforce a SHA-256, so a same-length wrong body is accepted by storage and
 * must be refused by the server when it resolves the upload. (fake-gcs-server does not serve
 * XML-API PUTs on signed URLs, so it cannot stand in here; real GCS is pending credentials.)
 */
final class GcsUploadGrantTest {

    private static final String BUCKET = "vgi-gcs-grants";
    private HttpServer server;
    private String endpoint;
    private Path keyFile;
    private final Map<String, byte[]> objects = new ConcurrentHashMap<>();
    private volatile String lastSignedHeaders = "";

    @BeforeEach
    void start() throws Exception {
        KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
        gen.initialize(2048);
        String pem = "-----BEGIN PRIVATE KEY-----\n"
                + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII))
                        .encodeToString(gen.generateKeyPair().getPrivate().getEncoded())
                + "\n-----END PRIVATE KEY-----\n";
        keyFile = Files.createTempFile("vgi-gcs-sa", ".json");
        Files.writeString(keyFile, "{\"type\":\"service_account\",\"project_id\":\"vgi-test\","
                + "\"private_key_id\":\"k1\",\"private_key\":" + jsonString(pem) + ","
                + "\"client_email\":\"signer@vgi-test.iam.gserviceaccount.com\",\"client_id\":\"1\","
                + "\"token_uri\":\"https://oauth2.googleapis.com/token\"}");
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handle);
        server.start();
        endpoint = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach
    void stop() throws IOException {
        server.stop(0);
        Files.deleteIfExists(keyFile);
    }

    private static String jsonString(String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n") + "\"";
    }

    private void handle(HttpExchange ex) throws IOException {
        Map<String, String> query = new HashMap<>();
        String raw = ex.getRequestURI().getRawQuery();
        if (raw != null) {
            for (String kv : raw.split("&")) {
                int i = kv.indexOf('=');
                if (i > 0) query.put(kv.substring(0, i), URLDecoder.decode(kv.substring(i + 1), StandardCharsets.UTF_8));
            }
        }
        String key = ex.getRequestURI().getPath().substring(1);
        byte[] body = ex.getRequestBody().readAllBytes();
        int status;
        byte[] out = new byte[0];
        if (!"GOOG4-RSA-SHA256".equals(query.get("X-Goog-Algorithm")) || !query.containsKey("X-Goog-Signature")) {
            status = 403;
        } else if (ex.getRequestMethod().equals("PUT")) {
            String signed = query.getOrDefault("X-Goog-SignedHeaders", "");
            lastSignedHeaders = signed;
            status = 200;
            for (String h : signed.split(";")) {
                if (!h.isEmpty() && !h.equals("host") && ex.getRequestHeaders().getFirst(h) == null) status = 403;
            }
            String range = ex.getRequestHeaders().getFirst(GcsStorage.LENGTH_RANGE_HEADER);
            if (status == 200 && range != null) {
                String[] lohi = range.split(",");
                if (body.length < Long.parseLong(lohi[0].strip()) || body.length > Long.parseLong(lohi[1].strip())) {
                    status = 400;
                }
            }
            if (status == 200) objects.put(key, body);
        } else {
            byte[] b = objects.get(key);
            status = b == null ? 404 : 200;
            if (b != null) out = b;
        }
        ex.sendResponseHeaders(status, out.length == 0 ? -1 : out.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(out); }
    }

    private GcsStorage storage() {
        return GcsStorage.builder(BUCKET).keyPrefix("t/rpc/").endpoint(endpoint).credentialsFile(keyFile).build();
    }

    private static int put(UploadUrl grant, byte[] body) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(grant.uploadUrl()))
                .PUT(HttpRequest.BodyPublishers.ofByteArray(body));
        grant.headers().forEach(b::header);
        return HttpClient.newHttpClient().send(b.build(), HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    private static String sha256(byte[] b) throws Exception {
        return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(b));
    }

    @Test
    void grantSignsTheExactLengthAndStorageRefusesAnyOtherLength() throws Exception {
        byte[] body = "an upload of exactly forty-one bytes long".getBytes(StandardCharsets.UTF_8);
        try (GcsStorage gcs = storage()) {
            UploadUrl grant = gcs.generateUploadUrl(body.length, sha256(body));
            assertEquals(Map.of(GcsStorage.LENGTH_RANGE_HEADER, body.length + "," + body.length), grant.headers());
            assertTrue(grant.uploadUrl().startsWith(endpoint + "/" + BUCKET + "/t/rpc/"), grant.uploadUrl());
            assertTrue(grant.objectId().startsWith("t/rpc/"), grant.objectId());
            assertEquals(400, put(grant, java.util.Arrays.copyOf(body, body.length - 1)));
            assertEquals(400, put(grant, java.util.Arrays.copyOf(body, body.length + 1)));
            assertEquals(200, put(grant, body));
            assertTrue(java.util.List.of(lastSignedHeaders.split(";")).contains(GcsStorage.LENGTH_RANGE_HEADER),
                    "signed headers: " + lastSignedHeaders);
            assertArrayEquals(body, HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create(grant.downloadUrl())).build(),
                    HttpResponse.BodyHandlers.ofByteArray()).body());
        }
    }

    @Test
    void sameLengthWrongBodyIsRefusedWhenTheServerResolvesTheUpload() throws Exception {
        byte[] body = new byte[4096];
        java.util.Arrays.fill(body, (byte) 7);
        byte[] wrong = body.clone();
        wrong[0] ^= (byte) 0xff;
        try (GcsStorage gcs = storage()) {
            UploadUrl grant = gcs.generateUploadUrl(body.length, sha256(body));
            // GCS has no SHA-256 to enforce: the same-length wrong body is stored.
            assertEquals(200, put(grant, wrong));
            ExternalFetcher fetcher = new ExternalFetcher(ExternalLocationConfig.builder()
                    .urlValidator(gcs.urlValidator()).maxRetries(0).build());
            IOException e = assertThrows(IOException.class,
                    () -> fetcher.fetch(URI.create(grant.downloadUrl()), sha256(body)));
            assertTrue(e.getMessage().contains("sha256 mismatch"), e.getMessage());
            // The right body passes the same check.
            assertEquals(200, put(grant, body));
            assertArrayEquals(body, fetcher.fetch(URI.create(grant.downloadUrl()), sha256(body)));
        }
    }

    @Test
    void fromEnvStoreIssuesGrants() {
        try (GcsStorage gcs = GcsStorage.fromEnv(Map.of(
                GcsStorage.ENV_BUCKET, BUCKET,
                GcsStorage.ENV_ENDPOINT, endpoint,
                GcsStorage.ENV_ALLOW_PLAIN_HTTP, "1",
                GcsStorage.ENV_CREDENTIALS_FILE, keyFile.toString()))) {
            UploadUrl grant = gcs.generateUploadUrl(10, "0".repeat(64));
            assertTrue(grant.objectId().startsWith("rpc/"), grant.objectId());
            assertThrows(IllegalArgumentException.class, () -> gcs.generateUploadUrl(0, "0".repeat(64)));
            assertThrows(IllegalArgumentException.class, () -> gcs.generateUploadUrl(10, "XYZ"));
        }
    }
}
