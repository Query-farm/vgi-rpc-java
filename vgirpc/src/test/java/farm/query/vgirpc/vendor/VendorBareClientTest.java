// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.vendor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A tiny in-process vendor + storage that records what each request carried:
 * the bearer token reaches {@code /v1} only, never a signed URL; signed query
 * strings never reach an error message; vended URLs off the pinned hosts are
 * refused.
 */
class VendorBareClientTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String TOKEN = "unit-token";

    private HttpServer server;
    private String base;
    private final Map<String, byte[]> objects = new ConcurrentHashMap<>();
    private final List<Map<String, List<String>>> storageHeaders = new CopyOnWriteArrayList<>();
    private final List<String> apiAuth = new CopyOnWriteArrayList<>();
    private final List<JsonNode> putRequests = new CopyOnWriteArrayList<>();
    private final AtomicBoolean offHostGrant = new AtomicBoolean();
    private final AtomicInteger storage503 = new AtomicInteger();
    private final AtomicBoolean storage403 = new AtomicBoolean();

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        base = "http://127.0.0.1:" + server.getAddress().getPort();
        server.createContext("/v1/", this::api);
        server.createContext("/store/", this::store);
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private static void send(HttpExchange ex, int status, byte[] body, String type) throws IOException {
        if (type != null) ex.getResponseHeaders().add("Content-Type", type);
        ex.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
        if (body.length > 0) ex.getResponseBody().write(body);
        ex.close();
    }

    private void api(HttpExchange ex) throws IOException {
        apiAuth.add(ex.getRequestHeaders().getFirst("Authorization"));
        String path = ex.getRequestURI().getPath();
        byte[] in = ex.getRequestBody().readAllBytes();
        JsonNode req = in.length == 0 ? JSON.createObjectNode() : JSON.readTree(in);
        Map<String, Object> out;
        switch (path) {
            case "/v1/info" -> out = Map.of("tenant", "t", "region", "r",
                    "upload_hosts", List.of("127.0.0.1:" + server.getAddress().getPort()),
                    "download_hosts", List.of("127.0.0.1:" + server.getAddress().getPort()),
                    "url_prefixes", List.of(base + "/store/"));
            case "/v1/blobs/put" -> {
                putRequests.add(req);
                String id = "obj" + objects.size();
                objects.put(id, new byte[0]);
                String host = offHostGrant.get() ? "https://evil.example.com" : base;
                out = Map.of("object_id", id,
                        "upload_url", host + "/store/" + id + "?X-Amz-Signature=PUTSECRET",
                        "upload_method", "PUT",
                        "required_headers", Map.of("content-length", req.get("size").asText(),
                                "x-amz-checksum-sha256", req.get("sha256_b64").asText()),
                        "expires_at", "2099-01-01T00:00:00Z");
            }
            case "/v1/blobs/get" -> {
                String id = req.get("object_id").asText();
                byte[] data = objects.get(id);
                if (data == null) {
                    send(ex, 404, "{\"error\":\"not_found\",\"message\":\"no such object\"}"
                            .getBytes(StandardCharsets.UTF_8), "application/json");
                    return;
                }
                out = Map.of("object_id", id, "download_url", base + "/store/" + id + "?X-Amz-Signature=GETSECRET",
                        "size", data.length, "sha256_b64", b64(data), "expires_at", 4_102_444_800L);
            }
            default -> {
                send(ex, 404, new byte[0], null);
                return;
            }
        }
        send(ex, 200, JSON.writeValueAsBytes(out), "application/json");
    }

    private void store(HttpExchange ex) throws IOException {
        storageHeaders.add(Map.copyOf(ex.getRequestHeaders()));
        String id = ex.getRequestURI().getPath().substring("/store/".length());
        byte[] in = ex.getRequestBody().readAllBytes();
        if (storage503.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
            send(ex, 503, new byte[0], null);
            return;
        }
        if (storage403.get()) {
            send(ex, 403, "<Error><Code>AccessDenied</Code></Error>".getBytes(StandardCharsets.UTF_8), null);
            return;
        }
        if (ex.getRequestMethod().equals("PUT")) {
            String declared = ex.getRequestHeaders().getFirst("x-amz-checksum-sha256");
            if (!b64(in).equals(declared)) {
                send(ex, 400, "<Error><Code>BadDigest</Code></Error>".getBytes(StandardCharsets.UTF_8), null);
                return;
            }
            objects.put(id, in);
            send(ex, 200, new byte[0], null);
        } else {
            send(ex, 200, objects.getOrDefault(id, new byte[0]), "application/octet-stream");
        }
    }

    private static String b64(byte[] data) {
        try {
            return Base64.getEncoder().encodeToString(MessageDigest.getInstance("SHA-256").digest(data));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private VendorBlobClient client() {
        return new VendorBlobClient(base, TOKEN, "fs", java.time.Duration.ofSeconds(5), 3, 0.01, 600, 1 << 20);
    }

    @Test
    void tokenGoesToTheVendorOnlyNeverToStorage() {
        byte[] data = "hello vendor".getBytes(StandardCharsets.UTF_8);
        try (VendorBlobClient c = client()) {
            BlobRef ref = c.putBytes(data, new BlobScope("loc-anon", "s.00"), "k.0");
            assertArrayEquals(data, c.getBytes(ref));
        }
        assertFalse(apiAuth.isEmpty());
        apiAuth.forEach(a -> assertEquals("Bearer " + TOKEN, a));
        assertEquals(2, storageHeaders.size());
        for (Map<String, List<String>> h : storageHeaders) {
            assertNull(h.get("Authorization"), "storage request carried Authorization");
            assertNull(h.get("Cookie"));
        }
    }

    @Test
    void transientStorageErrorsRetry() {
        try (VendorBlobClient c = client()) {
            storage503.set(2);
            BlobRef ref = c.putBytes(new byte[] {1, 2, 3}, null, null);
            assertArrayEquals(new byte[] {1, 2, 3}, c.getBytes(ref));
        }
    }

    @Test
    void errorsNeverCarryTheSignedQuery() {
        try (VendorBlobClient c = client()) {
            BlobRef ref = c.putBytes(new byte[] {9}, null, null);
            storage403.set(true);
            VendorError e = assertThrows(VendorError.class, () -> c.getBytes(ref));
            assertEquals(403, e.status());
            assertEquals("AccessDenied", e.code());
            assertFalse(e.getMessage().contains("SECRET"), e.getMessage());
            assertTrue(e.getMessage().contains("/store/"), e.getMessage());
        }
    }

    @Test
    void missingObjectIsA404WithTheVendorCode() {
        try (VendorBlobClient c = client()) {
            VendorError e = assertThrows(VendorError.class, () -> c.getBytes(new BlobRef("nope", 1, "00")));
            assertEquals(404, e.status());
            assertEquals("not_found", e.code());
        }
    }

    @Test
    void verifiesAgainstTheReferenceNotTheVendor() {
        try (VendorBlobClient c = client()) {
            BlobRef ref = c.putBytes(new byte[] {1, 2, 3, 4}, null, null);
            VendorError size = assertThrows(VendorError.class,
                    () -> c.getBytes(new BlobRef(ref.objectId(), 5, ref.sha256())));
            assertEquals(VendorError.SIZE_MISMATCH, size.code());
            VendorError sha = assertThrows(VendorError.class,
                    () -> c.getBytes(new BlobRef(ref.objectId(), 4, "00".repeat(32))));
            assertEquals(VendorError.SHA256_MISMATCH, sha.code());
        }
    }

    @Test
    void grantOffThePinnedHostsIsRefused() {
        offHostGrant.set(true);
        try (VendorBlobClient c = client()) {
            VendorError e = assertThrows(VendorError.class, () -> c.putBytes(new byte[] {1}, null, null));
            assertTrue(e.getMessage().contains("pinned"), e.getMessage());
            assertFalse(e.getMessage().contains("SECRET"), e.getMessage());
        }
        assertTrue(storageHeaders.isEmpty(), "nothing may be sent to an unpinned host");
    }

    @Test
    void clientUploadGrantsCarryTheOriginMarkerAndServerWritesDoNot() {
        byte[] body = "client request".getBytes(StandardCharsets.UTF_8);
        try (VendorStorage s = new VendorStorage(base, TOKEN, "rpc", java.time.Duration.ofSeconds(5), 3, 0.01,
                600, true)) {
            s.generateUploadUrl(body.length, java.util.HexFormat.of().formatHex(VendorStorage.sha256(body)));
            s.upload("server result".getBytes(StandardCharsets.UTF_8), null);
        }
        try (VendorBlobClient c = client()) {
            c.putBytes("stored value".getBytes(StandardCharsets.UTF_8), null, null);
        }
        assertEquals(3, putRequests.size());
        JsonNode upload = putRequests.get(0);
        assertEquals("client_upload", VendorStorage.CLIENT_UPLOAD_ORIGIN);
        assertEquals(VendorStorage.CLIENT_UPLOAD_ORIGIN, upload.path("origin").asText(null));
        assertEquals("rpc", upload.path("purpose").asText());
        assertFalse(putRequests.get(1).has("origin"), "a server result write is not a client upload");
        assertFalse(putRequests.get(2).has("origin"), "a storage write is not a client upload");
    }
}
