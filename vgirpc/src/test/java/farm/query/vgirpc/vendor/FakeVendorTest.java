// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.vendor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import farm.query.vgirpc.external.ExternalLocationConfig;
import farm.query.vgirpc.external.UploadUrlProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The vendor clients against vgi-rpc-python's {@code vgi-rpc-fake-vendor
 * --test-routes}, including its fault routes.
 *
 * <p>Runs when {@code VGI_BLOB_VENDOR_URL} names such a vendor (the CI
 * {@code blob vendor} job starts one with {@code uvx}); skipped otherwise.</p>
 */
class FakeVendorTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();

    private String url;
    private String token;

    @BeforeEach
    void setUp() throws Exception {
        url = System.getenv("VGI_BLOB_VENDOR_URL");
        assumeTrue(url != null && !url.isBlank(), "VGI_BLOB_VENDOR_URL (a fake vendor with --test-routes) not set");
        String t = System.getenv("VGI_BLOB_VENDOR_TOKEN");
        token = t == null || t.isBlank() ? "test-token" : t;
        admin("/__admin__/reset");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (url != null && !url.isBlank()) admin("/__admin__/reset");
    }

    private JsonNode admin(String path) throws Exception {
        HttpResponse<byte[]> r = HTTP.send(HttpRequest.newBuilder(URI.create(url + path)).GET().build(),
                HttpResponse.BodyHandlers.ofByteArray());
        assertEquals(200, r.statusCode(), path);
        return JSON.readTree(r.body());
    }

    private JsonNode stats() throws Exception {
        return admin("/__stats__");
    }

    private VendorBlobClient blobs() {
        return new VendorBlobClient(url, token);
    }

    @Test
    void putGetDeleteRoundTrip() throws Exception {
        byte[] data = "x".repeat(5000).getBytes(StandardCharsets.UTF_8);
        try (VendorBlobClient c = blobs()) {
            JsonNode before = stats();
            BlobRef ref = c.putBytes(data, new BlobScope("loc-anon", "s.abcd"), null);
            assertEquals(5000, ref.size());
            assertArrayEquals(data, c.getBytes(ref));
            JsonNode after = stats();
            assertEquals(1, after.get("puts").asLong() - before.get("puts").asLong());
            assertEquals(1, after.get("gets").asLong() - before.get("gets").asLong());
            assertEquals(5000, after.get("bytes_uploaded").asLong() - before.get("bytes_uploaded").asLong());
            assertEquals(1, c.deleteRefs(List.of(ref)));
            VendorError gone = assertThrows(VendorError.class, () -> c.getBytes(ref));
            assertEquals(404, gone.status());
            assertEquals("not_found", gone.code());
        }
    }

    @Test
    void emptyBlobRoundTrips() {
        try (VendorBlobClient c = blobs()) {
            BlobRef ref = c.putBytes(new byte[0], null, null);
            assertArrayEquals(new byte[0], c.getBytes(ref));
        }
    }

    @Test
    void idempotencyKeyReusesTheObject() {
        try (VendorBlobClient c = blobs()) {
            String key = java.util.UUID.randomUUID().toString().replace("-", "") + ".0";
            BlobRef a = c.putBytes(new byte[] {1, 2, 3}, null, key);
            BlobRef b = c.putBytes(new byte[] {1, 2, 3}, null, key);
            assertEquals(a.objectId(), b.objectId());
            VendorError conflict = assertThrows(VendorError.class, () -> c.putBytes(new byte[] {4}, null, key));
            assertEquals(409, conflict.status());
            assertEquals("idempotency_conflict", conflict.code());
        }
    }

    @Test
    void deleteScopeRemovesOnlyThatScope() {
        try (VendorBlobClient c = blobs()) {
            String exec = "q." + Long.toHexString(System.nanoTime());
            BlobRef in = c.putBytes(new byte[] {1}, new BlobScope("att-1", exec), null);
            BlobRef out = c.putBytes(new byte[] {2}, new BlobScope("att-1", exec + "x"), null);
            assertEquals(1, c.deleteScope("att-1", exec));
            assertThrows(VendorError.class, () -> c.getBytes(in));
            assertArrayEquals(new byte[] {2}, c.getBytes(out));
        }
    }

    @Test
    void corruptDownloadIsAShaMismatch() throws Exception {
        try (VendorBlobClient c = blobs()) {
            BlobRef ref = c.putBytes("payload".getBytes(StandardCharsets.UTF_8), null, null);
            admin("/__admin__/fault?purpose=fs&action=corrupt&count=1");
            VendorError e = assertThrows(VendorError.class, () -> c.getBytes(ref));
            assertEquals(VendorError.SHA256_MISMATCH, e.code());
            admin("/__admin__/reset");
            assertArrayEquals("payload".getBytes(StandardCharsets.UTF_8), c.getBytes(ref));
        }
    }

    @Test
    void missingDownloadIsA404() throws Exception {
        try (VendorBlobClient c = blobs()) {
            BlobRef ref = c.putBytes("payload".getBytes(StandardCharsets.UTF_8), null, null);
            admin("/__admin__/fault?purpose=fs&action=missing&count=1");
            VendorError e = assertThrows(VendorError.class, () -> c.getBytes(ref));
            assertEquals(404, e.status());
            assertEquals("NoSuchKey", e.code());
        }
    }

    @Test
    void storageUploadReturnsAPinnedDownloadUrl() throws Exception {
        byte[] body = "arrow-ipc-bytes".getBytes(StandardCharsets.UTF_8);
        try (VendorStorage s = new VendorStorage(url, token)) {
            JsonNode start = stats();
            URI download = s.upload(body, null);
            // A server result write: puts_rpc, never a client-upload grant.
            JsonNode after = stats();
            assertEquals(start.get("puts_rpc").asLong() + 1, after.get("puts_rpc").asLong());
            assertEquals(start.get("puts_upload").asLong(), after.get("puts_upload").asLong());
            assertDoesNotThrow(() -> s.urlValidator().accept(download));
            HttpResponse<byte[]> r = HTTP.send(HttpRequest.newBuilder(download).GET().build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(200, r.statusCode());
            assertArrayEquals(body, r.body());
            assertThrows(IllegalArgumentException.class,
                    () -> s.urlValidator().accept(URI.create("https://evil.example.com/r2/get/x")));
        }
    }

    @Test
    void generateUploadUrlVendsASizedChecksummedPair() throws Exception {
        try (VendorStorage s = new VendorStorage(url, token)) {
            byte[] body = new byte[] {7, 7, 7};
            String sha = java.util.HexFormat.of().formatHex(VendorStorage.sha256(body));
            JsonNode start = stats();
            long before = start.get("puts").asLong();
            long unsizedBefore = start.path("upload_urls").asLong();
            UploadUrlProvider.UploadUrl u = s.generateUploadUrl(body.length, sha);
            assertNotEquals(u.uploadUrl(), u.downloadUrl());
            // The sized purpose=rpc grant, never the unsized /v1/upload_urls path,
            // marked origin=client_upload: the fake counts it as a client upload
            // grant (puts_upload), not a server rpc write (puts_rpc).
            JsonNode granted = stats();
            assertEquals(before + 1, granted.get("puts").asLong());
            assertEquals(unsizedBefore, granted.path("upload_urls").asLong());
            assertEquals(start.get("puts_upload").asLong() + 1, granted.get("puts_upload").asLong());
            assertEquals(start.get("puts_rpc").asLong(), granted.get("puts_rpc").asLong());
            assertEquals(java.util.Base64.getEncoder().encodeToString(VendorStorage.sha256(body)),
                    u.headers().get("x-amz-checksum-sha256"));
            assertTrue(u.headers().keySet().stream().noneMatch("content-length"::equalsIgnoreCase));

            // Wrong bytes of the right length: storage refuses them.
            HttpRequest.Builder bad = HttpRequest.newBuilder(URI.create(u.uploadUrl()))
                    .PUT(HttpRequest.BodyPublishers.ofByteArray(new byte[] {7, 7, 8}));
            u.headers().forEach(bad::header);
            HttpResponse<Void> refused = HTTP.send(bad.build(), HttpResponse.BodyHandlers.discarding());
            assertEquals(400, refused.statusCode());

            HttpRequest.Builder put = HttpRequest.newBuilder(URI.create(u.uploadUrl()))
                    .PUT(HttpRequest.BodyPublishers.ofByteArray(body));
            u.headers().forEach(put::header);
            HttpResponse<Void> ok = HTTP.send(put.build(), HttpResponse.BodyHandlers.discarding());
            assertTrue(ok.statusCode() / 100 == 2, "PUT " + ok.statusCode());
            JsonNode uploaded = stats();
            assertEquals(granted.get("client_uploads").asLong() + 1, uploaded.get("client_uploads").asLong());
            assertEquals(granted.get("client_upload_refusals").asLong() + 1,
                    uploaded.get("client_upload_refusals").asLong());
            HttpResponse<byte[]> get = HTTP.send(HttpRequest.newBuilder(URI.create(u.downloadUrl())).GET().build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            assertArrayEquals(body, get.body());
        }
    }

    @Test
    void wrongTokenIsUnauthorized() {
        try (VendorBlobClient c = new VendorBlobClient(url, "not-a-token")) {
            VendorError e = assertThrows(VendorError.class, () -> c.putBytes(new byte[] {1}, null, null));
            assertEquals(401, e.status());
        }
    }

    @Test
    void envConfigUsesTheVendor() {
        ExternalLocationConfig cfg = BlobVendor.externalConfigFromEnv(Map.of(
                "VGI_BLOB_VENDOR_URL", url, "VGI_BLOB_VENDOR_TOKEN", token,
                "VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES", "4096"));
        try (VendorStorage s = (VendorStorage) cfg.storage()) {
            assertEquals(4096, cfg.thresholdBytes());
            assertTrue(s.info().uploadHosts().size() >= 1);
        }
    }

    @Test
    void putObjectAndGetByIdRoundTrip() throws Exception {
        // The protocol-3.4 object API (STATE_REF_V1.md §10.1): a sized rpc grant marked
        // origin=state_ref, then a GET minted server-side from the object_id.
        try (VendorStorage s = new VendorStorage(url, token)) {
            byte[] body = "a sealed StateRef object".repeat(40).getBytes(StandardCharsets.UTF_8);
            JsonNode start = stats();
            String id = s.putObject(body);
            assertTrue(!id.isEmpty());
            JsonNode put = stats();
            assertEquals(start.get("puts_state_ref").asLong() + 1, put.get("puts_state_ref").asLong());
            assertEquals(start.get("puts_rpc").asLong() + 1, put.get("puts_rpc").asLong());
            assertArrayEquals(body, s.get(id, 1L << 20));
            JsonNode got = stats();
            assertEquals(put.get("gets_state_ref").asLong() + 1, got.get("gets_state_ref").asLong());
            assertEquals(0, got.path("upload_urls").asLong());
            assertThrows(VendorError.class, () -> s.get(id, body.length - 1L), "the fetch cap is enforced");

            JsonNode deleted = admin("/__admin__/delete?origin=state_ref");
            assertTrue(deleted.get("deleted").asLong() >= 1);
            assertThrows(farm.query.vgirpc.external.ObjectMissingException.class, () -> s.get(id, 1L << 20));
        }
    }

    @Test
    void generateUploadUrlCarriesTheObjectId() throws Exception {
        try (VendorStorage s = new VendorStorage(url, token)) {
            byte[] body = new byte[] {1, 2, 3, 4};
            UploadUrlProvider.UploadUrl u = s.generateUploadUrl(body.length,
                    java.util.HexFormat.of().formatHex(VendorStorage.sha256(body)));
            assertTrue(!u.objectId().isEmpty(), "every provider supplies the object_id (protocol 3.4)");
            HttpRequest.Builder put = HttpRequest.newBuilder(URI.create(u.uploadUrl()))
                    .PUT(HttpRequest.BodyPublishers.ofByteArray(body));
            u.headers().forEach(put::header);
            assertEquals(200, HTTP.send(put.build(), HttpResponse.BodyHandlers.discarding()).statusCode() / 100 * 100);
            JsonNode before = stats();
            assertArrayEquals(body, s.get(u.objectId(), 1024));
            assertEquals(before.get("gets_upload").asLong() + 1, stats().get("gets_upload").asLong());
        }
    }
}
