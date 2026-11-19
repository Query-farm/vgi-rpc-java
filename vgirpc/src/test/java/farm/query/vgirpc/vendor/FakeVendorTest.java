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
            URI download = s.upload(body, null);
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
    void generateUploadUrlVendsAWorkingPair() throws Exception {
        try (VendorStorage s = new VendorStorage(url, token)) {
            long before = stats().get("upload_urls").asLong();
            UploadUrlProvider.UploadUrl u = s.generateUploadUrl();
            assertNotEquals(u.uploadUrl(), u.downloadUrl());
            assertEquals(before + 1, stats().get("upload_urls").asLong());
            byte[] body = new byte[] {7, 7, 7};
            HttpResponse<Void> put = HTTP.send(HttpRequest.newBuilder(URI.create(u.uploadUrl()))
                    .PUT(HttpRequest.BodyPublishers.ofByteArray(body)).build(), HttpResponse.BodyHandlers.discarding());
            assertTrue(put.statusCode() / 100 == 2, "PUT " + put.statusCode());
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
}
