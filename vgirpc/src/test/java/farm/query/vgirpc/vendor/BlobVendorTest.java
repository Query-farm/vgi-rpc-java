// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.vendor;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import farm.query.vgirpc.external.ExternalLocationConfig;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Pinning, environment parsing and wire helpers; no vendor needed. */
class BlobVendorTest {

    private static void ok(Consumer<URI> v, String url) {
        assertDoesNotThrow(() -> v.accept(URI.create(url)), url);
    }

    private static void bad(Consumer<URI> v, String url) {
        assertThrows(IllegalArgumentException.class, () -> v.accept(URI.create(url)), url);
    }

    @Test
    void pinnedHostsAcceptOnlyExactHostAndPort() {
        Consumer<URI> v = BlobVendor.pinnedHostsValidator(List.of("r2.example.com", "eu.example.com:8443"),
                false, List.of());
        ok(v, "https://r2.example.com/bucket/key?X-Amz-Signature=abc");
        ok(v, "https://R2.EXAMPLE.COM:443/k");
        ok(v, "https://eu.example.com:8443/k");
        bad(v, "https://eu.example.com/k");            // entry with a port matches that port only
        bad(v, "https://r2.example.com:444/k");
        bad(v, "https://sub.r2.example.com/k");        // no subdomains
        bad(v, "http://r2.example.com/k");             // https only
        bad(v, "https://user:pw@r2.example.com/k");    // no userinfo
        bad(v, "https://evil.com/k");
    }

    @Test
    void pinnedHostsHttpOnlyWhenAllowed() {
        Consumer<URI> v = BlobVendor.pinnedHostsValidator(List.of("127.0.0.1:9000"), true, List.of());
        ok(v, "http://127.0.0.1:9000/r2/get/x");
        bad(v, "http://127.0.0.1:9001/r2/get/x");
        Consumer<URI> plain = BlobVendor.pinnedHostsValidator(List.of("localhost"), true, List.of());
        ok(plain, "http://localhost/x");
        ok(plain, "https://localhost/x");
    }

    @Test
    void pinnedHostsPrefixes() {
        Consumer<URI> v = BlobVendor.pinnedHostsValidator(List.of("s.example.com"), false,
                List.of("https://s.example.com/tenant-a/"));
        ok(v, "https://s.example.com/tenant-a/obj?sig=1");
        bad(v, "https://s.example.com/tenant-b/obj");
        bad(v, "https://s.example.com/tenant-a/../tenant-b/obj");
    }

    @Test
    void pinnedHostsRejectsBadEntries() {
        assertThrows(IllegalArgumentException.class,
                () -> BlobVendor.pinnedHostsValidator(List.of(), false, List.of()));
        assertThrows(IllegalArgumentException.class,
                () -> BlobVendor.pinnedHostsValidator(List.of("host/path"), false, List.of()));
        assertThrows(IllegalArgumentException.class,
                () -> BlobVendor.pinnedHostsValidator(List.of(""), false, List.of()));
    }

    @Test
    void rejectionNeverEchoesTheSignedQuery() {
        Consumer<URI> v = BlobVendor.pinnedHostsValidator(List.of("ok.example.com"), false, List.of());
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> v.accept(URI.create("https://evil.example.com/k?X-Amz-Signature=SECRET")));
        assertFalse(e.getMessage().contains("SECRET"), e.getMessage());
    }

    @Test
    void vendorUrlMustBeHttpsUnlessLoopback() {
        assertThrows(IllegalArgumentException.class, () -> new VendorBlobClient("http://example.com", "t"));
        assertThrows(IllegalArgumentException.class, () -> new VendorBlobClient("ftp://127.0.0.1", "t"));
        assertThrows(IllegalArgumentException.class, () -> new VendorBlobClient("https://example.com", ""));
        new VendorBlobClient("http://127.0.0.1:1", "t").close();
        new VendorBlobClient("http://localhost:1", "t").close();
        new VendorBlobClient("http://[::1]:1", "t").close();
        new VendorStorage("https://blobs.example.com", "t").close();
    }

    @Test
    void loopbackDetectionNeverResolvesNames() {
        assertTrue(VendorApi.isLoopbackHost("127.0.0.1"));
        assertTrue(VendorApi.isLoopbackHost("127.5.6.7"));
        assertTrue(VendorApi.isLoopbackHost("[::1]"));
        assertTrue(VendorApi.isLoopbackHost("localhost"));
        assertFalse(VendorApi.isLoopbackHost("10.0.0.1"));
        assertFalse(VendorApi.isLoopbackHost("localhost.evil.com"));
    }

    @Test
    void envConfig() {
        assertNull(BlobVendor.externalConfigFromEnv(Map.of()));
        assertThrows(IllegalArgumentException.class,
                () -> BlobVendor.externalConfigFromEnv(Map.of("VGI_BLOB_VENDOR_URL", "https://v.example.com")));
        ExternalLocationConfig cfg = BlobVendor.externalConfigFromEnv(Map.of(
                "VGI_BLOB_VENDOR_URL", "https://v.example.com",
                "VGI_CF_DO_TOKEN", "fallback",
                "VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES", "4096",
                "VGI_RPC_EXTERNAL_COMPRESSION", "zstd:5"));
        assertInstanceOf(VendorStorage.class, cfg.storage());
        assertEquals(4096, cfg.thresholdBytes());
        assertEquals(5, cfg.compression().level());
        ((VendorStorage) cfg.storage()).close();
        ExternalLocationConfig dflt = BlobVendor.externalConfigFromEnv(Map.of(
                "VGI_BLOB_VENDOR_URL", "https://v.example.com", "VGI_BLOB_VENDOR_TOKEN", "t"));
        assertEquals(1_048_576, dflt.thresholdBytes());
        assertNull(dflt.compression());
        ((VendorStorage) dflt.storage()).close();
        assertThrows(IllegalArgumentException.class, () -> BlobVendor.externalConfigFromEnv(Map.of(
                "VGI_BLOB_VENDOR_URL", "https://v.example.com", "VGI_BLOB_VENDOR_TOKEN", "t",
                "VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES", "0")));
        assertThrows(IllegalArgumentException.class, () -> BlobVendor.externalConfigFromEnv(Map.of(
                "VGI_BLOB_VENDOR_URL", "https://v.example.com", "VGI_BLOB_VENDOR_TOKEN", "t",
                "VGI_RPC_EXTERNAL_COMPRESSION", "brotli")));
        assertEquals(268435456L, BlobVendor.maxUploadBytesFromEnv(Map.of("VGI_RPC_MAX_UPLOAD_BYTES", "268435456")));
        assertNull(BlobVendor.maxUploadBytesFromEnv(Map.of()));
        assertThrows(IllegalArgumentException.class,
                () -> BlobVendor.maxRequestBytesFromEnv(Map.of("VGI_RPC_MAX_REQUEST_BYTES", "abc")));
    }

    @Test
    void httpSettingsWithoutVendorStillReadRequestCap() {
        BlobVendorHttpSettings s = BlobVendorHttpSettings.fromEnv(Map.of("VGI_RPC_MAX_REQUEST_BYTES", "32768"));
        assertFalse(s.vendorEnabled());
        assertEquals(32768L, s.maxRequestBytes());
        assertNull(s.uploadUrlProvider());
    }

    @Test
    void expiresParsing() {
        JsonNodeFactory f = JsonNodeFactory.instance;
        assertEquals(Instant.parse("2026-11-19T12:00:00Z"), VendorApi.parseExpires(f.textNode("2026-11-19T12:00:00Z")));
        assertEquals(Instant.parse("2026-11-19T12:00:00Z"),
                VendorApi.parseExpires(f.textNode("2026-11-19T12:00:00+00:00")));
        assertEquals(Instant.ofEpochSecond(1_800_000_000L), VendorApi.parseExpires(f.numberNode(1_800_000_000L)));
        assertEquals(Instant.ofEpochSecond(1_800_000_000L), VendorApi.parseExpires(f.numberNode(1_800_000_000_000L)));
        assertThrows(VendorError.class, () -> VendorApi.parseExpires(f.booleanNode(true)));
        assertThrows(VendorError.class, () -> VendorApi.parseExpires(f.textNode("soon")));
    }

    @Test
    void s3ErrorCodes() {
        assertEquals("NoSuchKey", VendorApi.s3ErrorCode(
                "<?xml version=\"1.0\"?><Error><Code>NoSuchKey</Code></Error>".getBytes(StandardCharsets.UTF_8)));
        assertNull(VendorApi.s3ErrorCode("nope".getBytes(StandardCharsets.UTF_8)));
        assertNull(VendorApi.s3ErrorCode("<Code>a b</Code>".getBytes(StandardCharsets.UTF_8)));
    }
}
