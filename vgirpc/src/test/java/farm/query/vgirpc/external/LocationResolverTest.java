// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.external;

import com.sun.net.httpserver.HttpServer;
import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamWriter;
import farm.query.vgirpc.wire.Metadata;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * End-to-end test: serve an Arrow IPC stream from a local HTTP server and
 * verify {@link LocationResolver} fetches + parses it back into the expected
 * data batch. Covers retry on transient failures and SHA-256 mismatch.
 */
final class LocationResolverTest {

    private static final Schema SCHEMA = new Schema(List.of(
            new Field("v", FieldType.notNullable(new ArrowType.Int(64, true)), null)));

    private HttpServer http;
    private int port;

    @BeforeEach
    void start() throws Exception {
        http = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        http.start();
        port = http.getAddress().getPort();
    }

    @AfterEach
    void stop() { if (http != null) http.stop(0); }

    private static byte[] buildIpcStream(long[] values) throws Exception {
        try (VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, Allocators.root());
             ByteArrayOutputStream bos = new ByteArrayOutputStream();
             IpcStreamWriter w = new IpcStreamWriter(bos)) {
            root.allocateNew();
            org.apache.arrow.vector.BigIntVector v = (org.apache.arrow.vector.BigIntVector) root.getVector(0);
            for (int i = 0; i < values.length; i++) v.setSafe(i, values[i]);
            root.setRowCount(values.length);
            w.writeBatch(root, null);
            w.writeEos();
            w.close();
            return bos.toByteArray();
        }
    }

    @Test
    void resolves_pointer_batch_end_to_end() throws Exception {
        byte[] ipc = buildIpcStream(new long[]{1, 2, 3});
        http.createContext("/blob", exchange -> {
            exchange.getResponseHeaders().set("Content-Type", "application/vnd.apache.arrow.stream");
            exchange.sendResponseHeaders(200, ipc.length);
            exchange.getResponseBody().write(ipc);
            exchange.getResponseBody().close();
        });

        LocationResolver resolver = new LocationResolver(
                ExternalLocationConfig.builder().urlValidator(ExternalLocationConfig.permissiveValidator()).build());
        Map<String, String> pointer = new LinkedHashMap<>();
        pointer.put(Metadata.LOCATION, "http://127.0.0.1:" + port + "/blob");
        pointer.put("other-key", "preserved");
        LocationResolver.Resolved out = resolver.resolve(pointer);
        try {
            assertEquals(3, out.root().getRowCount());
            Map<String, Object> row0 = Marshalling.decodeRow(out.root(), null);
            assertEquals(1L, row0.get("v"));
            assertEquals("preserved", out.customMetadata().get("other-key"));
        } finally {
            out.root().close();
        }
    }

    @Test
    void rejects_sha256_mismatch() throws Exception {
        byte[] ipc = buildIpcStream(new long[]{42});
        http.createContext("/blob2", exchange -> {
            exchange.sendResponseHeaders(200, ipc.length);
            exchange.getResponseBody().write(ipc);
            exchange.getResponseBody().close();
        });
        LocationResolver resolver = new LocationResolver(
                ExternalLocationConfig.builder().urlValidator(ExternalLocationConfig.permissiveValidator()).build());
        Map<String, String> pointer = new LinkedHashMap<>();
        pointer.put(Metadata.LOCATION, "http://127.0.0.1:" + port + "/blob2");
        pointer.put(Metadata.LOCATION_SHA256, "0".repeat(64));  // guaranteed mismatch
        assertThrows(java.io.IOException.class, () -> resolver.resolve(pointer));
    }

    @Test
    void sha256_match_passes() throws Exception {
        byte[] ipc = buildIpcStream(new long[]{99});
        String sha = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(ipc));
        http.createContext("/blob3", exchange -> {
            exchange.sendResponseHeaders(200, ipc.length);
            exchange.getResponseBody().write(ipc);
            exchange.getResponseBody().close();
        });
        LocationResolver resolver = new LocationResolver(
                ExternalLocationConfig.builder().urlValidator(ExternalLocationConfig.permissiveValidator()).build());
        Map<String, String> pointer = new LinkedHashMap<>();
        pointer.put(Metadata.LOCATION, "http://127.0.0.1:" + port + "/blob3");
        pointer.put(Metadata.LOCATION_SHA256, sha);
        LocationResolver.Resolved out = resolver.resolve(pointer);
        try {
            assertEquals(1, out.root().getRowCount());
        } finally {
            out.root().close();
        }
    }

    @Test
    void retries_transient_failures() throws Exception {
        byte[] ipc = buildIpcStream(new long[]{7});
        AtomicInteger hits = new AtomicInteger();
        http.createContext("/flaky", exchange -> {
            int n = hits.incrementAndGet();
            if (n <= 1) {
                exchange.sendResponseHeaders(503, -1);
                exchange.close();
            } else {
                exchange.sendResponseHeaders(200, ipc.length);
                exchange.getResponseBody().write(ipc);
                exchange.getResponseBody().close();
            }
        });
        LocationResolver resolver = new LocationResolver(
                ExternalLocationConfig.builder()
                        .urlValidator(ExternalLocationConfig.permissiveValidator())
                        .maxRetries(2)
                        .retryDelay(java.time.Duration.ofMillis(20))
                        .build());
        Map<String, String> pointer = new LinkedHashMap<>();
        pointer.put(Metadata.LOCATION, "http://127.0.0.1:" + port + "/flaky");
        LocationResolver.Resolved out = resolver.resolve(pointer);
        try {
            assertEquals(1, out.root().getRowCount());
            assertEquals(2, hits.get());
        } finally {
            out.root().close();
        }
    }

    @Test
    void rejects_non_https_by_default() throws Exception {
        LocationResolver resolver = new LocationResolver(ExternalLocationConfig.builder().build());
        Map<String, String> pointer = new LinkedHashMap<>();
        pointer.put(Metadata.LOCATION, "http://127.0.0.1:" + port + "/anything");
        assertThrows(IllegalArgumentException.class, () -> resolver.resolve(pointer));
    }
}
