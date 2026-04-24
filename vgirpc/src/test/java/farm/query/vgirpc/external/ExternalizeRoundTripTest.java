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
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Verifies {@link Externalizer} produces a pointer batch that
 * {@link LocationResolver} can fetch back, against a local in-memory storage
 * exposed over HTTP. Proves the upload↔fetch contract independently of S3.
 */
final class ExternalizeRoundTripTest {

    private static final Schema SCHEMA = new Schema(List.of(
            new Field("v", FieldType.notNullable(new ArrowType.Int(64, true)), null)));

    private HttpServer http;
    private int port;
    private final Map<String, byte[]> objects = new ConcurrentHashMap<>();

    @BeforeEach
    void start() throws Exception {
        http = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        http.createContext("/obj/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            String key = path.substring(path.indexOf("/obj/") + 5);
            byte[] body = objects.get(key);
            if (body == null) { exchange.sendResponseHeaders(404, -1); exchange.close(); return; }
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.getResponseBody().close();
        });
        http.start();
        port = http.getAddress().getPort();
    }

    @AfterEach
    void stop() { if (http != null) http.stop(0); }

    /** In-memory "storage" that serves uploads from the shared HttpServer. */
    private final class MapStorage implements ExternalStorage {
        @Override
        public URI upload(byte[] body, String contentEncoding) {
            String key = UUID.randomUUID().toString();
            objects.put(key, body);
            return URI.create("http://127.0.0.1:" + port + "/obj/" + key);
        }
    }

    @Test
    void maybeExternalize_returns_null_below_threshold() throws Exception {
        ExternalLocationConfig cfg = ExternalLocationConfig.builder()
                .storage(new MapStorage())
                .thresholdBytes(1L << 20) // 1 MiB
                .urlValidator(ExternalLocationConfig.permissiveValidator())
                .build();
        try (VectorSchemaRoot root = makeRoot(10)) {
            Externalizer.Pointer out = Externalizer.maybeExternalize(root, null, cfg);
            assertNull(out, "small batches stay inline");
        }
    }

    @Test
    void maybeExternalize_produces_pointer_and_uploads() throws Exception {
        ExternalLocationConfig cfg = ExternalLocationConfig.builder()
                .storage(new MapStorage())
                .thresholdBytes(16) // tiny: force externalisation even for small batches
                .urlValidator(ExternalLocationConfig.permissiveValidator())
                .build();
        try (VectorSchemaRoot root = makeRoot(1000)) {
            Externalizer.Pointer ptr = Externalizer.maybeExternalize(root, null, cfg);
            assertNotNull(ptr, "batch above threshold must be externalised");
            try {
                assertEquals(0, ptr.root().getRowCount());
                String url = ptr.customMetadata().get(Metadata.LOCATION);
                assertNotNull(url);
                assertNotNull(ptr.customMetadata().get(Metadata.LOCATION_SHA256));
                assertEquals("external", ptr.customMetadata().get(Metadata.LOCATION_SOURCE));
                assertEquals(1, objects.size());
            } finally {
                ptr.root().close();
            }
        }
    }

    @Test
    void roundtrip_externalize_then_resolve() throws Exception {
        ExternalLocationConfig cfg = ExternalLocationConfig.builder()
                .storage(new MapStorage())
                .thresholdBytes(16)
                .urlValidator(ExternalLocationConfig.permissiveValidator())
                .build();
        try (VectorSchemaRoot root = makeRoot(50)) {
            Externalizer.Pointer ptr = Externalizer.maybeExternalize(root, null, cfg);
            assertNotNull(ptr);

            // Resolve the pointer via the fetcher and check the payload matches.
            LocationResolver resolver = new LocationResolver(cfg);
            LocationResolver.Resolved resolved = resolver.resolve(ptr.customMetadata());
            try {
                assertEquals(50, resolved.root().getRowCount());
                Map<String, Object> row = Marshalling.decodeRow(resolved.root(), null);
                // decodeRow only reads row 0; make sure the round-tripped value matches.
                assertEquals(0L, row.get("v"));
                assertNull(resolved.customMetadata().get(Metadata.LOCATION));
            } finally {
                resolved.root().close();
                ptr.root().close();
            }
        }
    }

    // Helpers ------------------------------------------------------------

    private static VectorSchemaRoot makeRoot(int rows) {
        VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, Allocators.root());
        root.allocateNew();
        org.apache.arrow.vector.BigIntVector v = (org.apache.arrow.vector.BigIntVector) root.getVector(0);
        for (int i = 0; i < rows; i++) v.setSafe(i, i);
        root.setRowCount(rows);
        return root;
    }
}
