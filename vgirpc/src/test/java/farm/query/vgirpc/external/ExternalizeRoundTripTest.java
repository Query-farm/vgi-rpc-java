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
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
                // Exactly the writer's keys. vgi_rpc.location.source is the
                // reader's provenance stamp on a resolved batch; the reference
                // writer never puts it on a pointer.
                assertEquals(java.util.Set.of(Metadata.LOCATION, Metadata.LOCATION_SHA256),
                        ptr.customMetadata().keySet());
                assertEquals(1, objects.size());
            } finally {
                ptr.root().close();
            }
        }
    }

    @Test
    void maybeExternalize_pointer_carries_caller_metadata_plus_location_keys_only() throws Exception {
        ExternalLocationConfig cfg = ExternalLocationConfig.builder()
                .storage(new MapStorage())
                .thresholdBytes(16)
                .urlValidator(ExternalLocationConfig.permissiveValidator())
                .build();
        try (VectorSchemaRoot root = makeRoot(1000)) {
            Externalizer.Pointer ptr = Externalizer.maybeExternalize(root, Map.of("app.key", "v"), cfg);
            assertNotNull(ptr);
            try {
                assertEquals(java.util.Set.of("app.key", Metadata.LOCATION, Metadata.LOCATION_SHA256),
                        ptr.customMetadata().keySet());
                assertNull(ptr.customMetadata().get(Metadata.LOCATION_SOURCE),
                        "a writer must not stamp the reader-side provenance key");
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
                // Provenance is stamped by the reader, with the URL it fetched.
                assertEquals(ptr.customMetadata().get(Metadata.LOCATION),
                        resolved.customMetadata().get(Metadata.LOCATION_SOURCE));
                assertNotNull(resolved.customMetadata().get(Metadata.LOCATION_FETCH));
            } finally {
                resolved.root().close();
                ptr.root().close();
            }
        }
    }

    /**
     * A dictionary-encoded batch whose declared stream schema differs from the
     * root's own schema only in nullability must still upload its
     * dictionaries.
     *
     * <p>That is what a collector root looks like against the method's declared
     * output schema, and both the HTTP stream path and the pipe-family tick
     * loop hand the declared schema to the uploader. The payload used to
     * declare the dictionary and deliver none, so the fetched stream could not
     * be decoded.
     */
    @Test
    void roundtrip_dictionary_batch_whose_schema_differs_only_in_nullability() throws Exception {
        assertDictRoundTrip(dictSchema(false), dictSchema(true));
    }

    @Test
    void roundtrip_dictionary_batch_declared_with_its_own_schema() throws Exception {
        // Control: the equal-schema path.
        assertDictRoundTrip(dictSchema(false), null);
    }

    private void assertDictRoundTrip(Schema encodeSchema, Schema declared) throws Exception {
        ExternalLocationConfig cfg = ExternalLocationConfig.builder()
                .storage(new MapStorage())
                .thresholdBytes(1)
                .urlValidator(ExternalLocationConfig.permissiveValidator())
                .build();
        try (Marshalling.EncodedRow encoded = Marshalling.encodeRowForWire(
                encodeSchema, Map.of("s", "hello", "v", 7L), Allocators.root())) {
            assertNotNull(encoded.provider(), "the fixture must actually dictionary-encode");
            Schema wire = declared != null ? declared : encoded.root().getSchema();
            if (declared != null) {
                assertNotEquals(declared, encoded.root().getSchema(),
                        "fixture must exercise the declared-schema-differs path");
            }
            Externalizer.Pointer ptr = Externalizer.maybeExternalize(
                    encoded.root(), null, cfg, wire, encoded.provider());
            assertNotNull(ptr);
            try (LocationResolver.Resolved resolved =
                         new LocationResolver(cfg).resolve(ptr.customMetadata())) {
                assertEquals(1, resolved.root().getRowCount());
                Map<String, Object> row = Marshalling.decodeRow(resolved.root(), resolved.dictionaries());
                assertEquals("hello", row.get("s"));
                assertEquals(7L, row.get("v"));
            } finally {
                ptr.root().close();
            }
        }
    }

    private static Schema dictSchema(boolean nullableV) {
        return new Schema(List.of(
                new Field("s", new FieldType(false, new ArrowType.Utf8(),
                        new DictionaryEncoding(0L, false, new ArrowType.Int(16, true))), null),
                new Field("v", new FieldType(nullableV, new ArrowType.Int(64, true), null), null)));
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
