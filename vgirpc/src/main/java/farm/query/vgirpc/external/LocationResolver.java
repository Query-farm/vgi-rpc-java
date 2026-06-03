// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.external;

import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import farm.query.vgirpc.wire.Metadata;
import farm.query.vgirpc.wire.Wire;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Detect and transparently resolve external-location pointer batches.
 *
 * <p>A pointer batch is a zero-row batch whose custom metadata carries
 * {@code vgi_rpc.location}. On resolution the fetcher downloads the IPC bytes
 * from the URL, opens them as an Arrow stream, discards any log/error batches
 * (log dispatch is the caller's responsibility), and returns the (single) data
 * batch as a fresh {@link VectorSchemaRoot} plus metadata with the location
 * key stripped.</p>
 */
public final class LocationResolver {

    private final ExternalLocationConfig config;
    private final ExternalFetcher fetcher;

    public LocationResolver(ExternalLocationConfig config) {
        this.config = config;
        this.fetcher = new ExternalFetcher(config);
    }

    public ExternalLocationConfig config() { return config; }

    /** True iff the metadata describes an external-location pointer. */
    public static boolean isPointer(int rowCount, Map<String, String> meta) {
        return rowCount == 0 && meta != null && meta.get(Metadata.LOCATION) != null
                && meta.get(Metadata.LOG_LEVEL) == null;
    }

    /**
     * Resolve a pointer batch to its underlying data batch. Ownership of the
     * returned root transfers to the caller (must close). {@code metaOut} is
     * populated with the merged custom metadata minus {@code vgi_rpc.location}
     * and {@code vgi_rpc.location.sha256}; it may be null.
     */
    public Resolved resolve(Map<String, String> pointerMeta) throws Exception {
        String url = pointerMeta.get(Metadata.LOCATION);
        if (url == null) throw new IllegalArgumentException("pointer batch missing vgi_rpc.location");
        String sha = pointerMeta.get(Metadata.LOCATION_SHA256);
        byte[] body = fetcher.fetch(URI.create(url), sha);

        // Open the fetched stream, advance past any log/error batches, and take
        // the first data batch. We copy it into a caller-owned root because the
        // IpcStreamReader's root is recycled on close.
        try (IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(body), Allocators.root())) {
            while (true) {
                Map<String, String> md = r.readNextBatch();
                if (md == null) throw new java.io.IOException("external stream contained no data batch");
                VectorSchemaRoot root = r.root();
                Wire.BatchKind kind = Wire.classify(root.getRowCount(), md);
                if (kind == Wire.BatchKind.LOG) continue;          // external-stream logs are advisory
                if (kind == Wire.BatchKind.ERROR) throw Wire.errorFromMetadata(md);
                VectorSchemaRoot copy = copyRoot(root);
                Map<String, String> merged = new LinkedHashMap<>(pointerMeta);
                merged.remove(Metadata.LOCATION);
                merged.remove(Metadata.LOCATION_SHA256);
                if (md != null) merged.putAll(md);
                return new Resolved(copy, merged);
            }
        }
    }

    private static VectorSchemaRoot copyRoot(VectorSchemaRoot src) {
        VectorSchemaRoot dst = VectorSchemaRoot.create(src.getSchema(), Allocators.root());
        dst.allocateNew();
        int rows = src.getRowCount();
        for (int c = 0; c < src.getSchema().getFields().size(); c++) {
            org.apache.arrow.vector.FieldVector sv = src.getVector(c);
            org.apache.arrow.vector.FieldVector dv = dst.getVector(c);
            for (int r = 0; r < rows; r++) dv.copyFromSafe(r, r, sv);
        }
        dst.setRowCount(rows);
        return dst;
    }

    /**
     * The outcome of resolving an external-location pointer batch: the fetched,
     * materialised {@link VectorSchemaRoot} and the custom metadata that should
     * accompany it downstream.
     */
    public record Resolved(VectorSchemaRoot root, Map<String, String> customMetadata) {}
}
