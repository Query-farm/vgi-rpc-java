// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.external;

import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamWriter;
import farm.query.vgirpc.wire.Metadata;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Server-side helper that externalises data batches above the configured
 * threshold. The original batch is serialised to a standalone Arrow IPC
 * stream, uploaded via {@link ExternalStorage}, and replaced by a zero-row
 * pointer batch carrying the URL in {@code vgi_rpc.location} custom metadata.
 *
 * <p>Never externalises zero-row batches (logs, errors, finish markers) —
 * these are tiny and carry semantic metadata that must travel inline.</p>
 */
public final class Externalizer {

    private Externalizer() {}

    /** Returned when a batch is externalised — caller owns the pointer root. */
    public record Pointer(VectorSchemaRoot root, Map<String, String> customMetadata) {}

    /**
     * Possibly externalise {@code root}. Returns {@code null} to indicate
     * "keep the original inline" (below threshold or no storage configured).
     * Otherwise the returned pointer root replaces the original; caller must
     * close both the original and the pointer when done.
     */
    public static Pointer maybeExternalize(VectorSchemaRoot root,
                                            Map<String, String> existingMeta,
                                            ExternalLocationConfig config) throws Exception {
        if (config == null || config.storage() == null) return null;
        if (root.getRowCount() == 0) return null;

        long size = 0;
        for (org.apache.arrow.vector.FieldVector v : root.getFieldVectors()) {
            size += v.getBufferSize();
        }
        if (size < config.thresholdBytes()) return null;

        // Serialise the whole batch as a standalone IPC stream (schema + batch + EOS)
        // matching the wire format the fetcher consumes.
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (IpcStreamWriter w = new IpcStreamWriter(bos)) {
            w.writeBatch(root, existingMeta);
            w.writeEos();
        }
        byte[] body = bos.toByteArray();
        byte[] sha256 = MessageDigest.getInstance("SHA-256").digest(body);

        URI url = config.storage().upload(body, /* contentEncoding */ null);

        // Build a zero-row pointer root with the same schema.
        VectorSchemaRoot pointer = VectorSchemaRoot.create(root.getSchema(), Allocators.root());
        pointer.allocateNew();
        pointer.setRowCount(0);

        Map<String, String> pointerMeta = new LinkedHashMap<>();
        if (existingMeta != null) pointerMeta.putAll(existingMeta);
        pointerMeta.put(Metadata.LOCATION, url.toString());
        pointerMeta.put(Metadata.LOCATION_SHA256, HexFormat.of().formatHex(sha256));
        pointerMeta.put(Metadata.LOCATION_SOURCE, "external");

        return new Pointer(pointer, pointerMeta);
    }
}
