// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.external;

import com.github.luben.zstd.Zstd;
import farm.query.vgirpc.AccessLogScope;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamWriter;
import farm.query.vgirpc.wire.Metadata;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

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
     *
     * <p>Throws {@link ExternalizedResponseCapExceededException} — before any
     * upload happens — when the payload would take this response past the
     * {@link ExternalResponseBudget} installed by the transport. Callers that
     * fall back to inline delivery on upload <em>failure</em> must rethrow that
     * type rather than absorb it: it is a refusal, not a failure.
     */
    public static Pointer maybeExternalize(VectorSchemaRoot root,
                                            Map<String, String> existingMeta,
                                            ExternalLocationConfig config) throws Exception {
        return maybeExternalize(root, existingMeta, config, null);
    }

    /**
     * As {@link #maybeExternalize(VectorSchemaRoot, Map, ExternalLocationConfig)},
     * but declaring {@code declaredSchema} on the uploaded stream instead of the
     * root's own schema.
     *
     * <p>Needed because an externalised payload is a <em>standalone</em> IPC
     * stream: it declares its own schema, where an inline batch rides a stream
     * whose schema was declared once, up front, by whatever batch went first. A
     * root whose fields differ from that declared schema only in nullability
     * therefore travels invisibly inline and reads back as a schema mismatch the
     * moment it is externalised. Passing the stream's declared schema here makes
     * the uploaded bytes what inline delivery would have produced — the same
     * schema message, the same record batch buffers.
     *
     * @param root the batch to externalise
     * @param existingMeta custom metadata to carry, or {@code null}
     * @param config the external-location configuration
     * @param declaredSchema the schema the enclosing stream declared, or
     *     {@code null} to use the root's own
     * @return the pointer replacing the batch, or {@code null} to keep it inline
     * @throws Exception if serialisation or the upload fails
     */
    public static Pointer maybeExternalize(VectorSchemaRoot root,
                                            Map<String, String> existingMeta,
                                            ExternalLocationConfig config,
                                            Schema declaredSchema) throws Exception {
        return maybeExternalize(root, existingMeta, config, declaredSchema, null);
    }

    /** Dictionary-aware variant used when an encoded batch is externalized. */
    public static Pointer maybeExternalize(VectorSchemaRoot root,
                                            Map<String, String> existingMeta,
                                            ExternalLocationConfig config,
                                            Schema declaredSchema,
                                            DictionaryProvider dictionaryProvider) throws Exception {
        if (config == null || config.storage() == null) return null;
        if (root.getRowCount() == 0) return null;

        long size = 0;
        for (org.apache.arrow.vector.FieldVector v : root.getFieldVectors()) {
            size += v.getBufferSize();
        }
        if (size < config.thresholdBytes()) return null;

        Schema wireSchema = declaredSchema != null ? declaredSchema : root.getSchema();

        // Serialise the whole batch as a standalone IPC stream (schema + batch + EOS)
        // matching the wire format the fetcher consumes.
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (IpcStreamWriter w = new IpcStreamWriter(bos)) {
            if (wireSchema.equals(root.getSchema())) {
                w.writeBatch(root, existingMeta, dictionaryProvider);
            } else {
                // Declare the enclosing stream's schema and write the root's
                // buffers unchanged — byte-for-byte what inline delivery emits.
                w.writeSchema(wireSchema);
                VectorUnloader unloader = new VectorUnloader(root, true, NoCompressionCodec.INSTANCE, true);
                try (ArrowRecordBatch rb = unloader.getRecordBatch()) {
                    w.writeBatch(rb, existingMeta);
                }
            }
            w.writeEos();
        }
        byte[] body = bos.toByteArray();
        // SHA-256 is computed over the *raw* (pre-compression) IPC bytes so
        // both sides can verify the payload after the fetcher transparently
        // decompresses on download — matches the Python reference.
        byte[] sha256 = MessageDigest.getInstance("SHA-256").digest(body);

        String contentEncoding = null;
        byte[] uploadBody = body;
        ExternalLocationConfig.Compression comp = config.compression();
        if (comp != null && "zstd".equalsIgnoreCase(comp.algorithm())) {
            uploadBody = Zstd.compress(body, comp.level());
            contentEncoding = "zstd";
        }

        // Pre-flight the operator cap BEFORE the upload. Enforcing it afterwards
        // would still have spent the egress: bytes already in external storage
        // cannot be un-uploaded, which is exactly why this cap — unlike the wire
        // cap — has no soft-for-producers escape. Throws on overshoot.
        ExternalResponseBudget.reserve(uploadBody.length);
        // Counted here rather than at the call sites: this is the one place every
        // externalised payload passes through, so a new upload path cannot drift
        // from the total. These bytes never appear in the HTTP body — only the
        // pointer batch below does — so nothing at the transport can see them.
        AccessLogScope.countExternalized(uploadBody.length);
        URI url = config.storage().upload(uploadBody, contentEncoding);

        // Build a zero-row pointer root with the same schema the payload declares.
        VectorSchemaRoot pointer = VectorSchemaRoot.create(wireSchema, Allocators.root());
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
