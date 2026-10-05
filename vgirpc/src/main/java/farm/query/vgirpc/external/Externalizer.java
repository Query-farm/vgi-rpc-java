// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.external;

import com.github.luben.zstd.Zstd;
import farm.query.vgirpc.AccessLogScope;
import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamWriter;
import farm.query.vgirpc.wire.Wire;
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
import java.util.Objects;

/**
 * Server-side helper that externalises data batches above the configured
 * threshold. The original batch is serialised to a standalone Arrow IPC
 * stream, uploaded via {@link ExternalStorage}, and replaced by a zero-row
 * pointer batch carrying the URL in {@code vgi_rpc.location} custom metadata.
 *
 * <p>Never externalises zero-row batches (logs, errors, finish markers) —
 * these are tiny and carry semantic metadata that must travel inline.</p>
 *
 * <p>{@link #publishExternal} is the publish-once counterpart: it uploads a
 * unary result through the same serialise/hash/compress/upload path and
 * returns an {@link ExternalRef} a method can hand back on every later call.</p>
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

        byte[] body = serializeSingleBatch(root, existingMeta, wireSchema, dictionaryProvider);
        // The per-call path charges the response: the cap pre-flight and the
        // access-log egress counter both run between compression and upload.
        Uploaded up = uploadIpcBytes(body, config.storage(), config.compression(), true);
        URI url = up.url();

        // Build a zero-row pointer root declaring the payload's schema. Via
        // Wire.zeroRootFor, not VectorSchemaRoot.create: for a dictionary-encoded
        // column the latter builds the *value* vector while the writer declares
        // the *index* type, and the batch goes out with more buffers than its own
        // schema accounts for. PyArrow ignores the surplus; Arrow Java refuses the
        // batch outright, so the malformed pointer is invisible to every
        // cross-language test and fatal to a Java client reading a Java server.
        VectorSchemaRoot pointer = Wire.zeroRootFor(wireSchema);

        Map<String, String> pointerMeta = new LinkedHashMap<>();
        if (existingMeta != null) pointerMeta.putAll(existingMeta);
        pointerMeta.put(Metadata.LOCATION, url.toString());
        pointerMeta.put(Metadata.LOCATION_SHA256, up.sha256());
        pointerMeta.put(Metadata.LOCATION_SOURCE, "external");

        return new Pointer(pointer, pointerMeta);
    }

    // ------------------------------------------------------------------
    // Pre-published references
    // ------------------------------------------------------------------

    /**
     * Publish a unary result batch once and return a reusable reference.
     *
     * <p>Serialises {@code batch} exactly as the per-call externaliser does (a
     * standalone IPC stream of its schema plus this one batch), hashes the raw
     * bytes, compresses when {@code compression} is given (same codec and
     * {@code Content-Encoding} handling), and calls
     * {@link ExternalStorage#upload} once. Cache the returned
     * {@link ExternalRef} and answer later calls with
     * {@link farm.query.vgirpc.CallContext#respondWithExternalRef(ExternalRef)};
     * the server writes the pointer directly.</p>
     *
     * <p>Unlike {@link #maybeExternalize}, this is not a per-response upload:
     * it is neither charged to the calling request's
     * {@code max_externalized_response_bytes} budget nor counted as that
     * request's externalised egress.</p>
     *
     * @param batch the 1-row result batch, built against the method's result
     *     schema (see {@link #publishExternal(Schema, Object, ExternalStorage,
     *     ExternalLocationConfig.Compression, boolean)} to build it from a value)
     * @param storage storage backend to upload to
     * @param compression compression applied before upload (pass the server's
     *     {@link ExternalLocationConfig#compression()} to match it), or
     *     {@code null} to upload raw
     * @param includeSha256 when {@code false} the ref carries no digest, so
     *     clients skip the content check
     * @return a ref naming the uploaded object
     * @throws IllegalArgumentException if {@code batch} does not have exactly one row
     * @throws Exception if serialisation or the upload fails
     */
    public static ExternalRef publishExternal(VectorSchemaRoot batch,
                                              ExternalStorage storage,
                                              ExternalLocationConfig.Compression compression,
                                              boolean includeSha256) throws Exception {
        return publishExternal(batch, null, storage, compression, includeSha256);
    }

    /**
     * {@link #publishExternal(VectorSchemaRoot, ExternalStorage,
     * ExternalLocationConfig.Compression, boolean)} with a digest.
     *
     * @param batch the 1-row result batch
     * @param storage storage backend to upload to
     * @param compression compression applied before upload, or {@code null}
     * @return a ref naming the uploaded object, carrying its SHA-256
     * @throws Exception if serialisation or the upload fails
     */
    public static ExternalRef publishExternal(VectorSchemaRoot batch,
                                              ExternalStorage storage,
                                              ExternalLocationConfig.Compression compression)
            throws Exception {
        return publishExternal(batch, null, storage, compression, true);
    }

    /**
     * Dictionary-aware variant of {@link #publishExternal(VectorSchemaRoot,
     * ExternalStorage, ExternalLocationConfig.Compression, boolean)} for a
     * batch with dictionary-encoded (enum) columns.
     *
     * @param batch the 1-row result batch
     * @param dictionaryProvider dictionaries for encoded columns, or {@code null}
     * @param storage storage backend to upload to
     * @param compression compression applied before upload, or {@code null}
     * @param includeSha256 whether the ref carries the digest
     * @return a ref naming the uploaded object
     * @throws Exception if serialisation or the upload fails
     */
    public static ExternalRef publishExternal(VectorSchemaRoot batch,
                                              DictionaryProvider dictionaryProvider,
                                              ExternalStorage storage,
                                              ExternalLocationConfig.Compression compression,
                                              boolean includeSha256) throws Exception {
        Objects.requireNonNull(batch, "batch");
        Objects.requireNonNull(storage, "storage");
        if (batch.getRowCount() != 1) {
            throw new IllegalArgumentException(
                    "publishExternal expects a 1-row result batch, got " + batch.getRowCount() + " rows");
        }
        byte[] body = serializeSingleBatch(batch, null, batch.getSchema(), dictionaryProvider);
        Uploaded up = uploadIpcBytes(body, storage, compression, false);
        return new ExternalRef(up.url().toString(), includeSha256 ? up.sha256() : null);
    }

    /**
     * Build the result batch {@code {result: [value]}} against
     * {@code resultSchema} and publish it.
     *
     * <p>Obtain the schema from the service interface, e.g.
     * {@code ServiceIntrospector.describe(MyService.class).get("catalog").resultSchema()}.
     * {@code value} is marshalled the way the unary dispatcher marshals a
     * returned value.</p>
     *
     * @param resultSchema the method's single-column result schema
     * @param value the result value
     * @param storage storage backend to upload to
     * @param compression compression applied before upload, or {@code null}
     * @param includeSha256 whether the ref carries the digest
     * @return a ref naming the uploaded object
     * @throws IllegalArgumentException if {@code resultSchema} does not have exactly one field
     * @throws Exception if marshalling, serialisation or the upload fails
     */
    public static ExternalRef publishExternal(Schema resultSchema,
                                              Object value,
                                              ExternalStorage storage,
                                              ExternalLocationConfig.Compression compression,
                                              boolean includeSha256) throws Exception {
        Objects.requireNonNull(resultSchema, "resultSchema");
        if (resultSchema.getFields().size() != 1) {
            throw new IllegalArgumentException(
                    "publishExternal needs a single-column result schema, got " + resultSchema);
        }
        Map<String, Object> row = new LinkedHashMap<>();
        row.put(resultSchema.getFields().get(0).getName(), value);
        try (Marshalling.EncodedRow encoded =
                     Marshalling.encodeRowForWire(resultSchema, row, Allocators.root())) {
            return publishExternal(encoded.root(), encoded.provider(), storage, compression, includeSha256);
        }
    }

    // ------------------------------------------------------------------
    // Shared serialise / hash / compress / upload
    // ------------------------------------------------------------------

    /** Result of {@link #uploadIpcBytes}. */
    private record Uploaded(URI url, String sha256) {}

    /**
     * Serialise {@code root} as a complete standalone IPC stream (schema +
     * one batch + EOS) declaring {@code wireSchema}, matching the wire format
     * the fetcher consumes.
     */
    private static byte[] serializeSingleBatch(VectorSchemaRoot root,
                                               Map<String, String> meta,
                                               Schema wireSchema,
                                               DictionaryProvider dictionaryProvider) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (IpcStreamWriter w = new IpcStreamWriter(bos)) {
            if (wireSchema.equals(root.getSchema())) {
                w.writeBatch(root, meta, dictionaryProvider);
            } else {
                // Declare the enclosing stream's schema and write the root's
                // buffers unchanged — byte-for-byte what inline delivery emits.
                w.writeSchema(wireSchema);
                VectorUnloader unloader = new VectorUnloader(root, true, NoCompressionCodec.INSTANCE, true);
                try (ArrowRecordBatch rb = unloader.getRecordBatch()) {
                    w.writeBatch(rb, meta);
                }
            }
            w.writeEos();
        }
        return bos.toByteArray();
    }

    /**
     * Hash, optionally compress, and upload one serialised IPC stream — the
     * single choke point shared by every server-side externalisation path
     * (per-call batches and {@link #publishExternal}), so the bytes a pointer
     * names are always produced the same way.
     *
     * @param chargeResponse {@code true} for a per-response upload: pre-flight
     *     the {@link ExternalResponseBudget} and count the bytes as the
     *     request's externalised egress before uploading
     */
    private static Uploaded uploadIpcBytes(byte[] body,
                                           ExternalStorage storage,
                                           ExternalLocationConfig.Compression comp,
                                           boolean chargeResponse) throws Exception {
        // SHA-256 is computed over the *raw* (pre-compression) IPC bytes so
        // both sides can verify the payload after the fetcher transparently
        // decompresses on download — matches the Python reference.
        byte[] sha256 = MessageDigest.getInstance("SHA-256").digest(body);

        String contentEncoding = null;
        byte[] uploadBody = body;
        if (comp != null && "zstd".equalsIgnoreCase(comp.algorithm())) {
            uploadBody = Zstd.compress(body, comp.level());
            contentEncoding = "zstd";
        }

        if (chargeResponse) {
            // Pre-flight the operator cap BEFORE the upload. Enforcing it afterwards
            // would still have spent the egress: bytes already in external storage
            // cannot be un-uploaded, which is exactly why this cap — unlike the wire
            // cap — has no soft-for-producers escape. Throws on overshoot.
            ExternalResponseBudget.reserve(uploadBody.length);
            // Counted here rather than at the call sites: this is the one place every
            // externalised payload passes through, so a new upload path cannot drift
            // from the total. These bytes never appear in the HTTP body — only the
            // pointer batch does — so nothing at the transport can see them.
            AccessLogScope.countExternalized(uploadBody.length);
        }
        URI url = storage.upload(uploadBody, contentEncoding);
        return new Uploaded(url, HexFormat.of().formatHex(sha256));
    }
}
