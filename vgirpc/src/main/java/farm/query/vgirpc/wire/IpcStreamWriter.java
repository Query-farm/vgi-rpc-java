// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.wire;

import com.google.flatbuffers.FlatBufferBuilder;
import org.apache.arrow.flatbuf.KeyValue;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.MetadataVersion;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Map;

/**
 * Arrow IPC stream writer that supports per-batch {@code custom_metadata}.
 *
 * <p>Stock {@link ArrowStreamWriter} handles schema messages, dictionary
 * batches and the EOS marker for free, but its {@code writeBatch()} method
 * is tied to the {@link VectorSchemaRoot} given at construction time — VGI
 * emits multiple distinct VSRs into one stream (each batch from a producer
 * may be a freshly-allocated root), so we use the stock writer only for
 * {@link ArrowStreamWriter#start()} (schema + dict batches via
 * {@code ensureDictionariesWritten}) and emit each record batch ourselves
 * with optional {@code custom_metadata} on the Message header.</p>
 */
public final class IpcStreamWriter implements AutoCloseable {

    private final WritableByteChannel rawChannel;
    private final WriteChannel out;
    private ArrowStreamWriter delegate;
    private Schema declaredSchema;
    private boolean schemaEmittedDirect;
    private boolean closed;

    public IpcStreamWriter(OutputStream raw) {
        this.rawChannel = Channels.newChannel(raw);
        this.out = new WriteChannel(rawChannel);
    }

    /**
     * Record the schema this stream will carry. Actual emission is deferred:
     * if a {@link #writeBatch(VectorSchemaRoot, Map, DictionaryProvider)}
     * follows, the underlying {@link ArrowStreamWriter} emits the batch's
     * own schema (which must match the declared schema). If only an
     * {@link #writeBatch(ArrowRecordBatch, Map)} follows, this schema is
     * emitted just-in-time. If neither follows, {@link #close()} emits the
     * declared schema as a zero-batch stream.
     */
    public void writeSchema(Schema schema) throws IOException {
        if (delegate != null) throw new IllegalStateException("batches already emitted");
        if (declaredSchema != null) throw new IllegalStateException("schema already declared");
        declaredSchema = schema;
    }

    public void writeBatch(VectorSchemaRoot root, Map<String, String> customMetadata) throws IOException {
        writeBatch(root, customMetadata, null);
    }

    /**
     * Write a record batch. On the first call: emits the schema and any
     * required dictionary batches via the stock {@link ArrowStreamWriter}
     * (which handles nested fields, idempotent dict emission, and provider
     * lookups). Subsequent calls bypass the stock writer's per-batch path
     * (which would re-unload the constructor's VSR and ignore {@code root})
     * and emit the caller's batch directly.
     */
    public void writeBatch(VectorSchemaRoot root, Map<String, String> customMetadata,
                            DictionaryProvider provider) throws IOException {
        ensureSchemaAndDictsStarted(root, provider);
        VectorUnloader unloader = unloaderFor(root);
        try (ArrowRecordBatch batch = unloader.getRecordBatch()) {
            writeRecordBatch(batch, customMetadata);
        }
    }

    /**
     * Write an existing {@link ArrowRecordBatch}. The schema is emitted
     * just-in-time if not already; no dictionary batches are emitted here
     * (callers using this path do not have dict-encoded fields, since they
     * pass a pre-built record batch with no provider context).
     */
    public void writeBatch(ArrowRecordBatch batch, Map<String, String> customMetadata) throws IOException {
        if (delegate == null && !schemaEmittedDirect) {
            if (declaredSchema == null) {
                throw new IllegalStateException("writeSchema(Schema) must be called before writeBatch(ArrowRecordBatch, ...)");
            }
            MessageSerializer.serialize(out, declaredSchema);
            schemaEmittedDirect = true;
            declaredSchema = null;
        }
        writeRecordBatch(batch, customMetadata);
    }

    /** Write the EOS marker. Idempotent on close. */
    public void writeEos() throws IOException {
        ArrowStreamWriter.writeEndOfStream(out, IpcOption.DEFAULT);
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        if (delegate != null) {
            // Write EOS via the stock writer's end(), but do NOT call
            // delegate.close() — that would close the underlying channel
            // (the per-connection socket), but in VGI the channel persists
            // across many RPCs on the same connection.
            delegate.end();
            return;
        }
        if (!schemaEmittedDirect) {
            Schema s = declaredSchema != null ? declaredSchema : new Schema(java.util.Collections.emptyList());
            MessageSerializer.serialize(out, s);
        }
        writeEos();
    }

    /**
     * On the first batch, construct the stock {@link ArrowStreamWriter} and
     * call {@code start()} so it emits the schema and any dictionary
     * batches. We then take over record-batch emission ourselves; the stock
     * writer's record-batch path is tied to {@code root} and would ignore
     * subsequent VSRs.
     */
    private void ensureSchemaAndDictsStarted(VectorSchemaRoot root, DictionaryProvider provider) throws IOException {
        if (delegate != null) return;
        if (schemaEmittedDirect) {
            throw new IllegalStateException("schema already emitted via writeBatch(ArrowRecordBatch, ...)");
        }
        DictionaryProvider p = provider != null ? provider
                : new org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider();
        delegate = new ArrowStreamWriter(root, p, rawChannel);
        delegate.start();
        declaredSchema = null;
    }

    private static VectorUnloader unloaderFor(VectorSchemaRoot root) {
        return new VectorUnloader(root, /*includeNullCount*/ true,
                NoCompressionCodec.INSTANCE, /*alignBuffers*/ true);
    }

    private void writeRecordBatch(ArrowRecordBatch batch, Map<String, String> customMetadata) throws IOException {
        if (customMetadata == null || customMetadata.isEmpty()) {
            MessageSerializer.serialize(out, batch);
        } else {
            writeRecordBatchWithMetadata(out, batch, customMetadata);
        }
    }

    /**
     * Build a {@link Message} flatbuffer for {@code batch} with
     * {@code custom_metadata} attached and emit it (header frame + body).
     * Mirrors what {@code MessageSerializer.serialize(WriteChannel,
     * ArrowRecordBatch)} does internally; the only delta is the extra
     * {@code custom_metadata} vector on the Message.
     */
    private static void writeRecordBatchWithMetadata(WriteChannel out, ArrowRecordBatch batch,
                                                      Map<String, String> customMetadata) throws IOException {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int header = batch.writeTo(builder);
        long bodyLength = batch.computeBodyLength();
        int customMetaOffset = buildKeyValueVector(builder, customMetadata);
        int messageOffset = Message.createMessage(builder, MetadataVersion.V5,
                MessageHeader.RecordBatch, header, bodyLength, customMetaOffset);
        builder.finish(messageOffset);
        ByteBuffer messageBytes = builder.dataBuffer();

        MessageSerializer.writeMessageBuffer(out, messageBytes.remaining(), messageBytes);
        MessageSerializer.writeBatchBuffers(out, batch);
    }

    private static int buildKeyValueVector(FlatBufferBuilder b, Map<String, String> meta) {
        int[] entries = new int[meta.size()];
        int i = 0;
        for (Map.Entry<String, String> e : meta.entrySet()) {
            int key = b.createString(e.getKey());
            int val = b.createString(e.getValue());
            entries[i++] = KeyValue.createKeyValue(b, key, val);
        }
        return Message.createCustomMetadataVector(b, entries);
    }
}
