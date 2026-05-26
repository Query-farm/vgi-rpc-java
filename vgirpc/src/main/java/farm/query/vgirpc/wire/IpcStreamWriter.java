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
    /**
     * Shared {@link WriteChannel}. Once {@link #delegate} is constructed, this
     * field is replaced with the delegate's internal WriteChannel so the
     * per-WriteChannel {@code currentPosition} counter — which drives 8-byte
     * message alignment via {@code align()} — stays consistent across the
     * stock writer's schema/dict emission and our hand-rolled record-batch
     * emission. Two independent WriteChannels on the same underlying byte
     * channel would desync (each starts at position 0), corrupting the
     * stream's alignment padding for record batches that follow dict batches.
     */
    private WriteChannel out;
    private ArrowStreamWriter delegate;
    private Schema declaredSchema;
    private boolean schemaEmittedDirect;
    private boolean closed;

    public IpcStreamWriter(OutputStream raw) {
        this.rawChannel = Channels.newChannel(raw);
        this.out = new WriteChannel(rawChannel);
    }

    /** Write directly to a {@link WritableByteChannel} (e.g. a shared-memory
     *  segment channel), bypassing the {@code Channels.newChannel} adapter's
     *  heap-{@code byte[]} bounce. */
    public IpcStreamWriter(WritableByteChannel channel) {
        this.rawChannel = channel;
        this.out = new WriteChannel(channel);
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
            // ArrowStreamWriter.ensureDictionariesWritten() stashes a *copy* of
            // each dictionary vector in its private previousDictionaries map
            // (used to detect dict changes across batches). Those copies are
            // allocated from the worker's allocator and are only freed by
            // ArrowStreamWriter.close() — which we skip to keep the channel
            // open. Release them here so dict-encoded responses don't leak a
            // dictionary copy per emit.
            releaseDelegateDictionaryCopies(delegate);
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
        // start() writes the schema only. ArrowWriter.writeBatch() normally
        // calls ensureDictionariesWritten next, but we bypass writeBatch
        // (its record-batch path is tied to the constructor's VSR). Trigger
        // the dict-batch emission ourselves so dict-encoded columns survive
        // the round-trip — without this the consumer sees a schema that
        // declares dict<int, utf8> but no preceding dict batch, and
        // pyarrow/Arrow C++ rejects the stream with "IPC stream did not
        // have the expected number of dictionaries at the start".
        delegate.start();
        invokeEnsureDictionariesWritten(delegate);
        declaredSchema = null;
        // Adopt the delegate's WriteChannel so currentPosition (which drives
        // 8-byte message-frame alignment via WriteChannel.align()) stays
        // consistent across the stock writer's schema/dict emission and our
        // hand-rolled record-batch emission. Two independent WriteChannels
        // on the same underlying byte channel would each start at position 0
        // and pad inconsistently.
        this.out = delegateWriteChannel(delegate);
    }

    /**
     * Pull the stock writer's internal {@link WriteChannel} (field
     * {@code ArrowWriter.out}) so that record batches written through us
     * share the delegate's position counter. Reflection is the only path —
     * the field is package-private with no accessor.
     */
    private static WriteChannel delegateWriteChannel(ArrowStreamWriter delegate) {
        try {
            java.lang.reflect.Field f =
                    org.apache.arrow.vector.ipc.ArrowWriter.class.getDeclaredField("out");
            f.setAccessible(true);
            return (WriteChannel) f.get(delegate);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Cannot access ArrowWriter.out reflectively; "
                    + "Arrow Java API likely changed", e);
        }
    }

    /**
     * Invoke {@code ArrowWriter.ensureDictionariesWritten(provider,
     * dictionaryIdsUsed)} reflectively. Stock's {@code writeBatch()} does
     * this between {@code ensureStarted()} and {@code writeRecordBatch()};
     * we replicate that step because we don't go through {@code writeBatch}.
     */
    private static void invokeEnsureDictionariesWritten(ArrowStreamWriter delegate) {
        try {
            Class<?> writerCls = org.apache.arrow.vector.ipc.ArrowWriter.class;
            java.lang.reflect.Field providerField = writerCls.getDeclaredField("dictionaryProvider");
            providerField.setAccessible(true);
            java.lang.reflect.Field idsField = writerCls.getDeclaredField("dictionaryIdsUsed");
            idsField.setAccessible(true);
            java.lang.reflect.Method ensure = writerCls.getDeclaredMethod(
                    "ensureDictionariesWritten", DictionaryProvider.class, java.util.Set.class);
            ensure.setAccessible(true);
            ensure.invoke(delegate, providerField.get(delegate), idsField.get(delegate));
        } catch (ReflectiveOperationException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new RuntimeException("Cannot invoke ArrowWriter.ensureDictionariesWritten; "
                    + "Arrow Java API likely changed", cause);
        }
    }

    /**
     * Close and clear {@code ArrowStreamWriter.previousDictionaries} — the
     * per-dictionary copies the stock writer retains across batches — without
     * closing the writer (which would close the shared channel). The field is
     * private with no accessor, so reflection is the only path; if Arrow's
     * internals change we swallow the error rather than break the write path
     * (worst case reverts to the prior leak, never a crash).
     */
    private static void releaseDelegateDictionaryCopies(ArrowStreamWriter delegate) {
        try {
            java.lang.reflect.Field f =
                    org.apache.arrow.vector.ipc.ArrowStreamWriter.class.getDeclaredField("previousDictionaries");
            f.setAccessible(true);
            Object value = f.get(delegate);
            if (value instanceof Map<?, ?> prev) {
                for (Object v : prev.values()) {
                    if (v instanceof AutoCloseable c) {
                        try {
                            c.close();
                        } catch (Exception ignore) {
                            // best-effort; continue closing the rest
                        }
                    }
                }
                prev.clear();
            }
        } catch (ReflectiveOperationException e) {
            // Arrow API likely changed; leave previousDictionaries to the GC.
        }
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
