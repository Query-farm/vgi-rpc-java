// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.wire;

import com.google.flatbuffers.FlatBufferBuilder;
import org.apache.arrow.flatbuf.KeyValue;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.MetadataVersion;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.util.Map;

/**
 * Arrow IPC stream writer that supports per-batch {@code custom_metadata}.
 * The high-level {@link org.apache.arrow.vector.ipc.ArrowStreamWriter} does not
 * expose custom_metadata, so we construct the Message flatbuffer ourselves.
 */
public final class IpcStreamWriter implements AutoCloseable {

    private static final int IPC_CONTINUATION_TOKEN = MessageSerializer.IPC_CONTINUATION_TOKEN;

    private final WriteChannel out;
    private boolean schemaWritten;
    private boolean closed;

    public IpcStreamWriter(OutputStream raw) {
        this.out = new WriteChannel(Channels.newChannel(raw));
    }

    public void writeSchema(Schema schema) throws IOException {
        if (schemaWritten) throw new IllegalStateException("schema already written");
        MessageSerializer.serialize(out, schema);
        schemaWritten = true;
    }

    /** Convenience: serialise the contents of a {@link VectorSchemaRoot} as a batch. */
    public void writeBatch(VectorSchemaRoot root, Map<String, String> customMetadata) throws IOException {
        if (!schemaWritten) writeSchema(root.getSchema());
        VectorUnloader unloader = new VectorUnloader(root, true,
                NoCompressionCodec.INSTANCE, true);
        try (ArrowRecordBatch batch = unloader.getRecordBatch()) {
            writeBatch(batch, customMetadata);
        }
    }

    /**
     * Write an existing {@link ArrowRecordBatch} message with optional custom metadata.
     * Reproduces the wire layout produced by Arrow's reference Python writer.
     */
    public void writeBatch(ArrowRecordBatch batch, Map<String, String> customMetadata) throws IOException {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int header = batch.writeTo(builder);
        long bodyLength = batch.computeBodyLength();
        int customMetaOffset = customMetadata == null || customMetadata.isEmpty()
                ? 0
                : buildKeyValueVector(builder, customMetadata);
        int messageOffset = Message.createMessage(builder, MetadataVersion.V5,
                MessageHeader.RecordBatch, header, bodyLength, customMetaOffset);
        builder.finish(messageOffset);
        ByteBuffer messageBytes = builder.dataBuffer();

        writeMessageFrame(messageBytes);
        MessageSerializer.writeBatchBuffers(out, batch);
    }

    /** Schema with custom_metadata is currently unsupported by writers in this implementation. */
    public void writeEos() throws IOException {
        ByteBuffer eos = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        eos.putInt(IPC_CONTINUATION_TOKEN);
        eos.putInt(0);
        eos.flip();
        out.write(eos);
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

    /**
     * Write the framing for a Message: continuation token, 4-byte little-endian
     * length, the metadata bytes, then padding to 8 bytes.
     */
    private void writeMessageFrame(ByteBuffer messageBytes) throws IOException {
        int len = messageBytes.remaining();
        int padded = (len + 7) & ~7;
        int padding = padded - len;

        ByteBuffer header = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        header.putInt(IPC_CONTINUATION_TOKEN);
        header.putInt(padded);
        header.flip();
        out.write(header);
        out.write(messageBytes);
        if (padding > 0) {
            out.write(ByteBuffer.allocate(padding));
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        if (!schemaWritten) {
            writeSchema(new Schema(java.util.Collections.emptyList()));
        }
        writeEos();
    }
}
