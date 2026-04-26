// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.wire;

import org.apache.arrow.flatbuf.DictionaryBatch;
import org.apache.arrow.flatbuf.KeyValue;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Arrow IPC stream reader that surfaces per-batch {@code custom_metadata}
 * (which the high-level {@link org.apache.arrow.vector.ipc.ArrowStreamReader}
 * does not expose).
 */
public final class IpcStreamReader implements AutoCloseable {

    private static final int IPC_CONTINUATION_TOKEN = MessageSerializer.IPC_CONTINUATION_TOKEN;

    private final ReadChannel in;
    private final BufferAllocator allocator;
    private Schema schema;
    private VectorSchemaRoot root;
    private VectorLoader loader;
    private boolean closed;
    private final java.util.Map<Long, Dictionary> dictionaries = new java.util.HashMap<>();
    private final DictionaryProvider provider = new DictionaryProvider() {
        @Override public Dictionary lookup(long id) { return dictionaries.get(id); }
        @Override public java.util.Set<Long> getDictionaryIds() { return dictionaries.keySet(); }
    };

    public IpcStreamReader(InputStream raw, BufferAllocator allocator) {
        this.in = new ReadChannel(Channels.newChannel(raw));
        this.allocator = allocator;
    }

    /** The original wire schema of this stream (preserves dictionary metadata). */
    public Schema schema() throws IOException {
        ensureSchema();
        return schema;
    }

    /** Same as {@link #schema()}; named to make intent explicit at call sites. */
    public Schema wireSchema() throws IOException { return schema(); }

    /**
     * The root vector for the current batch. Mutated by each successful
     * {@link #readNextBatch()}.
     */
    public VectorSchemaRoot root() throws IOException {
        ensureSchema();
        return root;
    }

    /**
     * Read the next batch and return its custom_metadata (possibly empty).
     * Returns {@code null} when the stream is exhausted (EOS marker received).
     */
    public Map<String, String> readNextBatch() throws IOException {
        ensureSchema();
        Message msg = readMessage();
        if (msg == null) return null;
        if (msg.headerType() == MessageHeader.RecordBatch) {
            RecordBatch rb = (RecordBatch) msg.header(new RecordBatch());
            long bodyLen = msg.bodyLength();
            ArrowBuf bodyBuf = readMessageBody(bodyLen);
            try (ArrowRecordBatch arb = MessageSerializer.deserializeRecordBatch(rb, bodyBuf)) {
                if (loader == null) loader = new VectorLoader(root);
                loader.load(arb);
            }
            return readCustomMetadata(msg);
        }
        if (msg.headerType() == MessageHeader.DictionaryBatch) {
            DictionaryBatch db = (DictionaryBatch) msg.header(new DictionaryBatch());
            long bodyLen = msg.bodyLength();
            ArrowBuf bodyBuf = readMessageBody(bodyLen);
            try (ArrowDictionaryBatch adb = MessageSerializer.deserializeDictionaryBatch(msg, bodyBuf)) {
                installDictionary(adb);
            }
            return readNextBatch();
        }
        // Unknown header: skip body and recurse
        if (msg.bodyLength() > 0) {
            ByteBuffer skip = ByteBuffer.allocate((int) msg.bodyLength());
            in.readFully(skip);
        }
        return readNextBatch();
    }

    /** True if more data may be available; otherwise EOS / I/O end has been reached. */
    public boolean hasMore() {
        return !eos;
    }

    /** Consume any remaining batches until the EOS marker. */
    public void drain() throws IOException {
        while (!eos && readNextBatch() != null) {
            // discard any additional batches
        }
    }

    private boolean eos = false;

    private void ensureSchema() throws IOException {
        if (schema != null) return;
        Message msg = readMessage();
        if (msg == null || msg.headerType() != MessageHeader.Schema) {
            throw new IOException("Expected schema as first IPC message");
        }
        Schema raw = MessageSerializer.deserializeSchema(msg);
        // Materialise dict-encoded fields as their index type vectors anywhere
        // they appear (top-level OR nested inside list/map/struct), so the
        // VectorSchemaRoot's buffer count matches the wire batch. We resolve
        // values via the dictionary provider during decode. The original
        // schema (with DictionaryEncoding) is kept on `schema` for callers.
        java.util.List<Field> rootFields = new java.util.ArrayList<>();
        for (Field f : raw.getFields()) {
            rootFields.add(stripDictEncoding(f));
        }
        Schema rootSchema = new Schema(rootFields, raw.getCustomMetadata());
        this.schema = raw;
        this.root = VectorSchemaRoot.create(rootSchema, allocator);
    }

    private static Field stripDictEncoding(Field f) {
        DictionaryEncoding de = f.getDictionary();
        if (de != null) {
            return new Field(f.getName(),
                    new FieldType(f.isNullable(), de.getIndexType(), null),
                    java.util.Collections.emptyList());
        }
        java.util.List<Field> kids = f.getChildren();
        if (kids.isEmpty()) return f;
        java.util.List<Field> rewritten = new java.util.ArrayList<>(kids.size());
        boolean changed = false;
        for (Field c : kids) {
            Field nc = stripDictEncoding(c);
            if (nc != c) changed = true;
            rewritten.add(nc);
        }
        if (!changed) return f;
        return new Field(f.getName(), f.getFieldType(), rewritten);
    }

    private static Map<String, String> readCustomMetadata(Message msg) {
        int n = msg.customMetadataLength();
        if (n == 0) return Map.of();
        Map<String, String> map = new LinkedHashMap<>(n);
        for (int i = 0; i < n; i++) {
            KeyValue kv = msg.customMetadata(i);
            map.put(kv.key(), kv.value());
        }
        return map;
    }

    /** Reads one message frame, returning null on EOS. */
    private Message readMessage() throws IOException {
        ByteBuffer header = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        long got = in.readFully(header);
        if (got < 8) { eos = true; return null; }
        header.flip();
        int continuation = header.getInt();
        int length;
        if (continuation == IPC_CONTINUATION_TOKEN) {
            length = header.getInt();
        } else {
            // legacy 4-byte length without continuation
            length = continuation;
        }
        if (length == 0) { eos = true; return null; }
        ByteBuffer body = ByteBuffer.allocate(length);
        long n = in.readFully(body);
        if (n < length) throw new IOException("truncated IPC message metadata");
        body.flip();
        return Message.getRootAsMessage(body);
    }

    private ArrowBuf readMessageBody(long length) throws IOException {
        ArrowBuf buf = allocator.buffer(length);
        try {
            long n = in.readFully(buf, length);
            if (n < length) throw new IOException("truncated IPC message body");
            return buf;
        } catch (Throwable t) {
            buf.close();
            throw t;
        }
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        for (Dictionary d : dictionaries.values()) d.getVector().close();
        dictionaries.clear();
        if (root != null) root.close();
    }

    /** Decode a dictionary batch and install/replace the vector for its dict id. */
    private void installDictionary(ArrowDictionaryBatch adb) {
        long id = adb.getDictionaryId();
        // Locate a field in the schema that uses this dict id to learn the value type.
        Field referencingField = findFieldByDictId(id);
        if (referencingField == null) return;
        Field valueField = new Field(referencingField.getName(),
                new FieldType(true, referencingField.getType(), null),
                referencingField.getChildren());
        FieldVector dictVec = valueField.createVector(allocator);
        VectorSchemaRoot dictRoot = new VectorSchemaRoot(java.util.List.of(valueField), java.util.List.of(dictVec));
        VectorLoader dictLoader = new VectorLoader(dictRoot);
        dictLoader.load(adb.getDictionary());
        Dictionary existing = dictionaries.put(id,
                new Dictionary(dictVec, referencingField.getDictionary()));
        if (existing != null) existing.getVector().close();
    }

    private Field findFieldByDictId(long id) {
        if (schema == null) return null;
        for (Field f : schema.getFields()) {
            Field hit = findFieldByDictIdRecursive(f, id);
            if (hit != null) return hit;
        }
        return null;
    }

    private static Field findFieldByDictIdRecursive(Field f, long id) {
        DictionaryEncoding de = f.getDictionary();
        if (de != null && de.getId() == id) return f;
        for (Field c : f.getChildren()) {
            Field hit = findFieldByDictIdRecursive(c, id);
            if (hit != null) return hit;
        }
        return null;
    }

    /** Resolve a dictionary-encoded vector index to its underlying value (e.g. enum name). */
    public Object resolveDictValue(long dictId, int index) {
        Dictionary d = dictionaries.get(dictId);
        if (d == null) return null;
        FieldVector v = d.getVector();
        if (v instanceof org.apache.arrow.vector.VarCharVector vc) {
            return new String(vc.get(index), java.nio.charset.StandardCharsets.UTF_8);
        }
        return null;
    }

    /** Expose the dictionary provider so {@link farm.query.vgirpc.marshal.Marshalling} can resolve indices. */
    public DictionaryProvider dictionaryProvider() { return provider; }
}
