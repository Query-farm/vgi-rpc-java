// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.wire;

import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageChannelReader;
import org.apache.arrow.vector.ipc.message.VgiMessageReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.validate.ValidateVectorBufferVisitor;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ReadableByteChannel;
import java.util.List;
import java.util.Map;

/**
 * Arrow IPC stream reader that surfaces per-batch {@code custom_metadata}
 * (which the stock {@link ArrowStreamReader} does not expose).
 *
 * <p>Implementation: {@link VgiMessageReader} captures each message's
 * custom_metadata as it flows through (and refuses an unholdable body
 * without desyncing the stream), and we subclass
 * {@link ArrowStreamReader} so the original (wire-format) schema can be
 * recovered before {@code ArrowReader.initialize} converts it to memory
 * format. Record and dictionary batches are structurally validated against
 * their original field-node lengths before Arrow propagates the root row count
 * into the loaded vectors. Dictionary tracking, framing and EOS use the stock reader.</p>
 */
public final class IpcStreamReader implements AutoCloseable {

    private final BufferAllocator allocator;
    private final VgiMessageReader messageReader;
    private final StreamReaderImpl inner;
    private boolean eos = false;
    private boolean schemaInitialized = false;

    /**
     * Read an Arrow IPC stream from an {@link InputStream}.
     *
     * @param raw the source byte stream
     * @param allocator allocator for the decoded vectors
     */
    public IpcStreamReader(InputStream raw, BufferAllocator allocator) {
        this(StreamChannels.readable(raw), allocator);
    }

    /** Read directly from a {@link ReadableByteChannel} (e.g. a shared-memory
     *  segment channel), bypassing the {@code Channels.newChannel} adapter's
     *  heap-{@code byte[]} bounce. */
    public IpcStreamReader(ReadableByteChannel channel, BufferAllocator allocator) {
        this.allocator = allocator;
        ReadChannel rc = new ReadChannel(channel);
        this.messageReader = new VgiMessageReader(rc, allocator);
        this.inner = new StreamReaderImpl(messageReader, allocator);
    }

    /** Original wire-format schema (preserves {@code DictionaryEncoding} on value-typed fields). */
    public Schema schema() throws IOException {
        ensureSchema();
        return inner.wireSchema;
    }

    /** Same as {@link #schema()}; named to make intent explicit at call sites. */
    public Schema wireSchema() throws IOException { return schema(); }

    /**
     * The root vector for the current batch — mutated by each
     * {@link #readNextBatch()}. Note this VSR uses the <em>memory-format</em>
     * schema (dict-encoded fields are stored as their index-type vectors);
     * use {@link #schema()} when you need the wire-format schema to round-trip
     * back to a downstream writer.
     */
    public VectorSchemaRoot root() throws IOException {
        ensureSchema();
        return inner.getVectorSchemaRoot();
    }

    /**
     * Read the next batch and return its custom_metadata (possibly empty).
     * Returns {@code null} when the stream is exhausted (EOS marker received).
     */
    public Map<String, String> readNextBatch() throws IOException {
        ensureSchema();
        if (!inner.loadNextBatch()) {
            eos = true;
            return null;
        }
        return messageReader.lastCustomMetadata();
    }

    /** True if more data may be available; otherwise EOS / I/O end has been reached. */
    public boolean hasMore() { return !eos; }

    /** Consume any remaining batches until the EOS marker. */
    public void drain() throws IOException {
        while (!eos && readNextBatch() != null) {
            // discard
        }
    }

    /**
     * Release reader buffers without closing the underlying channel (it is the
     * per-connection socket, reused across calls).
     *
     * @throws IOException on close failure
     */
    @Override
    public void close() throws IOException {
        // closeReadSource=false: the underlying channel is the per-connection
        // socket and lives across many RPCs. Stock ArrowReader otherwise
        // closes it on close(), which would break the next request.
        inner.close(/*closeReadSource=*/false);
    }

    /** Expose the dictionary provider so callers can resolve indices. */
    public DictionaryProvider dictionaryProvider() {
        return inner;
    }

    /** Resolve a dictionary-encoded vector index to its underlying value (e.g. enum name). */
    public Object resolveDictValue(long dictId, int index) {
        Dictionary d = inner.lookup(dictId);
        if (d == null) return null;
        org.apache.arrow.vector.FieldVector v = d.getVector();
        if (v instanceof org.apache.arrow.vector.VarCharVector vc) {
            return new String(vc.get(index), java.nio.charset.StandardCharsets.UTF_8);
        }
        return null;
    }

    private void ensureSchema() throws IOException {
        if (schemaInitialized) return;
        // Triggers ArrowReader.initialize, which calls our overridden
        // readSchema() and captures wireSchema before the memory-format
        // conversion.
        inner.getVectorSchemaRoot();
        schemaInitialized = true;
    }

    /**
     * {@link ArrowStreamReader} subclass that captures the original
     * (wire-format) schema before {@code ArrowReader.initialize} converts
     * it to memory format.
     */
    private static final class StreamReaderImpl extends ArrowStreamReader {
        Schema wireSchema;
        private final BufferAllocator validationAllocator;

        StreamReaderImpl(MessageChannelReader mr, BufferAllocator alloc) {
            super(mr, alloc);
            validationAllocator = alloc;
        }

        @Override
        protected Schema readSchema() throws IOException {
            Schema s = super.readSchema();
            wireSchema = s;
            return s;
        }

        @Override
        protected void loadRecordBatch(ArrowRecordBatch batch) {
            try {
                VectorSchemaRoot target = getVectorSchemaRoot();
                loadAndValidate(batch, target);
                target.setRowCount(batch.getLength());
            } catch (IOException | RuntimeException failure) {
                throw new IllegalArgumentException("Malformed Arrow IPC batch: " + failure.getMessage(), failure);
            } finally {
                batch.close();
            }
        }

        @Override
        protected void loadDictionary(ArrowDictionaryBatch batch) {
            boolean validated = false;
            try {
                Dictionary dictionary = lookup(batch.getDictionaryId());
                if (dictionary == null) throw new IllegalArgumentException("Unknown dictionary ID " + batch.getDictionaryId());
                try (VectorSchemaRoot owner = VectorSchemaRoot.create(
                        new Schema(List.of(dictionary.getVector().getField())), validationAllocator)) {
                    loadAndValidate(batch.getDictionary(), owner);
                }
                validated = true;
            } catch (RuntimeException failure) {
                throw new IllegalArgumentException("Malformed Arrow IPC dictionary batch: " + failure.getMessage(), failure);
            } finally {
                if (!validated) batch.close();
            }
            super.loadDictionary(batch);
        }

        private void loadAndValidate(ArrowRecordBatch batch, VectorSchemaRoot target) {
            if (batch.getLength() < 0) throw new IllegalArgumentException("Negative IPC batch length");
            // VectorLoader finishes by propagating the root row count into
            // vectors. ListVector.setValueCount then resets its child's count
            // from the final offset, potentially allocating/padding a corrupt
            // list into apparently valid data. Validate the original field-node
            // lengths before that normalization. This wrapper borrows target's
            // vectors: the target owns them and they are only loaded once.
            VectorSchemaRoot raw = new VectorSchemaRoot(target.getSchema().getFields(), target.getFieldVectors(), batch.getLength()) {
                @Override public void setRowCount(int count) {
                    if (count != getRowCount()) throw new IllegalArgumentException("Unexpected IPC root length");
                    // Preserve the individual counts loaded from field nodes.
                }
            };
            new VectorLoader(raw).load(batch);
            // These vector types were created from the schema above. Arrow
            // 19's separate type visitor incorrectly treats MapVector as
            // ListVector and rejects valid maps, so check buffers directly.
            var validator = new ValidateVectorBufferVisitor();
            for (var vector : raw.getFieldVectors()) {
                if (vector.getValueCount() != batch.getLength()) {
                    throw new IllegalArgumentException("IPC root and field-node lengths differ");
                }
                vector.accept(validator, null);
            }
        }
    }

}
