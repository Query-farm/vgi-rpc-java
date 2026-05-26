// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.wire;

import org.apache.arrow.flatbuf.KeyValue;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageChannelReader;
import org.apache.arrow.vector.ipc.message.MessageResult;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Arrow IPC stream reader that surfaces per-batch {@code custom_metadata}
 * (which the stock {@link ArrowStreamReader} does not expose).
 *
 * <p>Implementation: subclass {@link MessageChannelReader} so we capture
 * each {@link Message}'s custom_metadata as it flows through, and subclass
 * {@link ArrowStreamReader} so the original (wire-format) schema can be
 * recovered before {@code ArrowReader.initialize} converts it to memory
 * format. Everything else — dictionary tracking, dict-batch loading,
 * record-batch loading, framing, EOS — is inherited from stock Arrow.</p>
 */
public final class IpcStreamReader implements AutoCloseable {

    private final BufferAllocator allocator;
    private final CapturingMessageReader messageReader;
    private final StreamReaderImpl inner;
    private boolean eos = false;
    private boolean schemaInitialized = false;

    public IpcStreamReader(InputStream raw, BufferAllocator allocator) {
        this(Channels.newChannel(raw), allocator);
    }

    /** Read directly from a {@link ReadableByteChannel} (e.g. a shared-memory
     *  segment channel), bypassing the {@code Channels.newChannel} adapter's
     *  heap-{@code byte[]} bounce. */
    public IpcStreamReader(ReadableByteChannel channel, BufferAllocator allocator) {
        this.allocator = allocator;
        ReadChannel rc = new ReadChannel(channel);
        this.messageReader = new CapturingMessageReader(rc, allocator);
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
        return messageReader.lastCustomMetadata;
    }

    /** True if more data may be available; otherwise EOS / I/O end has been reached. */
    public boolean hasMore() { return !eos; }

    /** Consume any remaining batches until the EOS marker. */
    public void drain() throws IOException {
        while (!eos && readNextBatch() != null) {
            // discard
        }
    }

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

        StreamReaderImpl(MessageChannelReader mr, BufferAllocator alloc) {
            super(mr, alloc);
        }

        @Override
        protected Schema readSchema() throws IOException {
            Schema s = super.readSchema();
            wireSchema = s;
            return s;
        }
    }

    /**
     * {@link MessageChannelReader} subclass that captures the
     * {@code custom_metadata} of the most-recently-read Message. Stock
     * {@link ArrowStreamReader} reads dict and record batches via this
     * channel; we expose the metadata of the last message read so callers
     * can pick it up after {@link ArrowStreamReader#loadNextBatch()} returns.
     */
    private static final class CapturingMessageReader extends MessageChannelReader {
        Map<String, String> lastCustomMetadata = Map.of();

        CapturingMessageReader(ReadChannel in, BufferAllocator allocator) {
            super(in, allocator);
        }

        @Override
        public MessageResult readNext() throws IOException {
            MessageResult r = super.readNext();
            if (r != null) {
                Message m = r.getMessage();
                int n = m.customMetadataLength();
                if (n == 0) {
                    lastCustomMetadata = Map.of();
                } else {
                    Map<String, String> map = new LinkedHashMap<>(n);
                    for (int i = 0; i < n; i++) {
                        KeyValue kv = m.customMetadata(i);
                        map.put(kv.key(), kv.value());
                    }
                    lastCustomMetadata = map;
                }
            }
            return r;
        }
    }
}
