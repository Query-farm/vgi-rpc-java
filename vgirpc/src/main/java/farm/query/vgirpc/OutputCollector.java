// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.log.Level;
import farm.query.vgirpc.log.Message;
import farm.query.vgirpc.wire.Metadata;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Accumulator for stream output. Each call to {@code process()} must emit at most
 * one data batch (use {@link #emit(VectorSchemaRoot)}) and may call
 * {@link #clientLog(Level, String)} any number of times. Producer streams use
 * {@link #finish()} to terminate the stream.
 */
public final class OutputCollector {

    /**
     * A buffered batch entry — either a log/zero-row metadata batch or the
     * single data batch. {@code dictionaryProvider} carries the dictionaries
     * referenced by any {@code DictionaryEncoding}-tagged field on
     * {@code root}; {@code null} when the batch has no dict-encoded columns,
     * in which case callers fall back to a stream-level provider (e.g. the
     * input-reader's dicts on the TIO/echo path).
     */
    public record Entry(VectorSchemaRoot root, Map<String, String> customMetadata,
                          boolean isData, DictionaryProvider dictionaryProvider) {
        public Entry(VectorSchemaRoot root, Map<String, String> customMetadata, boolean isData) {
            this(root, customMetadata, isData, null);
        }
    }

    private final Schema outputSchema;
    private final String serverId;
    private final boolean producerMode;
    private final List<Entry> entries = new ArrayList<>();
    private boolean finished;
    private int dataIdx = -1;

    public OutputCollector(Schema outputSchema, String serverId, boolean producerMode) {
        this.outputSchema = outputSchema;
        this.serverId = serverId;
        this.producerMode = producerMode;
    }

    public Schema outputSchema() { return outputSchema; }
    public boolean finished() { return finished; }
    public List<Entry> entries() { return Collections.unmodifiableList(entries); }

    public Entry dataEntry() {
        if (dataIdx < 0) throw new IllegalStateException("no data batch emitted");
        return entries.get(dataIdx);
    }

    /** Emit the (single) data batch for this call. Ownership of {@code root} transfers to the collector. */
    public void emit(VectorSchemaRoot root) { emit(root, null, null); }

    public void emit(VectorSchemaRoot root, Map<String, String> customMetadata) {
        emit(root, customMetadata, null);
    }

    /**
     * Variant that carries a per-batch {@link DictionaryProvider} — required
     * when {@code root}'s schema declares any {@code DictionaryEncoding}
     * fields. The provider must hold a {@link
     * org.apache.arrow.vector.dictionary.Dictionary} for every dictionary id
     * referenced by the schema; the framework hands it to
     * {@link farm.query.vgirpc.wire.IpcStreamWriter#writeBatch(VectorSchemaRoot, Map, DictionaryProvider)}
     * so the dict batches go out on the wire alongside the data.
     */
    public void emit(VectorSchemaRoot root, Map<String, String> customMetadata,
                       DictionaryProvider dictionaryProvider) {
        if (dataIdx >= 0) {
            throw new IllegalStateException("only one data batch may be emitted per call");
        }
        dataIdx = entries.size();
        entries.add(new Entry(root, customMetadata, true, dictionaryProvider));
    }

    /** Append a zero-row client-directed log batch. */
    public void clientLog(Level level, String text) {
        clientLog(new Message(level, text, null));
    }

    public void clientLog(Message msg) {
        Map<String, String> md = msg.addToMetadata(null);
        if (serverId != null) md.put(Metadata.SERVER_ID, serverId);
        VectorSchemaRoot zeroRow = VectorSchemaRoot.create(outputSchema, farm.query.vgirpc.wire.Allocators.root());
        zeroRow.allocateNew();
        zeroRow.setRowCount(0);
        entries.add(new Entry(zeroRow, md, false, null));
    }

    /** Producer-only: signal end of stream. */
    public void finish() {
        if (!producerMode) {
            throw new IllegalStateException("finish() not allowed on exchange streams");
        }
        finished = true;
    }

    /** Validate that exactly one data batch has been emitted (for non-producer flows). */
    public void validate() {
        if (dataIdx < 0) throw new IllegalStateException("no data batch was emitted");
    }
}
