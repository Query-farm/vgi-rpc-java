// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.shm;

import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import farm.query.vgirpc.wire.IpcStreamWriter;
import farm.query.vgirpc.wire.Metadata;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ForeignAllocation;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageMetadataResult;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Detect, resolve and produce shared-memory "pointer batches" — the local
 * analogue of {@link farm.query.vgirpc.external.LocationResolver} /
 * {@link farm.query.vgirpc.external.Externalizer}, but the payload lives in a
 * {@link Shm} segment rather than at an HTTP URL.
 *
 * <p>A pointer batch is a zero-row batch (same schema as the real batch) whose
 * custom metadata carries {@code vgi_rpc.shm_offset} + {@code vgi_rpc.shm_length}.
 * The original batch's custom metadata rides the pointer batch on the pipe; the
 * segment slice holds only the Arrow data (a standalone IPC stream:
 * schema + record batch + EOS), byte-compatible with the C++/Go/Python peers
 * (see {@code vgi_shm_segment.cpp::MaybeResolveBatch}).</p>
 *
 * <p>Dictionary-encoded batches (DuckDB ENUMs) are <em>not</em> routed through
 * shm — a zero-row pointer batch with a dict schema and no preceding dictionary
 * batch trips the consumer's "expected dictionaries at the start" check. Those
 * fall back to inline transfer, which already round-trips correctly. This keeps
 * results identical to the inline path while accelerating the common case.</p>
 */
public final class ShmResolver {

    private static final boolean DEBUG = System.getenv("VGI_RPC_SHM_DEBUG") != null;

    // Smallest batch (bytes) worth shipping through shm; below this the pipe wins,
    // because shm's fixed per-batch cost (slot allocation + pointer round trip +
    // the peer's resolve/free) outweighs the copy it saves. The crossover is
    // platform-specific: POSIX shm_open/mmap overtakes the pipe around 64-256KB,
    // while Windows' page-file mapping plus the fast overlapped-pipe read push it to ~0.5-1MB.
    // Overridable with VGI_RPC_SHM_MIN_BATCH_BYTES. Mirrors the same gate in the
    // C++ engine and the Python/Go/Rust SDK output paths.
    private static final long SHM_MIN_BATCH_BYTES = resolveShmMinBatchBytes();

    private static long resolveShmMinBatchBytes() {
        String env = System.getenv("VGI_RPC_SHM_MIN_BATCH_BYTES");
        if (env != null) {
            try {
                return Long.parseLong(env.trim());
            } catch (NumberFormatException ignore) {
                // fall through to platform default
            }
        }
        boolean windows = System.getProperty("os.name", "").toLowerCase().contains("win");
        return windows ? 1024L * 1024L : 128L * 1024L;
    }
    // Zero-copy inbound decode: wrap the segment region as a foreign ArrowBuf
    // instead of allocating+zeroing+copying. **Opt-in (VGI_RPC_SHM_ZEROCOPY=1),
    // off by default** — it's a wash-to-slight-loss in practice:
    //   * It defers the input free to batch close, so the segment must hold input
    //     AND output simultaneously (~2x the size). If undersized, the output
    //     allocation fails and falls back to the pipe — a large regression that is
    //     a sizing artifact, not the transport.
    //   * With an adequate segment it's ~12% SLOWER than the copy path for a
    //     passthrough (echo, large in / large out), because re-serializing reads
    //     the input back out of shm rather than from a cache-warm JVM copy.
    //   * It's a modest ~5-10% WIN only for large-input / small-output functions
    //     that don't re-emit the bulk (filters/aggregations/scalar predicates).
    // Measurements 2026-05-26 (Apple M3, threads=1).
    private static final boolean ZEROCOPY = "1".equals(System.getenv("VGI_RPC_SHM_ZEROCOPY"));

    private ShmResolver() {}

    /** Resolved inbound batch — caller owns {@code root} and must close it. */
    public record Resolved(VectorSchemaRoot root, Map<String, String> customMetadata) {}

    /** Outbound pointer to write in place of the real batch — caller owns {@code root}. */
    public record ShmPointer(VectorSchemaRoot root, Map<String, String> customMetadata) {}

    /** True iff the metadata describes a shm pointer batch (and is not a log batch). */
    public static boolean isPointer(int rowCount, Map<String, String> meta) {
        return rowCount == 0 && meta != null
                && meta.get(Metadata.SHM_OFFSET) != null
                && meta.get(Metadata.LOG_LEVEL) == null;
    }

    /** True iff this data batch is one we intend to route over shm (non-empty,
     *  non-dict) — i.e. it should become a pointer batch when a segment is present.
     *  Used to detect unintended inline fallback. */
    public static boolean shmEligible(VectorSchemaRoot root) {
        return root.getRowCount() > 0 && !hasDictionary(root.getSchema());
    }

    /**
     * Resolve an inbound pointer batch to its underlying data batch, reading the
     * IPC bytes from the segment and freeing the allocation. The returned root is
     * caller-owned. {@code pointerRoot}'s schema supplies the read schema.
     */
    public static Resolved resolve(Shm seg, VectorSchemaRoot pointerRoot,
                                   Map<String, String> meta) throws Exception {
        long offset = Long.parseLong(meta.get(Metadata.SHM_OFFSET));
        long length = Long.parseLong(meta.get(Metadata.SHM_LENGTH));
        // Validate the peer-supplied pointer against the mapping before reading —
        // offset/length are untrusted metadata crossing a trust boundary. Mirrors
        // the C++ side (vgi_shm_segment.cpp MaybeResolveBatch). Written so a huge
        // or negative value can't underflow past the guard; offset<HEADER_SIZE
        // also rejects negatives and any pointer into the header region.
        long size = seg.size();
        if (offset < Shm.HEADER_SIZE || offset > size || length < 0 || length > size - offset) {
            throw new IOException("shm pointer out of range: offset=" + offset
                    + " length=" + length + " size=" + size);
        }
        // Label the resolved batch with the pointer batch's schema. The pointer
        // arrived over the same stream an inline batch would, so its schema is
        // exactly what the caller compares against (the bind-time input schema);
        // a schema rederived from the data can differ in field metadata (e.g.
        // TIMESTAMP_TZ) and trip the downstream cast.
        Schema schema = pointerRoot.getSchema();
        VectorSchemaRoot root = (ZEROCOPY && !hasDictionary(schema))
                ? resolveZeroCopy(seg, offset, length, schema)
                : resolveCopy(seg, offset, length, schema);
        if (DEBUG) {
            System.err.println("[vgi-shm] resolved inbound off=" + offset + " len=" + length
                    + " rows=" + root.getRowCount() + (ZEROCOPY ? " (zero-copy)" : " (copy)"));
        }
        Map<String, String> merged = new LinkedHashMap<>(meta);
        merged.remove(Metadata.SHM_OFFSET);
        merged.remove(Metadata.SHM_LENGTH);
        merged.put(Metadata.SHM_SOURCE, seg.name());
        return new Resolved(root, merged);
    }

    /**
     * Zero-copy decode: wrap the record-batch body region of the segment as a
     * foreign {@link ArrowBuf} (no allocation, no zeroing, no memcpy) and load the
     * vectors over it. The segment slot is freed when the returned root is closed
     * (release0 → seg.free) — safe under lockstep, since the client cannot reuse
     * the region until the worker has replied. Buffer ownership mirrors Arrow's
     * own {@code MessageSerializer.deserializeRecordBatch(ReadChannel, allocator)}:
     * the slices retain the body, so we release our initial wrap reference.
     */
    static VectorSchemaRoot resolveZeroCopy(Shm seg, long offset, long length, Schema schema)
            throws Exception {
        BufferAllocator alloc = Allocators.root();
        ReadChannel in = new ReadChannel(seg.readChannelAt(offset, length));
        MessageMetadataResult schemaMsg = MessageSerializer.readMessage(in);
        if (schemaMsg == null) {
            throw new IOException("shm slice: missing schema message");
        }
        MessageMetadataResult rbMsg = MessageSerializer.readMessage(in);
        if (rbMsg == null || rbMsg.getMessage().headerType() != MessageHeader.RecordBatch) {
            throw new IOException("shm slice: expected record batch message");
        }
        long bodyLen = rbMsg.getMessageBodyLength();
        long bodyAddr = seg.addressAt(offset + in.bytesRead());
        ArrowBuf body = alloc.wrapForeignAllocation(new ForeignAllocation(bodyLen, bodyAddr) {
            @Override protected void release0() { seg.free(offset); }
        });
        // deserializeRecordBatch takes ownership of `body`: its per-buffer slices
        // retain the underlying allocation and it drops the incoming reference. So
        // we must NOT release `body` ourselves on the success path (that's a
        // double-release). On a failure before ownership transfers, release it.
        ArrowRecordBatch batch;
        try {
            batch = MessageSerializer.deserializeRecordBatch(rbMsg, body);
        } catch (Throwable t) {
            body.getReferenceManager().release();   // release0 -> seg.free
            throw t;
        }
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc);
        boolean ok = false;
        try {
            // load() retains the buffers into the vectors; once batch is closed the
            // foreign allocation lives exactly as long as the loaded vectors, and
            // its refcount hits 0 (-> release0 -> seg.free) when `root` is closed.
            new VectorLoader(root).load(batch);
            ok = true;
        } finally {
            batch.close();
            if (!ok) root.close();
        }
        return root;
    }

    /** Copy decode (fallback): read the IPC stream from the segment into fresh
     *  JVM buffers and free the slot immediately. */
    static VectorSchemaRoot resolveCopy(Shm seg, long offset, long length, Schema schema)
            throws Exception {
        VectorSchemaRoot copy;
        try (IpcStreamReader r = new IpcStreamReader(seg.readChannelAt(offset, length), Allocators.root())) {
            if (r.readNextBatch() == null) {
                throw new IOException("shm slice contained no data batch");
            }
            copy = copyRoot(r.root(), schema);
        }
        seg.free(offset);
        return copy;
    }

    /**
     * Possibly write {@code root} into the segment and return a zero-row pointer
     * batch to send in its place. Returns {@code null} to mean "keep inline":
     * no segment, zero-row batch, dict-encoded schema, serialization failure, or
     * the segment is full (the C++ client frees outbound allocations as it
     * consumes them; until it does, allocation may fail and we fall back).
     */
    public static ShmPointer maybeWriteToShm(Shm seg, VectorSchemaRoot root,
                                             Map<String, String> existingMeta) {
        if (seg == null || root.getRowCount() == 0) return null;
        if (hasDictionary(root.getSchema())) return null;

        // Upper-bound the IPC stream size (buffer bytes + framing slack) so we can
        // allocate once and serialize STRAIGHT into the segment — no intermediate
        // byte[] (drops the old toByteArray() + writeAt() double copy). If the
        // encode overflows the estimate we free and fall back to inline.
        long dataBytes = 0;
        for (FieldVector v : root.getFieldVectors()) dataBytes += v.getBufferSize();
        // Small batches are cheaper over the pipe than through shm.
        if (dataBytes < SHM_MIN_BATCH_BYTES) return null;
        long capacity = dataBytes + (dataBytes >> 6)
                + 16384L + 256L * root.getSchema().getFields().size();

        long off;
        try {
            off = seg.allocate(capacity);
        } catch (RuntimeException full) {     // segment / alloc-table full
            return null;
        }
        long written;
        try {
            ShmWriteChannel ch = seg.writeChannelAt(off, capacity);
            try (IpcStreamWriter w = new IpcStreamWriter(ch)) {
                w.writeBatch(root, null);     // bare data; metadata rides the pointer
                w.writeEos();
            }
            written = ch.written();
        } catch (Exception serializeFailed) { // overflow or encode error → inline
            seg.free(off);
            return null;
        }

        VectorSchemaRoot pointer = VectorSchemaRoot.create(root.getSchema(), Allocators.root());
        pointer.allocateNew();
        pointer.setRowCount(0);
        Map<String, String> meta = new LinkedHashMap<>();
        if (existingMeta != null) meta.putAll(existingMeta);
        meta.put(Metadata.SHM_OFFSET, Long.toString(off));
        meta.put(Metadata.SHM_LENGTH, Long.toString(written));
        return new ShmPointer(pointer, meta);
    }

    private static boolean hasDictionary(Schema s) {
        for (Field f : s.getFields()) {
            if (fieldHasDictionary(f)) return true;
        }
        return false;
    }

    private static boolean fieldHasDictionary(Field f) {
        if (f.getDictionary() != null) return true;
        for (Field c : f.getChildren()) {
            if (fieldHasDictionary(c)) return true;
        }
        return false;
    }

    /**
     * Move each vector out of the reader's (recycled-on-close) root into a fresh
     * caller-owned root via {@link TransferPair} — a zero-copy ownership transfer
     * that handles every Arrow type, including nested lists of TIMESTAMP_TZ that
     * row-wise {@code copyFromSafe}/{@code ComplexCopier} rejects. After transfer
     * the source vectors are empty, so the reader closes cleanly.
     */
    private static VectorSchemaRoot copyRoot(VectorSchemaRoot src, Schema schema) {
        List<FieldVector> moved = new ArrayList<>(src.getFieldVectors().size());
        for (FieldVector sv : src.getFieldVectors()) {
            TransferPair tp = sv.getTransferPair(sv.getAllocator());
            tp.transfer();
            moved.add((FieldVector) tp.getTo());
        }
        return new VectorSchemaRoot(schema, moved, src.getRowCount());
    }
}
