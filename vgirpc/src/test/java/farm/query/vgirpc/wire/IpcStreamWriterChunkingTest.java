// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.wire;

import farm.query.vgirpc.marshal.Marshalling;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the two things that decide whether this port survives a payload far
 * larger than one write.
 *
 * <p>The Python reference had to teach its transport writers to loop on the
 * returned count <em>and</em> clamp each call to 1 GiB, because a single
 * {@code write(2)} above {@code INT_MAX} on macOS either short-writes exactly
 * {@code INT_MAX} with no error (pipes — the peer then deadlocks waiting for
 * the tail) or fails with {@code EINVAL} (sockets). This port never reaches
 * that size at the syscall: {@code Channels.newChannel(OutputStream)} bounces
 * through a fixed 8 KiB heap array, and the transports' 64 KiB
 * {@link BufferedOutputStream} therefore never takes its direct-write bypass.
 * That is an invariant of two collaborating layers, neither of which states
 * it, so {@link #largeBatch_neverOffersMoreThanOneBufferToTheSink} states it:
 * write a 4 MiB value and watch the sink.
 *
 * <p>The second test is the ceiling that remains. {@code large_binary} exists
 * to carry values past 2 GiB and Go/Rust workers do; a Java array cannot, and
 * the failure used to read {@code NegativeArraySizeException: -2147483647}.
 */
public class IpcStreamWriterChunkingTest {

    /** Transport buffer size — every transport wraps its sink in one of these. */
    private static final int TRANSPORT_BUFFER = 1 << 16;

    /** Records the largest single {@code write} the sink was ever handed. */
    private static final class RecordingSink extends OutputStream {
        private int largestWrite;
        private long total;

        @Override public void write(int b) {
            largestWrite = Math.max(largestWrite, 1);
            total++;
        }

        @Override public void write(byte[] b, int off, int len) {
            largestWrite = Math.max(largestWrite, len);
            total += len;
        }
    }

    @Test
    public void largeBatch_neverOffersMoreThanOneBufferToTheSink() throws Exception {
        byte[] payload = new byte[4 * 1024 * 1024];
        java.util.Arrays.fill(payload, (byte) 0xA5);

        RecordingSink sink = new RecordingSink();
        Field field = Field.nullable("value", new ArrowType.LargeBinary());
        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(field)), alloc)) {
            LargeVarBinaryVector v = (LargeVarBinaryVector) root.getVector("value");
            v.allocateNew();
            v.setSafe(0, payload);
            v.setValueCount(1);
            root.setRowCount(1);

            // Exactly how every pipe/unix/TCP transport is wired: a buffered
            // stream over the real sink, handed to IpcStreamWriter.
            try (BufferedOutputStream buffered = new BufferedOutputStream(sink, TRANSPORT_BUFFER);
                 IpcStreamWriter w = new IpcStreamWriter(buffered)) {
                w.writeBatch(root, null);
            }
        }

        assertTrue(sink.total > payload.length,
                "the batch should have reached the sink; wrote " + sink.total + " bytes");
        assertTrue(sink.largestWrite <= TRANSPORT_BUFFER,
                "a single write of " + sink.largestWrite + " bytes reached the sink; nothing on this "
                        + "path may hand the kernel more than one " + TRANSPORT_BUFFER + "-byte buffer, "
                        + "or a >2 GiB payload short-writes on a macOS pipe and EINVALs on a socket");
    }

    @Test
    public void valueLongerThanIntMax_isRefusedByName() throws Exception {
        Field field = Field.nullable("value", new ArrowType.LargeBinary());
        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
             LargeVarBinaryVector v = new LargeVarBinaryVector(field, alloc)) {
            v.allocateNew();
            v.setSafe(0, new byte[] {1, 2, 3});
            v.setValueCount(1);
            // Forge the end offset rather than allocate 2 GiB: the guard reads
            // the 64-bit offsets, which is the whole point of it.
            v.getOffsetBuffer().setLong(8L, (1L << 31) + 1L);

            UnsupportedOperationException e = assertThrows(UnsupportedOperationException.class,
                    () -> Marshalling.readScalar(v, 0, field));
            assertTrue(e.getMessage().contains("large_binary value in field 'value' is 2147483649 bytes"),
                    "message should name the field and the real size: " + e.getMessage());
            assertTrue(e.getMessage().contains(String.valueOf(Integer.MAX_VALUE)),
                    "message should name the limit: " + e.getMessage());

            // Restore a sane offset so close() does not trip on the forgery.
            v.getOffsetBuffer().setLong(8L, 3L);
            assertEquals(3, ((byte[]) Marshalling.readScalar(v, 0, field)).length);
        }
    }
}
