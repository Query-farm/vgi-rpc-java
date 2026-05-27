// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.shm;

import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

final class ShmResolverTest {

    private static String uniqueName() {
        return "vgirt-" + Long.toHexString(ThreadLocalRandom.current().nextLong() & 0xFFFFFFFFL);
    }

    /**
     * Round-trip a batch through the segment (emit -> resolve) for each decode
     * path and assert the data survives AND the shared root allocator returns to
     * baseline. The balance check catches foreign-buffer refcount bugs (leak or
     * double-free) in the zero-copy path, which the integration worker (no debug
     * allocator) would miss. We exercise both paths explicitly because zero-copy
     * is opt-in (off by default), so {@code resolve()} alone wouldn't cover it.
     */
    @Test
    void zero_copy_roundtrip_is_balanced_and_correct() throws Exception {
        roundTrip(true);
    }

    @Test
    void copy_roundtrip_is_balanced_and_correct() throws Exception {
        roundTrip(false);
    }

    private static void roundTrip(boolean zeroCopy) throws Exception {
        BufferAllocator root = Allocators.root();
        long baseline = root.getAllocatedMemory();

        Schema schema = new Schema(List.of(
                Field.nullable("n", Types.MinorType.BIGINT.getType()),
                Field.nullable("s", Types.MinorType.VARCHAR.getType())));
        long[] nums = {10, 20, 30, 40};
        String[] strs = {"a", "bb", "ccc", "dddd"};

        try (FfmShm seg = FfmShm.create(uniqueName(), 1L << 20)) {
            try (VectorSchemaRoot src = VectorSchemaRoot.create(schema, root)) {
                BigIntVector nv = (BigIntVector) src.getVector("n");
                VarCharVector sv = (VarCharVector) src.getVector("s");
                nv.allocateNew(nums.length);
                sv.allocateNew();
                for (int i = 0; i < nums.length; i++) {
                    nv.setSafe(i, nums[i]);
                    sv.setSafe(i, strs[i].getBytes(StandardCharsets.UTF_8));
                }
                src.setRowCount(nums.length);

                var ptr = ShmResolver.maybeWriteToShm(seg, src, null);
                assertNotNull(ptr, "non-empty non-dict batch should be shm-eligible");
                long off, len;
                try (VectorSchemaRoot pointerRoot = ptr.root()) {
                    off = Long.parseLong(ptr.customMetadata().get(farm.query.vgirpc.wire.Metadata.SHM_OFFSET));
                    len = Long.parseLong(ptr.customMetadata().get(farm.query.vgirpc.wire.Metadata.SHM_LENGTH));
                    VectorSchemaRoot got = zeroCopy
                            ? ShmResolver.resolveZeroCopy(seg, off, len, schema)
                            : ShmResolver.resolveCopy(seg, off, len, schema);
                    try (VectorSchemaRoot g = got) {
                        assertEquals(nums.length, g.getRowCount());
                        BigIntVector gn = (BigIntVector) g.getVector("n");
                        VarCharVector gs = (VarCharVector) g.getVector("s");
                        for (int i = 0; i < nums.length; i++) {
                            assertEquals(nums[i], gn.get(i), "row " + i + " bigint");
                            assertEquals(strs[i], new String(gs.get(i), StandardCharsets.UTF_8), "row " + i + " varchar");
                        }
                    }
                }
            }
        }

        assertEquals(baseline, root.getAllocatedMemory(),
                "root allocator must return to baseline — no leaked/over-released buffers");
    }
}
