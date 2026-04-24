// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.shm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ShmSegmentTest {

    @Test
    void create_and_attach_roundtrip(@TempDir Path dir) throws Exception {
        Path p = dir.resolve("round.shm");
        byte[] payload = "hello shared memory".getBytes();
        long off;
        try (ShmSegment seg = ShmSegment.create(p, (1L << 17))) {
            off = seg.allocate(payload.length);
            seg.writeAt(off, payload);
        }

        // Re-open and read; owner flag on create means close() deleted the file,
        // so re-create with the same size + copy the payload back in for the test.
        try (ShmSegment seg = ShmSegment.create(p, (1L << 17))) {
            long off2 = seg.allocate(payload.length);
            seg.writeAt(off2, payload);

            // Attach from another handle (simulates a consumer process).
            try (ShmSegment attached = ShmSegment.attach(p)) {
                byte[] read = attached.readAt(off2, payload.length);
                assertArrayEquals(payload, read);
            }
        }
    }

    @Test
    void allocations_sort_by_offset_and_find_first_gap(@TempDir Path dir) throws Exception {
        Path p = dir.resolve("alloc.shm");
        try (ShmSegment seg = ShmSegment.create(p, (1L << 17))) {
            long a = seg.allocate(100);
            long b = seg.allocate(100);
            long c = seg.allocate(100);
            // a < b < c monotonically
            assertEquals(ShmSegment.HEADER_SIZE, a);
            assertEquals(a + 100, b);
            assertEquals(b + 100, c);

            // Free the middle one and allocate 50 bytes — should land in the gap.
            seg.free(b);
            long d = seg.allocate(50);
            assertEquals(b, d, "new allocation should fill the freed gap");
        }
    }

    @Test
    void out_of_space_throws(@TempDir Path dir) throws Exception {
        Path p = dir.resolve("full.shm");
        try (ShmSegment seg = ShmSegment.create(p, ShmSegment.HEADER_SIZE + 512)) {
            seg.allocate(400);
            assertThrows(IllegalStateException.class, () -> seg.allocate(200));
        }
    }

    @Test
    void bad_magic_rejected(@TempDir Path dir) throws Exception {
        Path p = dir.resolve("bad.shm");
        Files.write(p, new byte[ShmSegment.HEADER_SIZE + 100]);  // all zero → no magic
        assertThrows(java.io.IOException.class, () -> ShmSegment.attach(p));
    }
}
