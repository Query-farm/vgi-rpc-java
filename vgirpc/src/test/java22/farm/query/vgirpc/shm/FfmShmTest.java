// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.shm;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class FfmShmTest {

    /** POSIX shm names are short (macOS PSHMNAMLEN=31, incl. leading slash). */
    private static String uniqueName() {
        return "vgitst-" + Long.toHexString(ThreadLocalRandom.current().nextLong() & 0xFFFFFFFFL);
    }

    @Test
    void create_and_attach_roundtrip() throws Exception {
        String name = uniqueName();
        byte[] payload = "hello shared memory".getBytes();
        try (FfmShm owner = FfmShm.create(name, 1L << 17)) {
            long off = owner.allocate(payload.length);
            owner.writeAt(off, payload);

            // A second handle attaches to the same named segment (simulates the
            // consumer process) and sees the producer's bytes through real
            // POSIX shared memory.
            try (FfmShm attached = FfmShm.attach(name, owner.size())) {
                assertArrayEquals(payload, attached.readAt(off, payload.length));
            }
        }
        // Owner close() shm_unlink'd the segment — re-attach must fail.
        assertThrows(java.io.IOException.class, () -> FfmShm.attach(name, 1L << 17));
    }

    @Test
    void allocations_sort_by_offset_and_find_first_gap() throws Exception {
        try (FfmShm seg = FfmShm.create(uniqueName(), 1L << 17)) {
            long a = seg.allocate(100);
            long b = seg.allocate(100);
            long c = seg.allocate(100);
            assertEquals(FfmShm.HEADER_SIZE, a);
            assertEquals(a + 100, b);
            assertEquals(b + 100, c);

            // Free the middle one and allocate 50 bytes — should land in the gap.
            seg.free(b);
            long d = seg.allocate(50);
            assertEquals(b, d, "new allocation should fill the freed gap");
        }
    }

    @Test
    void out_of_space_throws() throws Exception {
        try (FfmShm seg = FfmShm.create(uniqueName(), FfmShm.HEADER_SIZE + 512)) {
            seg.allocate(400);
            assertThrows(IllegalStateException.class, () -> seg.allocate(200));
        }
    }

    @Test
    void bad_magic_rejected() throws Exception {
        String name = uniqueName();
        try (FfmShm owner = FfmShm.create(name, 1L << 17)) {
            owner.writeAt(0, new byte[]{0, 0, 0, 0});   // clobber the VGIS magic
            assertThrows(java.io.IOException.class, () -> FfmShm.attach(name, owner.size()));
        }
    }

    @Test
    void corrupt_num_allocs_rejected() throws Exception {
        try (FfmShm seg = FfmShm.create(uniqueName(), 1L << 17)) {
            // num_allocs is a little-endian uint32 at header offset 16. Clobber it
            // to 5000 (> MAX_ALLOCS=4094); allocate/free must refuse to walk the
            // bogus entry table rather than read past the header.
            owner_setNumAllocs(seg, 5000);
            assertThrows(IllegalStateException.class, () -> seg.allocate(100));
            assertThrows(IllegalStateException.class, () -> seg.free(65536));
        }
    }

    private static void owner_setNumAllocs(FfmShm seg, int n) {
        seg.writeAt(16, new byte[]{(byte) n, (byte) (n >> 8), (byte) (n >> 16), (byte) (n >> 24)});
    }
}
