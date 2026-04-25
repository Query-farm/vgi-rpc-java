// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class BoundedByteArrayOutputStreamTest {

    @Test
    void within_limit_round_trips() {
        BoundedByteArrayOutputStream out = new BoundedByteArrayOutputStream(16);
        byte[] payload = {1, 2, 3, 4, 5};
        out.write(payload, 0, payload.length);
        assertArrayEquals(payload, out.toByteArray());
        assertEquals(5, out.size());
    }

    @Test
    void single_byte_overflow_throws() {
        BoundedByteArrayOutputStream out = new BoundedByteArrayOutputStream(2);
        out.write(0);
        out.write(0);
        PayloadTooLargeException e = assertThrows(PayloadTooLargeException.class, () -> out.write(0));
        assertTrue(e.getMessage().contains("external-location"));
    }

    @Test
    void block_overflow_throws_before_write() {
        BoundedByteArrayOutputStream out = new BoundedByteArrayOutputStream(4);
        // First write fits.
        out.write(new byte[]{1, 2}, 0, 2);
        // Second write would overflow — must throw without partially appending.
        assertThrows(PayloadTooLargeException.class, () -> out.write(new byte[]{3, 4, 5}, 0, 3));
        assertEquals(2, out.size());
    }

    @Test
    void exact_limit_accepted() {
        BoundedByteArrayOutputStream out = new BoundedByteArrayOutputStream(3);
        out.write(new byte[]{1, 2, 3}, 0, 3);
        assertEquals(3, out.size());
    }
}
