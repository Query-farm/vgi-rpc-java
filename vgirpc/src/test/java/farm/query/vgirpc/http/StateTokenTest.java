// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class StateTokenTest {

    private static final byte[] KEY = {
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
            17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32
    };

    @Test
    void roundtrips_state_output_input_streamId() {
        StateToken src = new StateToken(
                new byte[]{1, 2, 3},
                new byte[]{4, 5},
                new byte[]{6},
                "stream-abc",
                1_700_000_000L);
        byte[] packed = src.pack(KEY);
        StateToken out = StateToken.unpack(packed, KEY);
        assertArrayEquals(src.state(), out.state());
        assertArrayEquals(src.outputSchema(), out.outputSchema());
        assertArrayEquals(src.inputSchema(), out.inputSchema());
        assertEquals(src.streamId(), out.streamId());
        assertEquals(src.createdAt(), out.createdAt());
    }

    @Test
    void ttl_disabled_by_default() {
        StateToken src = new StateToken(new byte[0], new byte[0], new byte[0], "",
                System.currentTimeMillis() / 1000 - 10_000);
        byte[] packed = src.pack(KEY);
        StateToken out = StateToken.unpack(packed, KEY, 0);
        assertEquals(src.createdAt(), out.createdAt());
    }

    @Test
    void ttl_expired_token_rejected() {
        StateToken src = new StateToken(new byte[0], new byte[0], new byte[0], "",
                System.currentTimeMillis() / 1000 - 100);
        byte[] packed = src.pack(KEY);
        assertThrows(TokenExpiredException.class, () -> StateToken.unpack(packed, KEY, 30));
    }

    @Test
    void ttl_fresh_token_allowed() {
        StateToken src = new StateToken(new byte[0], new byte[0], new byte[0], "",
                System.currentTimeMillis() / 1000 - 5);
        byte[] packed = src.pack(KEY);
        StateToken out = StateToken.unpack(packed, KEY, 30);
        assertEquals(src.createdAt(), out.createdAt());
    }

    @Test
    void tampered_signature_rejected() {
        StateToken src = new StateToken(new byte[]{1, 2, 3}, new byte[0], new byte[0], "",
                System.currentTimeMillis() / 1000);
        byte[] packed = src.pack(KEY);
        // Flip one payload byte before re-decoding
        packed[packed.length - 1] ^= 0x01;
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> StateToken.unpack(packed, KEY));
        assertTrue(e.getMessage().contains("signature"));
    }

    @Test
    void wrong_key_rejected() {
        StateToken src = new StateToken(new byte[]{1}, new byte[0], new byte[0], "",
                System.currentTimeMillis() / 1000);
        byte[] packed = src.pack(KEY);
        byte[] otherKey = new byte[32];
        otherKey[0] = 99;
        assertThrows(IllegalArgumentException.class, () -> StateToken.unpack(packed, otherKey));
    }
}
