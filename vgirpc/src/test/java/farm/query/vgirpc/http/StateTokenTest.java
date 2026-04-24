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

    private static final String ANON = "";

    @Test
    void roundtrips_state_output_input_streamId() {
        StateToken src = new StateToken(
                new byte[]{1, 2, 3},
                new byte[]{4, 5},
                new byte[]{6},
                "stream-abc",
                1_700_000_000L);
        byte[] packed = src.pack(KEY, ANON);
        StateToken out = StateToken.unpack(packed, KEY, 0, ANON);
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
        byte[] packed = src.pack(KEY, ANON);
        StateToken out = StateToken.unpack(packed, KEY, 0, ANON);
        assertEquals(src.createdAt(), out.createdAt());
    }

    @Test
    void ttl_expired_token_rejected() {
        StateToken src = new StateToken(new byte[0], new byte[0], new byte[0], "",
                System.currentTimeMillis() / 1000 - 100);
        byte[] packed = src.pack(KEY, ANON);
        assertThrows(TokenExpiredException.class, () -> StateToken.unpack(packed, KEY, 30, ANON));
    }

    @Test
    void ttl_fresh_token_allowed() {
        StateToken src = new StateToken(new byte[0], new byte[0], new byte[0], "",
                System.currentTimeMillis() / 1000 - 5);
        byte[] packed = src.pack(KEY, ANON);
        StateToken out = StateToken.unpack(packed, KEY, 30, ANON);
        assertEquals(src.createdAt(), out.createdAt());
    }

    @Test
    void tampered_signature_rejected() {
        StateToken src = new StateToken(new byte[]{1, 2, 3}, new byte[0], new byte[0], "",
                System.currentTimeMillis() / 1000);
        byte[] packed = src.pack(KEY, ANON);
        // Flip one payload byte before re-decoding
        packed[packed.length - 1] ^= 0x01;
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> StateToken.unpack(packed, KEY, 0, ANON));
        assertTrue(e.getMessage().contains("signature"));
    }

    @Test
    void wrong_key_rejected() {
        StateToken src = new StateToken(new byte[]{1}, new byte[0], new byte[0], "",
                System.currentTimeMillis() / 1000);
        byte[] packed = src.pack(KEY, ANON);
        byte[] otherKey = new byte[32];
        otherKey[0] = 99;
        assertThrows(IllegalArgumentException.class, () -> StateToken.unpack(packed, otherKey, 0, ANON));
    }

    @Test
    void principal_bound_token_accepted_by_same_principal() {
        StateToken src = new StateToken(new byte[]{7, 7}, new byte[0], new byte[0], "s",
                System.currentTimeMillis() / 1000);
        byte[] packed = src.pack(KEY, "alice");
        StateToken out = StateToken.unpack(packed, KEY, 0, "alice");
        assertArrayEquals(src.state(), out.state());
    }

    @Test
    void wrong_principal_rejected() {
        StateToken src = new StateToken(new byte[]{7, 7}, new byte[0], new byte[0], "s",
                System.currentTimeMillis() / 1000);
        byte[] packed = src.pack(KEY, "alice");
        // Bob presents Alice's token: HMAC verification uses Bob's derived key, which differs.
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> StateToken.unpack(packed, KEY, 0, "bob"));
        assertTrue(e.getMessage().contains("signature"));
    }

    @Test
    void anonymous_token_rejected_by_named_principal() {
        StateToken src = new StateToken(new byte[]{7, 7}, new byte[0], new byte[0], "s",
                System.currentTimeMillis() / 1000);
        byte[] packed = src.pack(KEY, ANON);
        assertThrows(IllegalArgumentException.class, () -> StateToken.unpack(packed, KEY, 0, "alice"));
    }
}
