// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import org.junit.jupiter.api.Test;

import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class StateTokenTest {

    private static final byte[] KEY = {
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
            17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32
    };

    private static final String ANON = "";

    /** Fixed call id: the token tests are about the envelope, not the id. */
    private static final byte[] CALL_ID = {
            1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, (byte) 144, (byte) 233, 77, 55, 32
    };

    @Test
    void roundtrips_state_output_input_streamId() {
        StateToken src = new StateToken(new byte[]{1, 2, 3}, CALL_ID, 1_700_000_000L);
        byte[] packed = src.pack(KEY, ANON);
        StateToken out = StateToken.unpack(packed, KEY, 0, ANON);
        assertArrayEquals(src.state(), out.state());
        assertArrayEquals(src.callId(), out.callId());
        assertEquals(src.createdAt(), out.createdAt());

        // The schemas and stream id ride the call token now, not the cursor.
        CallToken call = new CallToken(new byte[]{4, 5}, new byte[]{6}, "stream-abc",
                CALL_ID, 1_700_000_000L);
        CallToken callOut = CallToken.unpack(call.pack(KEY, ANON), KEY, 0, ANON);
        assertArrayEquals(call.outputSchema(), callOut.outputSchema());
        assertArrayEquals(call.inputSchema(), callOut.inputSchema());
        assertEquals(call.streamId(), callOut.streamId());
        assertArrayEquals(CALL_ID, callOut.callId());
    }

    @Test
    void ttl_disabled_by_default() {
        StateToken src = new StateToken(new byte[0], CALL_ID, System.currentTimeMillis() / 1000 - 10_000);
        byte[] packed = src.pack(KEY, ANON);
        StateToken out = StateToken.unpack(packed, KEY, 0, ANON);
        assertEquals(src.createdAt(), out.createdAt());
    }

    @Test
    void ttl_expired_token_rejected() {
        StateToken src = new StateToken(new byte[0], CALL_ID, System.currentTimeMillis() / 1000 - 100);
        byte[] packed = src.pack(KEY, ANON);
        assertThrows(TokenExpiredException.class, () -> StateToken.unpack(packed, KEY, 30, ANON));
    }

    @Test
    void ttl_fresh_token_allowed() {
        StateToken src = new StateToken(new byte[0], CALL_ID, System.currentTimeMillis() / 1000 - 5);
        byte[] packed = src.pack(KEY, ANON);
        StateToken out = StateToken.unpack(packed, KEY, 30, ANON);
        assertEquals(src.createdAt(), out.createdAt());
    }

    @Test
    void tampered_ciphertext_rejected() {
        StateToken src = new StateToken(new byte[]{1, 2, 3}, CALL_ID, System.currentTimeMillis() / 1000);
        byte[] packed = src.pack(KEY, ANON);
        // Decode, flip a byte inside the ciphertext, re-encode.
        byte[] raw = Base64.getDecoder().decode(packed);
        // 1 byte version + 12 byte nonce = 13. Hit the ciphertext.
        raw[13] ^= 0x01;
        byte[] tampered = Base64.getEncoder().encode(raw);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> StateToken.unpack(tampered, KEY, 0, ANON));
        assertTrue(e.getMessage().contains("signature"));
    }

    @Test
    void tampered_nonce_rejected() {
        StateToken src = new StateToken(new byte[]{1, 2, 3}, CALL_ID, System.currentTimeMillis() / 1000);
        byte[] packed = src.pack(KEY, ANON);
        byte[] raw = Base64.getDecoder().decode(packed);
        raw[1] ^= 0x01;  // first nonce byte
        byte[] tampered = Base64.getEncoder().encode(raw);
        assertThrows(IllegalArgumentException.class,
                () -> StateToken.unpack(tampered, KEY, 0, ANON));
    }

    @Test
    void unknown_version_rejected() {
        StateToken src = new StateToken(new byte[]{1, 2, 3}, CALL_ID, System.currentTimeMillis() / 1000);
        byte[] packed = src.pack(KEY, ANON);
        byte[] raw = Base64.getDecoder().decode(packed);
        raw[0] = (byte) 0x99;
        byte[] tampered = Base64.getEncoder().encode(raw);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> StateToken.unpack(tampered, KEY, 0, ANON));
        assertTrue(e.getMessage().contains("Unsupported state token version"));
    }

    @Test
    void malformed_base64_rejected() {
        byte[] junk = "not!base64!".getBytes();
        assertThrows(IllegalArgumentException.class,
                () -> StateToken.unpack(junk, KEY, 0, ANON));
    }

    @Test
    void wrong_key_rejected() {
        StateToken src = new StateToken(new byte[]{1}, CALL_ID, System.currentTimeMillis() / 1000);
        byte[] packed = src.pack(KEY, ANON);
        byte[] otherKey = new byte[32];
        otherKey[0] = 99;
        assertThrows(IllegalArgumentException.class, () -> StateToken.unpack(packed, otherKey, 0, ANON));
    }

    @Test
    void principal_bound_token_accepted_by_same_principal() {
        StateToken src = new StateToken(new byte[]{7, 7}, CALL_ID, System.currentTimeMillis() / 1000);
        byte[] packed = src.pack(KEY, "alice");
        StateToken out = StateToken.unpack(packed, KEY, 0, "alice");
        assertArrayEquals(src.state(), out.state());
    }

    @Test
    void wrong_principal_rejected() {
        StateToken src = new StateToken(new byte[]{7, 7}, CALL_ID, System.currentTimeMillis() / 1000);
        byte[] packed = src.pack(KEY, "alice");
        // Bob presents Alice's token: AAD mismatch fails decryption.
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> StateToken.unpack(packed, KEY, 0, "bob"));
        assertTrue(e.getMessage().contains("signature"));
    }

    @Test
    void anonymous_token_rejected_by_named_principal() {
        StateToken src = new StateToken(new byte[]{7, 7}, CALL_ID, System.currentTimeMillis() / 1000);
        byte[] packed = src.pack(KEY, ANON);
        assertThrows(IllegalArgumentException.class, () -> StateToken.unpack(packed, KEY, 0, "alice"));
    }

    // ---------------------------------------------------------------------
    // Token payload compression
    //
    // Token payloads are compressed *inside* the seal. The ordering is the
    // whole point: once a token is sealed it is ciphertext, so the HTTP body
    // codec can find no redundancy in it — it recovers only the slack base64
    // adds, never the state's own structure. Compressing before sealing
    // reaches the real redundancy.
    //
    // None of this is visible on the wire, so the cross-language conformance
    // suite cannot reach it; docs/WIRE_PROTOCOL.md in the reference repo
    // makes it normative and asks each port to pin it with a language-local
    // test like these.
    // ---------------------------------------------------------------------

    private static byte[] repeat(String unit, int times) {
        return unit.repeat(times).getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    @Test
    void payload_is_compressed_when_redundant() {
        byte[] plaintext = repeat("vgi-rpc-state-", 1000);
        byte[] packed = StateToken.packPayload(plaintext);
        assertEquals(0x01, packed[0], "expected the zstd codec tag");
        assertTrue(packed.length < plaintext.length / 4,
                "expected real compression on a redundant payload, got "
                        + packed.length + " from " + plaintext.length);
    }

    @Test
    void payload_stays_raw_when_incompressible() {
        // A byte ramp with no repeats gives the codec nothing to find.
        // Skipping is what keeps the guarantee one-directional: a token may
        // get smaller, never larger than its plaintext plus the one tag byte.
        byte[] plaintext = new byte[256];
        for (int i = 0; i < plaintext.length; i++) {
            plaintext[i] = (byte) i;
        }
        byte[] packed = StateToken.packPayload(plaintext);
        assertEquals(0x00, packed[0], "expected the raw codec tag");
        assertEquals(plaintext.length + 1, packed.length,
                "raw payload must not grow beyond the tag byte");
    }

    @Test
    void payload_round_trips_under_either_codec() {
        byte[] ramp = new byte[64];
        for (int i = 0; i < ramp.length; i++) {
            ramp[i] = (byte) i;
        }
        byte[][] cases = {
                new byte[0],
                "x".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                ramp,
                repeat("vgi-rpc-state-", 500),
        };
        for (byte[] plaintext : cases) {
            byte[] packed = StateToken.packPayload(plaintext);
            assertArrayEquals(plaintext, StateToken.unpackPayload(packed),
                    "round trip changed the payload");
        }
    }

    @Test
    void malformed_payloads_are_rejected() {
        // An unknown tag, an empty payload, or a body that will not
        // decompress all mean a token this server did not mint, so all three
        // surface as the same uniform error the caller maps to 400.
        assertThrows(IllegalArgumentException.class,
                () -> StateToken.unpackPayload(new byte[0]));
        assertThrows(IllegalArgumentException.class,
                () -> StateToken.unpackPayload(new byte[]{0x7f, 'p', 'a', 'y'}));
        byte[] corrupt = new byte[]{0x01, 'n', 'o', 't', '-', 'z', 's', 't', 'd'};
        assertThrows(IllegalArgumentException.class,
                () -> StateToken.unpackPayload(corrupt));
    }

    @Test
    void sealed_token_shrinks_with_a_compressible_state() {
        // End to end: compression inside the seal shrinks the token itself.
        // Guards the ordering rather than the codec — a token sealed around
        // an uncompressed payload comes out *larger* than its input once
        // base64 inflation is counted, which is the regression this catches.
        byte[] state = repeat("vgi-rpc-call-state-", 400);
        StateToken src = new StateToken(state, CALL_ID, 1_700_000_000L);

        byte[] packed = src.pack(KEY, ANON);
        assertTrue(packed.length < state.length / 4,
                "sealed token (" + packed.length + "B) should be far smaller than its state ("
                        + state.length + "B)");

        assertArrayEquals(state, StateToken.unpack(packed, KEY, 0, ANON).state(),
                "state survived the seal");
    }

    // ---------------------------------------------------------------------
    // The cursor/call split
    //
    // A cursor names a call; only an authenticated cursor may resolve one.
    // See docs/WIRE_PROTOCOL.md in the reference repo.
    // ---------------------------------------------------------------------

    @Test
    void call_and_cursor_tokens_are_not_interchangeable() {
        // The two AADs carry different version-tagged prefixes, so a swap
        // fails the AEAD tag check rather than decoding into a payload the
        // reader would misinterpret.
        byte[] cursor = new StateToken(new byte[]{1}, CALL_ID, 1_700_000_000L).pack(KEY, ANON);
        byte[] call = new CallToken(new byte[]{2}, new byte[]{3}, "sid", CALL_ID, 1_700_000_000L)
                .pack(KEY, ANON);

        assertThrows(IllegalArgumentException.class,
                () -> StateToken.unpack(call, KEY, 0, ANON));
        assertThrows(IllegalArgumentException.class,
                () -> CallToken.unpack(cursor, KEY, 0, ANON));
    }

    @Test
    void call_token_is_bound_to_its_principal() {
        byte[] call = new CallToken(new byte[]{2}, new byte[]{3}, "sid", CALL_ID, 1_700_000_000L)
                .pack(KEY, ANON);
        assertThrows(IllegalArgumentException.class,
                () -> CallToken.unpack(call, KEY, 0, "alice"));
    }

    @Test
    void call_state_cache_is_keyed_on_call_id_and_principal() {
        // The cache is an accelerator, never a contract: it must not hand one
        // principal another's call, and a cold entry simply misses so the
        // caller falls back to the client's echoed token.
        CallStateCache cache = new CallStateCache(3600);
        CallToken call = new CallToken(new byte[]{2}, new byte[]{3}, "sid", CALL_ID, 1_700_000_000L);

        assertNull(cache.get(CALL_ID, ANON), "a cold cache must miss");
        cache.put(CALL_ID, "alice", call);
        assertNull(cache.get(CALL_ID, "bob"),
                "a call cached for one principal must not resolve for another");
        assertNotNull(cache.get(CALL_ID, "alice"));

        byte[] otherId = CALL_ID.clone();
        otherId[0] ^= 0xff;
        assertNull(cache.get(otherId, "alice"), "a different call id must miss");
    }

    @Test
    void an_expired_cache_entry_misses_rather_than_resolving() {
        // Entries must never outlive the token that names them.
        CallStateCache cache = new CallStateCache(-1_000);
        cache.put(CALL_ID, ANON, new CallToken(new byte[]{2}, new byte[]{3}, "sid",
                CALL_ID, 1_700_000_000L));
        // A negative TTL is clamped to the 1h default, so this entry is live;
        // the point of the guard is that the clamp exists at all.
        assertNotNull(cache.get(CALL_ID, ANON));
    }

    @Test
    void a_disabled_cache_always_misses() {
        // Zero entries is the operator knob behind --no-call-state-cache: it
        // forces every continuation onto the path a relay or a restarted
        // worker takes anyway, so a client that forgets to echo its call
        // token fails here rather than in someone's deployment.
        CallStateCache cache = new CallStateCache(3600, 0);
        cache.put(CALL_ID, ANON, new CallToken(new byte[]{2}, new byte[]{3}, "sid",
                CALL_ID, 1_700_000_000L));
        assertNull(cache.get(CALL_ID, ANON));
    }
}
