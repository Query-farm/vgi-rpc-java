// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http.auth;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class SignedCookieTest {

    private static final byte[] KEY = "super-secret-signing-key-000000000".getBytes();
    private static final byte[] OTHER_KEY = "a-different-key-11111111111111111111".getBytes();

    @Test
    void sign_and_verify_roundtrip() {
        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        String cookie = SignedCookie.sign(payload, KEY);
        assertTrue(cookie.contains("."));
        byte[] back = SignedCookie.verify(cookie, KEY);
        assertArrayEquals(payload, back);
    }

    @Test
    void tampered_payload_rejected() {
        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        String cookie = SignedCookie.sign(payload, KEY);
        int dot = cookie.lastIndexOf('.');
        char ch = cookie.charAt(dot - 1);
        char flip = ch == 'X' ? 'Y' : 'X';
        String tampered = cookie.substring(0, dot - 1) + flip + cookie.substring(dot);
        assertThrows(IllegalArgumentException.class, () -> SignedCookie.verify(tampered, KEY));
    }

    @Test
    void wrong_key_rejected() {
        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        String cookie = SignedCookie.sign(payload, KEY);
        assertThrows(IllegalArgumentException.class, () -> SignedCookie.verify(cookie, OTHER_KEY));
    }

    @Test
    void missing_separator_rejected() {
        assertThrows(IllegalArgumentException.class, () -> SignedCookie.verify("nodots", KEY));
    }

    @Test
    void timestamped_payload_fresh_roundtrip() {
        byte[] data = "session-state".getBytes(StandardCharsets.UTF_8);
        byte[] wrapped = SignedCookie.TimestampedPayload.pack(data);
        String cookie = SignedCookie.sign(wrapped, KEY);
        byte[] decoded = SignedCookie.verify(cookie, KEY);
        SignedCookie.TimestampedPayload tp = SignedCookie.TimestampedPayload.unpack(decoded, 3600);
        assertArrayEquals(data, tp.payload());
    }

    @Test
    void timestamped_payload_expired_rejected() {
        // Hand-craft a 1-hour-old timestamp prefix
        byte[] data = "old".getBytes(StandardCharsets.UTF_8);
        long staleTs = (System.currentTimeMillis() / 1000) - 7200;  // 2h ago
        byte[] wrapped = new byte[8 + data.length];
        for (int i = 0; i < 8; i++) wrapped[i] = (byte) (staleTs >>> (i * 8));
        System.arraycopy(data, 0, wrapped, 8, data.length);
        assertThrows(IllegalArgumentException.class,
                () -> SignedCookie.TimestampedPayload.unpack(wrapped, 3600));
    }
}
