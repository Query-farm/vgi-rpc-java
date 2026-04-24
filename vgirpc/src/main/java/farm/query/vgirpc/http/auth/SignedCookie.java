// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http.auth;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

/**
 * HMAC-SHA256 signed cookie primitive for OAuth session state. Wire layout:
 *
 * <pre>
 *   payload (arbitrary bytes) . base64url(mac)
 * </pre>
 *
 * <p>The {@code .} delimiter is chosen so cookie values stay single-line and
 * do not collide with URL-safe base64 characters (which use {@code -} and
 * {@code _}). A {@link TimestampedPayload} helper offers expiry enforcement.</p>
 */
public final class SignedCookie {

    private static final Base64.Encoder ENC = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DEC = Base64.getUrlDecoder();

    private SignedCookie() {}

    /** Sign {@code payload} with {@code key}. Returns {@code payloadB64 "." macB64}. */
    public static String sign(byte[] payload, byte[] key) {
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(key, "key");
        byte[] payloadB64 = ENC.encode(payload);
        byte[] mac = Crypto.hmacSha256(key, payloadB64);
        return new String(payloadB64, StandardCharsets.US_ASCII) + '.' + ENC.encodeToString(mac);
    }

    /**
     * Verify + decode a signed cookie value. Throws {@link IllegalArgumentException}
     * on missing separator, bad base64, or signature mismatch.
     */
    public static byte[] verify(String cookieValue, byte[] key) {
        Objects.requireNonNull(cookieValue, "cookieValue");
        int dot = cookieValue.lastIndexOf('.');
        if (dot < 0) throw new IllegalArgumentException("malformed signed cookie: missing '.'");
        byte[] payloadB64 = cookieValue.substring(0, dot).getBytes(StandardCharsets.US_ASCII);
        byte[] expected = Crypto.hmacSha256(key, payloadB64);
        byte[] actual;
        try {
            actual = DEC.decode(cookieValue.substring(dot + 1));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("malformed signed cookie: bad base64 mac");
        }
        if (!Crypto.constantTimeEquals(expected, actual)) {
            throw new IllegalArgumentException("signed cookie signature verification failed");
        }
        try {
            return DEC.decode(cookieValue.substring(0, dot));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("malformed signed cookie: bad base64 payload");
        }
    }

    /** Payload helper that prepends an 8-byte little-endian Unix-seconds timestamp for TTL enforcement. */
    public static final class TimestampedPayload {

        private final byte[] payload;
        private final long createdAt;

        public TimestampedPayload(byte[] payload, long createdAt) {
            this.payload = payload;
            this.createdAt = createdAt;
        }

        public byte[] payload() { return payload; }
        public long createdAt() { return createdAt; }

        /** Pack {@code payload} with a current timestamp for {@link SignedCookie#sign}. */
        public static byte[] pack(byte[] payload) {
            long now = System.currentTimeMillis() / 1000;
            byte[] out = new byte[8 + payload.length];
            for (int i = 0; i < 8; i++) out[i] = (byte) (now >>> (i * 8));
            System.arraycopy(payload, 0, out, 8, payload.length);
            return out;
        }

        /** Unpack bytes produced by {@link #pack}; throws when older than {@code ttlSeconds}. */
        public static TimestampedPayload unpack(byte[] bytes, long ttlSeconds) {
            if (bytes == null || bytes.length < 8) {
                throw new IllegalArgumentException("malformed timestamped payload");
            }
            long ts = 0;
            for (int i = 0; i < 8; i++) ts |= ((long) (bytes[i] & 0xFF)) << (i * 8);
            if (ttlSeconds > 0) {
                long age = (System.currentTimeMillis() / 1000) - ts;
                if (age > ttlSeconds) {
                    throw new IllegalArgumentException("cookie payload expired (age=" + age + "s)");
                }
            }
            byte[] payload = new byte[bytes.length - 8];
            System.arraycopy(bytes, 8, payload, 0, payload.length);
            return new TimestampedPayload(payload, ts);
        }
    }

}
