// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

/**
 * HTTP streaming state token: HMAC-SHA256 signed envelope holding stream state,
 * schemas, and a stream id so the server can recover on the next exchange.
 *
 * <p>Wire-compatible with Python {@code vgi_rpc.http.server._state_token} (v3):
 * <pre>
 *   [1 byte:  version=3]
 *   [8 bytes: created_at uint64 LE]
 *   [4 bytes: state_len uint32 LE] [state bytes]
 *   [4 bytes: schema_len uint32 LE] [output_schema bytes]
 *   [4 bytes: input_schema_len uint32 LE] [input_schema bytes]
 *   [4 bytes: stream_id_len uint32 LE] [stream_id utf8]
 *   [32 bytes: HMAC-SHA256(key, all above)]
 * </pre>
 * The whole thing is base64 encoded for UTF-8-safe Arrow custom metadata.
 */
public final class StateToken {

    private static final byte VERSION = 3;
    private static final int HMAC_LEN = 32;

    public final byte[] state;
    public final byte[] outputSchema;
    public final byte[] inputSchema;
    public final String streamId;
    public final long createdAt;

    public StateToken(byte[] state, byte[] outputSchema, byte[] inputSchema, String streamId, long createdAt) {
        this.state = state;
        this.outputSchema = outputSchema;
        this.inputSchema = inputSchema;
        this.streamId = streamId != null ? streamId : "";
        this.createdAt = createdAt;
    }

    /** Serialise + HMAC-sign + base64-encode the token. */
    public byte[] pack(byte[] signingKey) {
        byte[] streamIdBytes = streamId.getBytes(StandardCharsets.UTF_8);
        int payloadLen = 1 + 8
                + 4 + state.length
                + 4 + outputSchema.length
                + 4 + inputSchema.length
                + 4 + streamIdBytes.length;
        ByteBuffer payload = ByteBuffer.allocate(payloadLen).order(ByteOrder.LITTLE_ENDIAN);
        payload.put(VERSION);
        payload.putLong(createdAt);
        putSegment(payload, state);
        putSegment(payload, outputSchema);
        putSegment(payload, inputSchema);
        putSegment(payload, streamIdBytes);
        byte[] payloadBytes = payload.array();
        byte[] mac = hmac(signingKey, payloadBytes);
        byte[] full = new byte[payloadBytes.length + HMAC_LEN];
        System.arraycopy(payloadBytes, 0, full, 0, payloadBytes.length);
        System.arraycopy(mac, 0, full, payloadBytes.length, HMAC_LEN);
        return Base64.getEncoder().encode(full);
    }

    /** Decode + verify + unpack the token. TTL disabled when {@code ttlSeconds <= 0}. */
    public static StateToken unpack(byte[] b64, byte[] signingKey, long ttlSeconds) {
        byte[] raw = Base64.getDecoder().decode(b64);
        if (raw.length < 1 + 8 + 4 * 4 + HMAC_LEN) {
            throw new IllegalArgumentException("Malformed state token");
        }
        int payloadEnd = raw.length - HMAC_LEN;
        byte[] payloadBytes = new byte[payloadEnd];
        System.arraycopy(raw, 0, payloadBytes, 0, payloadEnd);
        byte[] receivedMac = new byte[HMAC_LEN];
        System.arraycopy(raw, payloadEnd, receivedMac, 0, HMAC_LEN);
        byte[] expectedMac = hmac(signingKey, payloadBytes);
        if (!constantTimeEquals(receivedMac, expectedMac)) {
            throw new IllegalArgumentException("State token signature verification failed");
        }
        ByteBuffer bb = ByteBuffer.wrap(payloadBytes).order(ByteOrder.LITTLE_ENDIAN);
        byte version = bb.get();
        if (version != VERSION) {
            throw new IllegalArgumentException("Unsupported state token version " + version + " (expected " + VERSION + ")");
        }
        long createdAt = bb.getLong();
        if (ttlSeconds > 0) {
            long now = System.currentTimeMillis() / 1000;
            if (now - createdAt > ttlSeconds) {
                throw new TokenExpiredException("State token expired (age=" + (now - createdAt)
                        + "s, ttl=" + ttlSeconds + "s)");
            }
        }
        byte[] state = getSegment(bb);
        byte[] outputSchema = getSegment(bb);
        byte[] inputSchema = getSegment(bb);
        byte[] streamIdBytes = getSegment(bb);
        return new StateToken(state, outputSchema, inputSchema,
                new String(streamIdBytes, StandardCharsets.UTF_8), createdAt);
    }

    /** Backwards-compat overload: no TTL enforcement. */
    public static StateToken unpack(byte[] b64, byte[] signingKey) {
        return unpack(b64, signingKey, 0);
    }

    private static void putSegment(ByteBuffer b, byte[] seg) {
        b.putInt(seg.length);
        b.put(seg);
    }

    private static byte[] getSegment(ByteBuffer b) {
        int len = b.getInt();
        if (len < 0 || len > b.remaining()) throw new IllegalArgumentException("Malformed segment");
        byte[] out = new byte[len];
        b.get(out);
        return out;
    }

    private static byte[] hmac(byte[] key, byte[] data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(Objects.requireNonNull(key), "HmacSHA256"));
            return mac.doFinal(data);
        } catch (Exception e) {
            throw new RuntimeException("HMAC failed", e);
        }
    }

    private static boolean constantTimeEquals(byte[] a, byte[] b) {
        if (a.length != b.length) return false;
        int r = 0;
        for (int i = 0; i < a.length; i++) r |= a[i] ^ b[i];
        return r == 0;
    }
}
