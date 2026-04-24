// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.http.auth.Crypto;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

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
 *   [32 bytes: HMAC-SHA256(perPrincipalKey, all above)]
 * </pre>
 * The whole thing is base64 encoded for UTF-8-safe Arrow custom metadata.
 *
 * <p>The MAC is keyed by {@link Crypto#deriveStateTokenKey}(signingKey, principal)
 * rather than by {@code signingKey} directly. This binds the token to the
 * authenticated principal: an attacker who sniffs or exfiltrates user A's token
 * cannot replay it as user B — the verifier derives B's key and HMAC fails.
 * Anonymous streams bind to the empty string; as long as {@code /init} and
 * {@code /exchange} agree on the principal, the token round-trips.</p>
 */
public record StateToken(
        byte[] state,
        byte[] outputSchema,
        byte[] inputSchema,
        String streamId,
        long createdAt) {

    private static final byte VERSION = 3;
    private static final int HMAC_LEN = 32;

    public StateToken {
        state = state.clone();
        outputSchema = outputSchema.clone();
        inputSchema = inputSchema.clone();
        streamId = streamId != null ? streamId : "";
    }

    @Override public byte[] state()        { return state.clone(); }
    @Override public byte[] outputSchema() { return outputSchema.clone(); }
    @Override public byte[] inputSchema()  { return inputSchema.clone(); }

    /**
     * Serialise + HMAC-sign + base64-encode the token. The signing key is
     * derived per-{@code principal} so the token cannot be replayed by a
     * different user; pass {@code ""} for anonymous streams.
     */
    public byte[] pack(byte[] signingKey, String principal) {
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
        byte[] mac = Crypto.hmacSha256(Crypto.deriveStateTokenKey(signingKey, principal), payloadBytes);
        byte[] full = new byte[payloadBytes.length + HMAC_LEN];
        System.arraycopy(payloadBytes, 0, full, 0, payloadBytes.length);
        System.arraycopy(mac, 0, full, payloadBytes.length, HMAC_LEN);
        return Base64.getEncoder().encode(full);
    }

    /**
     * Decode + verify + unpack the token. The verifier derives the same
     * per-{@code principal} key as {@link #pack}; a mismatched principal
     * fails HMAC verification identically to any other tamper.
     * TTL disabled when {@code ttlSeconds <= 0}.
     */
    public static StateToken unpack(byte[] b64, byte[] signingKey, long ttlSeconds, String principal) {
        byte[] raw = Base64.getDecoder().decode(b64);
        if (raw.length < 1 + 8 + 4 * 4 + HMAC_LEN) {
            throw new IllegalArgumentException("Malformed state token");
        }
        int payloadEnd = raw.length - HMAC_LEN;
        byte[] payloadBytes = new byte[payloadEnd];
        System.arraycopy(raw, 0, payloadBytes, 0, payloadEnd);
        byte[] receivedMac = new byte[HMAC_LEN];
        System.arraycopy(raw, payloadEnd, receivedMac, 0, HMAC_LEN);
        byte[] expectedMac = Crypto.hmacSha256(Crypto.deriveStateTokenKey(signingKey, principal), payloadBytes);
        if (!Crypto.constantTimeEquals(receivedMac, expectedMac)) {
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
}
