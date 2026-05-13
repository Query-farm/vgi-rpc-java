// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.http.auth.Crypto;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * HTTP streaming state token: AEAD-sealed envelope holding stream state,
 * schemas, and a stream id so the server can recover on the next exchange.
 *
 * <p>Wire format (v4):
 * <pre>
 *   base64(
 *     [1 byte:  version = 4]
 *     [12 bytes: ChaCha20-Poly1305 nonce (random)]
 *     [..]      ciphertext + Poly1305 tag
 *               plaintext:
 *                 [8 bytes:  created_at uint64 LE]
 *                 [4 bytes:  state_len uint32 LE]   [state bytes]
 *                 [4 bytes:  schema_len uint32 LE]  [output_schema bytes]
 *                 [4 bytes:  input_schema_len LE]   [input_schema bytes]
 *                 [4 bytes:  stream_id_len LE]      [stream_id utf8]
 *   )
 * </pre>
 *
 * <p>The {@code created_at} timestamp lives inside the ciphertext so TTL
 * enforcement runs after authenticity is established. The version byte is
 * informational (a self-describing format marker); a tampered version byte
 * still fails decryption because we use the matching algorithm for that
 * version. The {@code principal} is bound via AEAD associated data —
 * a token minted for one identity fails decryption when presented by
 * another, with no per-principal key derivation needed.</p>
 */
public record StateToken(
        byte[] state,
        byte[] outputSchema,
        byte[] inputSchema,
        String streamId,
        long createdAt) {

    private static final byte VERSION = 4;
    private static final int VERSION_LEN = 1;

    /** Prefix mixed into AEAD AAD to bind tokens to a format generation. */
    private static final byte[] AAD_PREFIX = "vgi_rpc.state.v4\0".getBytes(StandardCharsets.UTF_8);

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
     * Serialise, AEAD-seal, and base64-encode the token. The AAD binds the
     * token to {@code principal} so it cannot be opened by a different
     * caller; pass {@code ""} (or {@code null}) for anonymous streams.
     */
    public byte[] pack(byte[] tokenKey, String principal) {
        byte[] streamIdBytes = streamId.getBytes(StandardCharsets.UTF_8);
        int payloadLen = 8
                + 4 + state.length
                + 4 + outputSchema.length
                + 4 + inputSchema.length
                + 4 + streamIdBytes.length;
        ByteBuffer payload = ByteBuffer.allocate(payloadLen).order(ByteOrder.LITTLE_ENDIAN);
        payload.putLong(createdAt);
        putSegment(payload, state);
        putSegment(payload, outputSchema);
        putSegment(payload, inputSchema);
        putSegment(payload, streamIdBytes);
        byte[] sealed = Crypto.chacha20Poly1305Seal(tokenKey, payload.array(), aad(principal));
        byte[] wire = new byte[VERSION_LEN + sealed.length];
        wire[0] = VERSION;
        System.arraycopy(sealed, 0, wire, VERSION_LEN, sealed.length);
        return Base64.getEncoder().encode(wire);
    }

    /**
     * Decode + open + unpack the token. Decryption (which checks the
     * Poly1305 tag) authenticates the payload; any tampering, wrong key,
     * or AAD mismatch (e.g. cross-principal replay) surfaces as an
     * IllegalArgumentException with a uniform "signature" message so
     * callers cannot distinguish failure modes via timing or message.
     * TTL disabled when {@code ttlSeconds <= 0}.
     */
    public static StateToken unpack(byte[] b64, byte[] tokenKey, long ttlSeconds, String principal) {
        byte[] raw;
        try {
            raw = Base64.getDecoder().decode(b64);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Malformed state token", e);
        }
        if (raw.length < VERSION_LEN + Crypto.AEAD_NONCE_LEN + Crypto.AEAD_TAG_LEN) {
            throw new IllegalArgumentException("Malformed state token");
        }
        byte version = raw[0];
        if (version != VERSION) {
            throw new IllegalArgumentException("Unsupported state token version " + version
                    + " (expected " + VERSION + ")");
        }
        byte[] sealed = new byte[raw.length - VERSION_LEN];
        System.arraycopy(raw, VERSION_LEN, sealed, 0, sealed.length);
        byte[] plaintext;
        try {
            plaintext = Crypto.chacha20Poly1305Open(tokenKey, sealed, aad(principal));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("State token signature verification failed", e);
        }
        if (plaintext.length < 8) {
            throw new IllegalArgumentException("Malformed state token");
        }
        ByteBuffer bb = ByteBuffer.wrap(plaintext).order(ByteOrder.LITTLE_ENDIAN);
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

    /**
     * Build the AAD that binds a state token to its caller. Anonymous and
     * authenticated tokens produce distinct AAD strings so an anonymous
     * token cannot be presented under a named identity (and vice versa).
     */
    private static byte[] aad(String principal) {
        String p = principal != null ? principal : "";
        byte[] tail;
        if (p.isEmpty()) {
            tail = new byte[]{0x00, 'a', 'n', 'o', 'n', 'y', 'm', 'o', 'u', 's'};
        } else {
            byte[] pBytes = p.getBytes(StandardCharsets.UTF_8);
            tail = new byte[1 + pBytes.length];
            tail[0] = 0x01;
            System.arraycopy(pBytes, 0, tail, 1, pBytes.length);
        }
        byte[] out = new byte[AAD_PREFIX.length + tail.length];
        System.arraycopy(AAD_PREFIX, 0, out, 0, AAD_PREFIX.length);
        System.arraycopy(tail, 0, out, AAD_PREFIX.length, tail.length);
        return out;
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
