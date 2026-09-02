// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.http.auth.Crypto;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * The half of a stream's state that is fixed for the life of the call: the
 * resolved schemas and the stream id, plus the {@code callId} that binds this
 * call to its cursors.
 *
 * <p>Minted once, by {@code /init}, under {@code vgi_rpc.call_state#b64}, and
 * <strong>never re-issued</strong> — only {@link StateToken}, the cursor,
 * comes back per turn. The client echoes it on every subsequent request: the
 * server may resolve the call from a per-process cache while one is warm, but
 * a continuation landing on a process that never saw the {@code /init} has
 * only the client's copy to work from.</p>
 *
 * <p>Wire format (v1):
 * <pre>
 *   base64(
 *     [1 byte:   version = 1]
 *     [12 bytes: ChaCha20-Poly1305 nonce (random)]
 *     [..]       ciphertext + Poly1305 tag
 *                sealed payload:
 *                  [1 byte:  codec — 0x00 raw, 0x01 zstd]
 *                  [..]      the plaintext below, compressed per codec
 *                plaintext:
 *                  [8 bytes: created_at uint64 LE]
 *                  [16 bytes: call_id]
 *                  [4 bytes: schema_len LE]       [output_schema bytes]
 *                  [4 bytes: input_schema_len LE] [input_schema bytes]
 *                  [4 bytes: stream_id_len LE]    [stream_id utf8]
 *   )
 * </pre>
 */
public record CallToken(
        byte[] outputSchema,
        byte[] inputSchema,
        String streamId,
        byte[] callId,
        long createdAt,
        long responseLimitBytes) {

    private static final byte VERSION = 2;
    private static final int VERSION_LEN = 1;

    /**
     * Prefix mixed into AEAD AAD. Distinct from the cursor's, so a call token
     * and a cursor token are not interchangeable even for the same principal.
     */
    private static final byte[] AAD_PREFIX = "vgi_rpc.call.v1\0".getBytes(StandardCharsets.UTF_8);
    private static final byte[] BOUND_AAD_PREFIX = "vgi_rpc.call.v2\0".getBytes(StandardCharsets.UTF_8);

    public CallToken {
        outputSchema = outputSchema.clone();
        inputSchema = inputSchema.clone();
        callId = callId.clone();
        streamId = streamId != null ? streamId : "";
    }

    /** Compatibility constructor for callers that do not negotiate a response budget. */
    public CallToken(byte[] outputSchema, byte[] inputSchema, String streamId,
                     byte[] callId, long createdAt) {
        this(outputSchema, inputSchema, streamId, callId, createdAt, Long.MAX_VALUE);
    }

    @Override public byte[] outputSchema() { return outputSchema.clone(); }
    @Override public byte[] inputSchema()  { return inputSchema.clone(); }
    @Override public byte[] callId()       { return callId.clone(); }

    /** Serialise, AEAD-seal, and base64-encode the token. */
    public byte[] pack(byte[] tokenKey, String principal) {
        return packWithAad(tokenKey, Tokens.aad(AAD_PREFIX, principal));
    }

    /** Seal using domain, principal, and peer-evidence binding when present. */
    public byte[] pack(byte[] tokenKey, AuthContext auth) {
        return packWithAad(tokenKey, Tokens.aad(AAD_PREFIX, BOUND_AAD_PREFIX, auth));
    }

    private byte[] packWithAad(byte[] tokenKey, byte[] aad) {
        byte[] streamIdBytes = streamId.getBytes(StandardCharsets.UTF_8);
        int payloadLen = 8 + 8 + Tokens.CALL_ID_LEN
                + 4 + outputSchema.length
                + 4 + inputSchema.length
                + 4 + streamIdBytes.length;
        ByteBuffer payload = ByteBuffer.allocate(payloadLen).order(ByteOrder.LITTLE_ENDIAN);
        payload.putLong(createdAt);
        payload.putLong(responseLimitBytes);
        payload.put(callId);
        putSegment(payload, outputSchema);
        putSegment(payload, inputSchema);
        putSegment(payload, streamIdBytes);
        byte[] sealed = Crypto.chacha20Poly1305Seal(
                tokenKey, Tokens.packPayload(payload.array()), aad);
        byte[] wire = new byte[VERSION_LEN + sealed.length];
        wire[0] = VERSION;
        System.arraycopy(sealed, 0, wire, VERSION_LEN, sealed.length);
        return Base64.getEncoder().encode(wire);
    }

    /**
     * Decode + open + unpack. Every tampering, wrong-key, or AAD-mismatch
     * failure surfaces as the same uniform "signature" message so callers
     * cannot distinguish failure modes via timing or content. TTL disabled
     * when {@code ttlSeconds <= 0}.
     */
    public static CallToken unpack(byte[] b64, byte[] tokenKey, long ttlSeconds, String principal) {
        return unpackWithAad(b64, tokenKey, ttlSeconds, Tokens.aad(AAD_PREFIX, principal));
    }

    /** Open using domain, principal, and peer-evidence binding when present. */
    public static CallToken unpack(byte[] b64, byte[] tokenKey, long ttlSeconds, AuthContext auth) {
        return unpackWithAad(b64, tokenKey, ttlSeconds, Tokens.aad(AAD_PREFIX, BOUND_AAD_PREFIX, auth));
    }

    private static CallToken unpackWithAad(byte[] b64, byte[] tokenKey, long ttlSeconds, byte[] aad) {
        byte[] raw;
        try {
            raw = Base64.getDecoder().decode(b64);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Malformed state token", e);
        }
        if (raw.length < VERSION_LEN + Crypto.AEAD_NONCE_LEN + Crypto.AEAD_TAG_LEN) {
            throw new IllegalArgumentException("Malformed state token");
        }
        if (raw[0] != VERSION) {
            throw new IllegalArgumentException("Unsupported call token version " + raw[0]
                    + " (expected " + VERSION + ")");
        }
        byte[] sealed = new byte[raw.length - VERSION_LEN];
        System.arraycopy(raw, VERSION_LEN, sealed, 0, sealed.length);
        byte[] opened;
        try {
            opened = Crypto.chacha20Poly1305Open(tokenKey, sealed, aad);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("State token signature verification failed", e);
        }
        // Decompress only after authentication: nothing an attacker supplies
        // reaches the decoder without the token key.
        byte[] plaintext = Tokens.unpackPayload(opened);
        if (plaintext.length < 16 + Tokens.CALL_ID_LEN) {
            throw new IllegalArgumentException("Malformed state token");
        }
        ByteBuffer bb = ByteBuffer.wrap(plaintext).order(ByteOrder.LITTLE_ENDIAN);
        long createdAt = bb.getLong();
        long responseLimitBytes = bb.getLong();
        byte[] callId = new byte[Tokens.CALL_ID_LEN];
        bb.get(callId);
        if (ttlSeconds > 0) {
            long now = System.currentTimeMillis() / 1000;
            if (now - createdAt > ttlSeconds) {
                throw new TokenExpiredException("Call token expired (age=" + (now - createdAt)
                        + "s, ttl=" + ttlSeconds + "s)");
            }
        }
        byte[] outputSchema = getSegment(bb);
        byte[] inputSchema = getSegment(bb);
        byte[] streamIdBytes = getSegment(bb);
        return new CallToken(outputSchema, inputSchema,
                new String(streamIdBytes, StandardCharsets.UTF_8), callId, createdAt,
                responseLimitBytes);
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
