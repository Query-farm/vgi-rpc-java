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
 * <p>Since v3 (protocol 3.4, STATE_REF_V1.md §8) the two schemas sit behind a
 * {@link farm.query.vgirpc.stateref.StateRef} so wide tables do not blow the
 * turn budget: inline in the common case, stored once (one GET per process per
 * stream, then the object cache) when they are large.</p>
 *
 * <p>Wire format (v3):
 * <pre>
 *   base64(
 *     [1 byte:   version = 3]
 *     [12 bytes: ChaCha20-Poly1305 nonce (random)]
 *     [..]       ciphertext + Poly1305 tag
 *                sealed payload:
 *                  [1 byte:  codec — 0x00 raw, 0x01 zstd]
 *                  [..]      the plaintext below, compressed per codec
 *                plaintext:
 *                  [8 bytes: created_at uint64 LE]
 *                  [8 bytes: response_limit_bytes uint64 LE]
 *                  [16 bytes: call_id]
 *                  [4 bytes: schemas_len LE]   [schemas_ref: StateRef bytes whose value is
 *                                               u32 LE len || output_schema || u32 LE len || input_schema]
 *                  [4 bytes: stream_id_len LE] [stream_id utf8]
 *   )
 * </pre>
 *
 * @param schemasRef the encoded {@code StateRef} holding both schemas
 * @param streamId the stream's lifecycle id
 * @param callId the stream's 16-byte call id
 * @param createdAt seconds since the epoch at mint
 * @param responseLimitBytes the negotiated response cap
 * @param sealedLength the token's sealed (base64-decoded) length when known, else 0; not
 *     part of the wire format
 */
public record CallToken(
        byte[] schemasRef,
        String streamId,
        byte[] callId,
        long createdAt,
        long responseLimitBytes,
        long sealedLength) {

    private static final byte VERSION = 3;
    private static final int VERSION_LEN = 1;

    /**
     * Prefix mixed into AEAD AAD. Distinct from the cursor's, so a call token
     * and a cursor token are not interchangeable even for the same principal.
     */
    private static final byte[] AAD_PREFIX = "vgi_rpc.call.v1\0".getBytes(StandardCharsets.UTF_8);
    private static final byte[] BOUND_AAD_PREFIX = "vgi_rpc.call.v2\0".getBytes(StandardCharsets.UTF_8);

    public CallToken {
        schemasRef = schemasRef.clone();
        callId = callId.clone();
        streamId = streamId != null ? streamId : "";
    }

    /**
     * A call token whose schemas are inline (no storage involved).
     *
     * @param outputSchema serialized output schema
     * @param inputSchema serialized input schema
     * @param streamId the stream id
     * @param callId the call id
     * @param createdAt seconds since the epoch
     * @param responseLimitBytes the negotiated response cap
     */
    public CallToken(byte[] outputSchema, byte[] inputSchema, String streamId,
                     byte[] callId, long createdAt, long responseLimitBytes) {
        this(inlineSchemasRef(outputSchema, inputSchema), streamId, callId, createdAt, responseLimitBytes, 0);
    }

    /** Compatibility constructor for callers that do not negotiate a response budget. */
    public CallToken(byte[] outputSchema, byte[] inputSchema, String streamId,
                     byte[] callId, long createdAt) {
        this(outputSchema, inputSchema, streamId, callId, createdAt, Long.MAX_VALUE);
    }

    /**
     * The {@code schemas_ref} value: {@code u32 LE len || output || u32 LE len || input}.
     *
     * @param outputSchema serialized output schema
     * @param inputSchema serialized input schema
     * @return the value bytes
     */
    public static byte[] packSchemas(byte[] outputSchema, byte[] inputSchema) {
        ByteBuffer b = ByteBuffer.allocate(8 + outputSchema.length + inputSchema.length)
                .order(ByteOrder.LITTLE_ENDIAN);
        putSegment(b, outputSchema);
        putSegment(b, inputSchema);
        return b.array();
    }

    /**
     * Reverse {@link #packSchemas}.
     *
     * @param value the value bytes
     * @return {@code [output, input]}
     * @throws IllegalArgumentException on a malformed value
     */
    public static byte[][] unpackSchemas(byte[] value) {
        ByteBuffer b = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN);
        try {
            byte[] out = getSegment(b);
            byte[] in = getSegment(b);
            if (b.hasRemaining()) throw new IllegalArgumentException("Malformed state token");
            return new byte[][] {out, in};
        } catch (java.nio.BufferUnderflowException e) {
            throw new IllegalArgumentException("Malformed state token");
        }
    }

    private static byte[] inlineSchemasRef(byte[] outputSchema, byte[] inputSchema) {
        byte[] payload = farm.query.vgirpc.stateref.TokenCodec.pack(packSchemas(outputSchema, inputSchema));
        byte[] ref = new byte[1 + payload.length];
        ref[0] = 0x01;
        System.arraycopy(payload, 0, ref, 1, payload.length);
        return ref;
    }

    /** @return the token with its sealed length recorded */
    CallToken withSealedLength(long length) {
        return new CallToken(schemasRef, streamId, callId, createdAt, responseLimitBytes, length);
    }

    /**
     * The output schema of an inline {@code schemas_ref} (tests, tools). The server resolves
     * through the {@code StateRef} machinery, which also handles the stored form.
     *
     * @return the serialized output schema
     * @throws IllegalStateException when the schemas are stored
     */
    public byte[] outputSchema() { return inlineSchemas()[0]; }

    /**
     * @return the serialized input schema of an inline {@code schemas_ref}
     * @throws IllegalStateException when the schemas are stored
     */
    public byte[] inputSchema() { return inlineSchemas()[1]; }

    private byte[][] inlineSchemas() {
        if (schemasRef.length == 0 || schemasRef[0] != 0x01) {
            throw new IllegalStateException("the call token's schemas are stored; resolve them through the server");
        }
        byte[] payload = new byte[schemasRef.length - 1];
        System.arraycopy(schemasRef, 1, payload, 0, payload.length);
        return unpackSchemas(farm.query.vgirpc.stateref.TokenCodec.unpack(payload,
                farm.query.vgirpc.stateref.TokenCodec.MAX_TOKEN_PLAINTEXT_BYTES));
    }

    @Override public byte[] schemasRef() { return schemasRef.clone(); }
    @Override public byte[] callId()     { return callId.clone(); }

    /** Serialise, AEAD-seal, and base64-encode the token. */
    public byte[] pack(byte[] tokenKey, String principal) {
        return packWithAad(tokenKey, Tokens.aad(AAD_PREFIX, principal));
    }

    /** Seal using domain, principal, and peer-evidence binding when present. */
    public byte[] pack(byte[] tokenKey, AuthContext auth) {
        return pack(tokenKey, auth, Tokens.SERVER_SCOPE);
    }

    /**
     * Seal bound additionally to the protocol the stream started on.
     *
     * @param tokenKey the server's token key
     * @param auth the caller's authenticated context
     * @param protocol the routing key that owns this stream
     * @return the base64 token
     */
    public byte[] pack(byte[] tokenKey, AuthContext auth, String protocol) {
        return packWithAad(tokenKey, Tokens.aad(AAD_PREFIX, BOUND_AAD_PREFIX, auth, protocol));
    }

    private byte[] packWithAad(byte[] tokenKey, byte[] aad) {
        byte[] streamIdBytes = streamId.getBytes(StandardCharsets.UTF_8);
        int payloadLen = 8 + 8 + Tokens.CALL_ID_LEN
                + 4 + schemasRef.length
                + 4 + streamIdBytes.length;
        ByteBuffer payload = ByteBuffer.allocate(payloadLen).order(ByteOrder.LITTLE_ENDIAN);
        payload.putLong(createdAt);
        payload.putLong(responseLimitBytes);
        payload.put(callId);
        putSegment(payload, schemasRef);
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
        return unpack(b64, tokenKey, ttlSeconds, auth, Tokens.SERVER_SCOPE);
    }

    /**
     * Open a token that was sealed against {@code protocol}.
     *
     * @param b64 the base64 token as presented
     * @param tokenKey the server's token key
     * @param ttlSeconds the token lifetime; {@code <= 0} disables the check
     * @param auth the caller's authenticated context
     * @param protocol the routing key the request path resolved to
     * @return the opened token
     */
    public static CallToken unpack(byte[] b64, byte[] tokenKey, long ttlSeconds, AuthContext auth,
                                   String protocol) {
        return unpackWithAad(b64, tokenKey, ttlSeconds,
                Tokens.aad(AAD_PREFIX, BOUND_AAD_PREFIX, auth, protocol));
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
        byte[] schemasRef = getSegment(bb);
        byte[] streamIdBytes = getSegment(bb);
        return new CallToken(schemasRef, new String(streamIdBytes, StandardCharsets.UTF_8), callId,
                createdAt, responseLimitBytes, raw.length);
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
