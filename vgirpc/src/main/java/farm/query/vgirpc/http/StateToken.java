// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import com.github.luben.zstd.Zstd;

import farm.query.vgirpc.http.auth.Crypto;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * HTTP streaming state token: AEAD-sealed envelope holding stream state,
 * schemas, and a stream id so the server can recover on the next exchange.
 *
 * <p>Wire format (v5):
 * <pre>
 *   base64(
 *     [1 byte:  version = 5]
 *     [12 bytes: ChaCha20-Poly1305 nonce (random)]
 *     [..]      ciphertext + Poly1305 tag
 *               sealed payload:
 *                 [1 byte:   codec — 0x00 raw, 0x01 zstd]
 *                 [..]       the plaintext below, compressed per codec
 *               plaintext:
 *                 [8 bytes:  created_at uint64 LE]
 *                 [4 bytes:  state_len uint32 LE]   [state bytes]
 *                 [4 bytes:  schema_len uint32 LE]  [output_schema bytes]
 *                 [4 bytes:  input_schema_len LE]   [input_schema bytes]
 *                 [4 bytes:  stream_id_len LE]      [stream_id utf8]
 *   )
 * </pre>
 *
 * <p>Compression happens <em>inside</em> the seal, and the order is the whole
 * point: once sealed, a token is ciphertext, so the HTTP body codec can no
 * longer find any redundancy in it — it recovers only the slack base64 adds,
 * never the state's own structure. Compressing first reaches the real
 * redundancy. Compression is skipped when it does not pay, so a small token
 * never grows beyond its plaintext plus the one tag byte.</p>
 *
 * <p>v4 sealed the plaintext directly. The version bump matters for rolling
 * deploys: a v4 plaintext starts straight into {@code created_at}, so a v5
 * reader would take its first byte as a codec tag and mis-frame the rest.
 * Rejecting the old version outright turns that into the same clean failure
 * as any other stale token. The AAD prefix stays at {@code v4}, matching the
 * Python reference, which likewise bumped its token version without
 * regenerating its AAD.</p>
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
        byte[] callId,
        long createdAt) {

    private static final byte VERSION = 6;
    private static final int VERSION_LEN = 1;

    /** Prefix mixed into AEAD AAD to bind tokens to a format generation. */
    private static final byte[] AAD_PREFIX = "vgi_rpc.state.v4\0".getBytes(StandardCharsets.UTF_8);

    /** Codec tags for the sealed payload. See {@link #packPayload}. */
    private static final byte CODEC_RAW = 0x00;
    private static final byte CODEC_ZSTD = 0x01;

    /**
     * Matches the Python reference's choice. At token payload sizes this
     * measures the same speed as level 1 and slightly smaller, while the
     * levels that compress materially better cost many times the CPU for a
     * few hundred bytes.
     */
    private static final int ZSTD_LEVEL = 3;

    /**
     * Bounds decompression. The payload is authenticated before it is ever
     * decompressed, so this guards against a framework bug rather than an
     * attacker — but an unbounded decompress on a request path is not worth
     * having.
     */
    private static final long MAX_PLAINTEXT_BYTES = 64L << 20;

    public StateToken {
        state = state.clone();
        callId = callId.clone();
    }

    @Override public byte[] state()  { return state.clone(); }
    @Override public byte[] callId() { return callId.clone(); }

    /**
     * Serialise, AEAD-seal, and base64-encode the token. The AAD binds the
     * token to {@code principal} so it cannot be opened by a different
     * caller; pass {@code ""} (or {@code null}) for anonymous streams.
     */
    public byte[] pack(byte[] tokenKey, String principal) {
        int payloadLen = 8 + Tokens.CALL_ID_LEN + 4 + state.length;
        ByteBuffer payload = ByteBuffer.allocate(payloadLen).order(ByteOrder.LITTLE_ENDIAN);
        payload.putLong(createdAt);
        payload.put(callId);
        putSegment(payload, state);
        byte[] sealed = Crypto.chacha20Poly1305Seal(tokenKey, packPayload(payload.array()), aad(principal));
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
        byte[] opened;
        try {
            opened = Crypto.chacha20Poly1305Open(tokenKey, sealed, aad(principal));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("State token signature verification failed", e);
        }
        // Decompress only after authentication: nothing an attacker supplies
        // reaches the decoder without the token key.
        byte[] plaintext = unpackPayload(opened);
        if (plaintext.length < 8 + Tokens.CALL_ID_LEN) {
            throw new IllegalArgumentException("Malformed state token");
        }
        ByteBuffer bb = ByteBuffer.wrap(plaintext).order(ByteOrder.LITTLE_ENDIAN);
        long createdAt = bb.getLong();
        byte[] callId = new byte[Tokens.CALL_ID_LEN];
        bb.get(callId);
        if (ttlSeconds > 0) {
            long now = System.currentTimeMillis() / 1000;
            if (now - createdAt > ttlSeconds) {
                throw new TokenExpiredException("State token expired (age=" + (now - createdAt)
                        + "s, ttl=" + ttlSeconds + "s)");
            }
        }
        byte[] state = getSegment(bb);
        return new StateToken(state, callId, createdAt);
    }

    /**
     * Build the AAD that binds a state token to its caller. Anonymous and
     * authenticated tokens produce distinct AAD strings so an anonymous
     * token cannot be presented under a named identity (and vice versa).
     */
    private static byte[] aad(String principal) {
        return Tokens.aad(AAD_PREFIX, principal);
    }

    /**
     * Compress a token payload and tag which codec was used.
     *
     * <p>Delegates to {@link Tokens}, which both token kinds share. Kept here
     * as the package-private entry point the token tests exercise.</p>
     */
    static byte[] packPayload(byte[] plaintext) {
        return Tokens.packPayload(plaintext);
    }

    /** Reverse {@link #packPayload}. */
    static byte[] unpackPayload(byte[] data) {
        return Tokens.unpackPayload(data);
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
