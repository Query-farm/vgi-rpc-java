// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.AuthContext;
import com.github.luben.zstd.Zstd;

import java.nio.charset.StandardCharsets;

/**
 * Framing shared by a stream's two state tokens.
 *
 * <p>A stream's state divides into a part fixed for the life of the call —
 * the resolved schemas and the stream id — and a part that advances per turn.
 * Carrying both in one token means every continuation re-serializes,
 * re-seals, re-opens and re-parses the fixed part, which for a typical stream
 * is most of the payload. So the two travel separately as {@link CallToken}
 * and {@link StateToken}; see {@code docs/WIRE_PROTOCOL.md} in the reference
 * repo, which requires the split.</p>
 *
 * <p>Both kinds share this envelope: compress under a codec tag, then seal.
 * Only the plaintext framing inside differs.</p>
 */
final class Tokens {

    private Tokens() {
    }

    /** Length of the random per-stream id minted at {@code /init}. */
    static final int CALL_ID_LEN = 16;

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

    /**
     * Build the AAD that binds a token to its caller, under a prefix that
     * also binds it to its <em>kind</em>.
     *
     * <p>The cursor and call prefixes differ deliberately, so the two are not
     * interchangeable even for the same principal: presenting one where the
     * other is expected fails the AEAD tag check rather than decoding into a
     * payload the reader would misinterpret. Anonymous and authenticated
     * tokens likewise produce distinct AAD strings.</p>
     */
    static byte[] aad(byte[] prefix, String principal) {
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
        byte[] out = new byte[prefix.length + tail.length];
        System.arraycopy(prefix, 0, out, 0, prefix.length);
        System.arraycopy(tail, 0, out, prefix.length, tail.length);
        return out;
    }

    /** New AAD generation used only when transport evidence contributes a digest. */
    static byte[] aad(byte[] legacyPrefix, byte[] boundPrefix, AuthContext auth) {
        String binding = evidenceBinding(auth);
        if (binding == null) return aad(legacyPrefix, auth != null ? auth.principal() : null);
        byte[] identity;
        if (auth == null || !auth.authenticated()) {
            identity = "\0anonymous".getBytes(StandardCharsets.UTF_8);
        } else {
            byte[] domain = (auth.domain() != null ? auth.domain() : "").getBytes(StandardCharsets.UTF_8);
            byte[] principal = (auth.principal() != null ? auth.principal() : "").getBytes(StandardCharsets.UTF_8);
            identity = new byte[1 + domain.length + 1 + principal.length];
            identity[0] = 0x01;
            System.arraycopy(domain, 0, identity, 1, domain.length);
            System.arraycopy(principal, 0, identity, 2 + domain.length, principal.length);
        }
        byte[] digest = binding.getBytes(StandardCharsets.UTF_8);
        byte[] out = new byte[boundPrefix.length + identity.length + 1 + digest.length];
        System.arraycopy(boundPrefix, 0, out, 0, boundPrefix.length);
        System.arraycopy(identity, 0, out, boundPrefix.length, identity.length);
        System.arraycopy(digest, 0, out, boundPrefix.length + identity.length + 1, digest.length);
        return out;
    }

    static String evidenceBinding(AuthContext auth) {
        Object value = auth != null ? auth.claims().get("peer_evidence_binding") : null;
        return value instanceof String text && !text.isEmpty() ? text : null;
    }

    static String cacheIdentity(AuthContext auth) {
        String authenticated = auth != null && auth.authenticated() ? "1" : "0";
        String domain = auth != null && auth.domain() != null ? auth.domain() : "";
        String principal = auth != null && auth.principal() != null ? auth.principal() : "";
        String binding = evidenceBinding(auth);
        return authenticated + "\0" + domain + "\0" + principal + "\0" + (binding != null ? binding : "");
    }

    /**
     * Compress a token payload and tag which codec was used.
     *
     * <p>Compression is skipped when it does not pay — small payloads can come
     * out larger, and the flag byte means the reader does not have to guess.
     * None of this is visible on the wire, so the cross-language conformance
     * suite cannot check it; the token tests pin it instead.</p>
     */
    static byte[] packPayload(byte[] plaintext) {
        byte[] packed = Zstd.compress(plaintext, ZSTD_LEVEL);
        if (packed.length < plaintext.length) {
            return prefixed(CODEC_ZSTD, packed);
        }
        return prefixed(CODEC_RAW, plaintext);
    }

    /**
     * Reverse {@link #packPayload}. An unknown tag or a body that will not
     * decompress means a token this server did not mint, so both surface as
     * the same uniform "Malformed state token" every other token failure uses.
     */
    static byte[] unpackPayload(byte[] data) {
        if (data.length == 0) {
            throw new IllegalArgumentException("Malformed state token");
        }
        byte[] body = new byte[data.length - 1];
        System.arraycopy(data, 1, body, 0, body.length);
        switch (data[0]) {
            case CODEC_RAW:
                return body;
            case CODEC_ZSTD:
                long size = Zstd.getFrameContentSize(body);
                if (size <= 0 || size > MAX_PLAINTEXT_BYTES) {
                    throw new IllegalArgumentException("Malformed state token");
                }
                byte[] out = new byte[(int) size];
                long ret = Zstd.decompress(out, body);
                if (Zstd.isError(ret) || ret != size) {
                    throw new IllegalArgumentException("Malformed state token");
                }
                return out;
            default:
                throw new IllegalArgumentException("Malformed state token");
        }
    }

    private static byte[] prefixed(byte tag, byte[] body) {
        byte[] out = new byte[1 + body.length];
        out[0] = tag;
        System.arraycopy(body, 0, out, 1, body.length);
        return out;
    }
}
