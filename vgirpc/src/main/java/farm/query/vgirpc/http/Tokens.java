// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.stateref.TokenCodec;

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

    /**
     * Scope for tokens that belong to the server rather than to any one protocol -- session
     * tokens, which live at the un-namespaced prefix.
     *
     * <p>Spelled with a leading NUL so it cannot collide with a protocol name, which must start
     * {@code [A-Za-z_]}. Naming it at every call site is deliberate: a new one then cannot
     * silently inherit an unbound token.
     */
    static final String SERVER_SCOPE = "\u0000server";

    /**
     * Append the protocol scope that binds a token to the protocol its stream started on.
     *
     * <p>The protocol lives in the AAD rather than in the plaintext so a cross-protocol
     * continuation fails the AEAD tag check -- it is refused exactly as an invalid token, with no
     * comparison code to get wrong and nothing to forget on the call-state cache-hit path, where
     * the call token is never opened at all. The cursor is always opened first, so binding it
     * there covers both paths.
     */
    private static byte[] scoped(byte[] base, String protocol) {
        byte[] p = (protocol != null ? protocol : SERVER_SCOPE).getBytes(StandardCharsets.UTF_8);
        byte[] out = new byte[base.length + 1 + p.length];
        System.arraycopy(base, 0, out, 0, base.length);
        out[base.length] = 0x00;
        System.arraycopy(p, 0, out, base.length + 1, p.length);
        return out;
    }

    /** As {@link #aad(byte[], String)}, additionally bound to the owning protocol. */
    static byte[] aad(byte[] prefix, String principal, String protocol) {
        return scoped(aad(prefix, principal), protocol);
    }

    /** As {@link #aad(byte[], byte[], AuthContext)}, additionally bound to the owning protocol. */
    static byte[] aad(byte[] legacyPrefix, byte[] boundPrefix, AuthContext auth, String protocol) {
        return scoped(aad(legacyPrefix, boundPrefix, auth), protocol);
    }

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
     * Compress a token payload and tag which codec was used: the standard token codec
     * ({@link TokenCodec}, STATE_REF_V1.md §7).
     *
     * <p>Compression is skipped when it does not pay — small payloads can come
     * out larger, and the flag byte means the reader does not have to guess.
     * None of this is visible on the wire, so the cross-language conformance
     * suite cannot check it; the token tests pin it instead.</p>
     */
    static byte[] packPayload(byte[] plaintext) {
        return TokenCodec.pack(plaintext);
    }

    /**
     * Reverse {@link #packPayload}. An unknown tag or a body that will not
     * decompress means a token this server did not mint, so both surface as
     * the same uniform "Malformed state token" every other token failure uses.
     */
    static byte[] unpackPayload(byte[] data) {
        try {
            return TokenCodec.unpack(data, TokenCodec.MAX_TOKEN_PLAINTEXT_BYTES);
        } catch (TokenCodec.TokenCodecException e) {
            throw new IllegalArgumentException("Malformed state token");
        }
    }
}
