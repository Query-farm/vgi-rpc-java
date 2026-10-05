// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.external;

import farm.query.vgirpc.wire.Metadata;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A reference to an already-published unary result.
 *
 * <p>A unary method answers with a ref by calling
 * {@link farm.query.vgirpc.CallContext#respondWithExternalRef(ExternalRef)}; the
 * server then writes the ExternalLocation pointer batch for {@link #url()}
 * directly — no result value is built or validated, nothing is serialised,
 * compressed, or uploaded during the call, the shared-memory and inline routes
 * are never taken, and the ref is used whether or not the server has external
 * storage configured and regardless of the externalisation threshold. It is not
 * charged to {@code max_externalized_response_bytes}. Clients resolve it like
 * any other pointer, so they need no change.</p>
 *
 * <p>Build one with {@link Externalizer#publishExternal} (or by hand for an
 * object published out of band). The object at {@code url} must be an Arrow IPC
 * stream (optionally {@code Content-Encoding}-compressed) whose schema is the
 * method's result schema and which holds exactly one 1-row data batch.</p>
 *
 * <p>The caller owns caching the ref and the object's lifecycle: a long-lived
 * ref must not point at an object under the short-TTL lifecycle rule used for
 * per-call uploads, and a pre-signed URL expires — re-sign or rebuild the ref
 * before then. Only return a ref to callers who are all entitled to the same
 * content.</p>
 *
 * @param url where the published IPC stream lives; never empty
 * @param sha256 lowercase hex SHA-256 of the raw (pre-compression) IPC stream
 *     bytes, sent as {@code vgi_rpc.location.sha256}; {@code null} omits the
 *     key so clients skip the content check (for an object rewritten in place,
 *     or one too large to hash)
 */
public record ExternalRef(String url, String sha256) {

    /**
     * Validate the URL and digest shape.
     *
     * @throws NullPointerException if {@code url} is {@code null}
     * @throws IllegalArgumentException if {@code url} is empty or {@code sha256}
     *     is not 64 lowercase hex characters
     */
    public ExternalRef {
        Objects.requireNonNull(url, "ExternalRef.url");
        if (url.isEmpty()) {
            throw new IllegalArgumentException("ExternalRef.url must be non-empty");
        }
        if (sha256 != null && !isLowerHexSha256(sha256)) {
            throw new IllegalArgumentException(
                    "ExternalRef.sha256 must be 64 lowercase hex characters (or null)");
        }
    }

    /**
     * A ref with no digest: clients skip the content check.
     *
     * @param url where the published IPC stream lives
     * @return the ref
     */
    public static ExternalRef of(String url) {
        return new ExternalRef(url, null);
    }

    /**
     * A ref for {@code url} with the given digest.
     *
     * @param url where the published IPC stream lives
     * @param sha256 lowercase hex SHA-256 of the raw IPC bytes, or {@code null}
     * @return the ref
     */
    public static ExternalRef of(URI url, String sha256) {
        return new ExternalRef(Objects.requireNonNull(url, "url").toString(), sha256);
    }

    /**
     * The custom metadata of the zero-row pointer batch announcing this ref:
     * {@code vgi_rpc.location}, plus {@code vgi_rpc.location.sha256} only when
     * the ref carries a digest.
     *
     * @return a fresh, mutable map in wire order
     */
    public Map<String, String> pointerMetadata() {
        Map<String, String> meta = new LinkedHashMap<>();
        meta.put(Metadata.LOCATION, url);
        if (sha256 != null) meta.put(Metadata.LOCATION_SHA256, sha256);
        return meta;
    }

    private static boolean isLowerHexSha256(String s) {
        if (s.length() != 64) return false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return false;
        }
        return true;
    }
}
