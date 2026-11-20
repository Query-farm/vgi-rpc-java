// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.external;

import java.time.Instant;
import java.util.Map;

/**
 * Vends pre-signed PUT/GET URL pairs that clients use to externalise large
 * request batches. The HTTP server's {@code __upload_url__/init} endpoint
 * delegates here, once per {@code (size, sha256)} entry the client sends.
 *
 * <p>Together with {@link ExternalStorage} this completes the round-trip
 * externalisation protocol: the server uploads large responses through
 * {@code ExternalStorage}, while clients upload large requests through
 * URLs vended by this interface and embed the resulting download URL in a
 * pointer batch.</p>
 *
 * <p>Upload-URL protocol v2 ({@code VGI-Upload-URL-Support: 2}): every URL is
 * vended for one known body. A backend that can sign the length and the
 * checksum (S3, R2, the blob vendor) does, so storage itself refuses a body
 * that is not the one the client announced; one that cannot still signs what
 * it can, and the server's {@code vgi_rpc.location.sha256} check on the
 * pointer catches the rest.</p>
 */
public interface UploadUrlProvider {

    /**
     * Generate one {@link UploadUrl} pair for a body of exactly {@code size}
     * bytes whose SHA-256 is {@code sha256Hex}.
     *
     * @param size      exact byte length the client will PUT; at least 1
     * @param sha256Hex SHA-256 of those bytes, 64 lowercase hex characters
     * @return a freshly-issued URL pair; never {@code null}
     * @throws Exception if URL generation fails; the HTTP server reports the
     *         failure to the client as a {@code 500} error stream
     */
    UploadUrl generateUploadUrl(long size, String sha256Hex) throws Exception;

    /**
     * A vended PUT/GET URL pair plus expiry and the headers the PUT must carry.
     * {@code uploadUrl} accepts a single PUT of exactly the announced bytes;
     * {@code downloadUrl} returns those bytes with a GET. {@code expiresAt} is
     * the wall-clock UTC instant at which the URLs are no longer valid
     * (advisory; servers MAY reject earlier).
     *
     * @param uploadUrl pre-signed URL the client PUTs the externalised body to
     * @param downloadUrl pre-signed URL the server GETs to retrieve the body
     * @param expiresAt UTC instant after which both URLs stop working; may be
     *        {@code null}, in which case the server advertises a one-hour expiry
     * @param headers headers the client must send on the PUT verbatim (the
     *        signature covers them); never {@code null}, never
     *        {@code Content-Length}
     */
    record UploadUrl(String uploadUrl, String downloadUrl, Instant expiresAt, Map<String, String> headers) {

        /** Copy the header map, dropping {@code Content-Length}: the client always sends a plain one. */
        public UploadUrl {
            java.util.LinkedHashMap<String, String> h = new java.util.LinkedHashMap<>();
            if (headers != null) {
                headers.forEach((k, v) -> {
                    if (!"content-length".equalsIgnoreCase(k)) h.put(k, v);
                });
            }
            headers = java.util.Collections.unmodifiableMap(h);
        }

        /**
         * A pair whose PUT needs no extra headers.
         *
         * @param uploadUrl pre-signed PUT URL
         * @param downloadUrl pre-signed GET URL
         * @param expiresAt expiry, or {@code null}
         */
        public UploadUrl(String uploadUrl, String downloadUrl, Instant expiresAt) {
            this(uploadUrl, downloadUrl, expiresAt, Map.of());
        }

        @Override
        public String toString() {
            // Never print the signed URLs.
            return "UploadUrl[expiresAt=" + expiresAt + ", headers=" + headers.keySet() + "]";
        }
    }
}
