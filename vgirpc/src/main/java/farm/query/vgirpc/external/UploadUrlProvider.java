// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.external;

import java.time.Instant;

/**
 * Vends pre-signed PUT/GET URL pairs that clients use to externalise large
 * request batches. The HTTP server's {@code __upload_url__/init} endpoint
 * delegates here when a client requests upload URLs.
 *
 * <p>Together with {@link ExternalStorage} this completes the round-trip
 * externalisation protocol: the server uploads large responses through
 * {@code ExternalStorage}, while clients upload large requests through
 * URLs vended by this interface and embed the resulting download URL in a
 * pointer batch.</p>
 */
public interface UploadUrlProvider {

    /**
     * Generate a single {@link UploadUrl} pair. Implementations typically call
     * out to an object store's pre-signed-URL API.
     *
     * @return a freshly-issued URL pair; never {@code null}
     */
    UploadUrl generateUploadUrl() throws Exception;

    /**
     * A vended PUT/GET URL pair plus expiry. {@code uploadUrl} accepts a
     * single PUT of opaque bytes; {@code downloadUrl} returns those bytes
     * with a GET. {@code expiresAt} is the wall-clock UTC instant at which
     * the URLs are no longer valid (advisory; servers MAY reject earlier).
     */
    record UploadUrl(String uploadUrl, String downloadUrl, Instant expiresAt) {}
}
