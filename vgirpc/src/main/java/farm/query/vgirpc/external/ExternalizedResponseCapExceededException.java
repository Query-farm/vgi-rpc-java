// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.external;

/**
 * Raised when a response would push more bytes to external storage than
 * {@code max_externalized_response_bytes} allows.
 *
 * <p>Its own type, not a plain {@code RuntimeException}, because the upload
 * paths wrap {@link Externalizer#maybeExternalize} in a catch-all that falls
 * back to writing the batch inline: an upload that failed for transport
 * reasons should still deliver data. An operator cap is the opposite — it is a
 * deliberate refusal, and swallowing it would answer success for the very
 * response the operator asked to be refused. Every such catch-all rethrows
 * this type ahead of its fallback.
 *
 * <p>The message names {@code max_externalized_response_bytes} literally: the
 * cross-language conformance suite matches on that token, and a client that
 * read the advertised {@code VGI-Max-Externalized-Response-Bytes} header needs
 * to recognise the limit it just hit.
 */
public final class ExternalizedResponseCapExceededException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * @param projectedBytes total external bytes the response would have uploaded
     * @param capBytes the configured {@code max_externalized_response_bytes}
     * @param methodName the RPC method whose response tripped the cap
     */
    ExternalizedResponseCapExceededException(long projectedBytes, long capBytes, String methodName) {
        super("Externalised payload exceeds max_externalized_response_bytes ("
                + projectedBytes + " > " + capBytes + ") for method '"
                + (methodName == null ? "" : methodName) + "'");
    }
}
