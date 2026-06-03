// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

/**
 * Thrown when an HTTP request body or RPC response exceeds the server's
 * configured size cap. Maps to HTTP 413. The intended remediation for large
 * batches is the external-location protocol (see
 * {@code farm.query.vgirpc.external}); this exception's message points
 * callers at it.
 */
public final class PayloadTooLargeException extends RuntimeException {
    /** @param message diagnostic message (typically points callers at the external-location protocol) */
    public PayloadTooLargeException(String message) { super(message); }
}
