// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

/** Raised when one decoded Arrow IPC response exceeds its negotiated hard limit. */
public final class ResponseTooLargeError extends RuntimeException {
    public ResponseTooLargeError(String method, long actual, long limit) {
        super("method '" + method + "' exceeds max_response_bytes ("
                + actual + " > " + limit + ")");
    }
}
