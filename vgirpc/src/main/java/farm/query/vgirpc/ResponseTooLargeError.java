// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

/** Raised when one decoded Arrow IPC response exceeds its negotiated hard limit. */
public final class ResponseTooLargeError extends RuntimeException
        implements farm.query.vgirpc.errors.HasErrorCode {
    public ResponseTooLargeError(String method, long actual, long limit) {
        super("method '" + method + "' exceeds max_response_bytes ("
                + actual + " > " + limit + ")");
    }

    /** {@code RESOURCE_EXHAUSTED} with no {@code RetryInfo}, so not retryable: the same call
     *  against the same limits produces the same result. */
    @Override
    public farm.query.vgirpc.errors.Code errorCode() {
        return farm.query.vgirpc.errors.Code.RESOURCE_EXHAUSTED;
    }
}
