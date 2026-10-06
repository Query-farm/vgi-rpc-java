// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.errors.Code;
import farm.query.vgirpc.errors.ErrorDetail;
import farm.query.vgirpc.errors.HasErrorCode;
import farm.query.vgirpc.errors.HasErrorDetails;

import java.util.List;
import java.util.Map;

/**
 * Raised when a sticky-session {@code open_session} is invoked after the
 * server has entered drain mode. Existing sessions continue to serve;
 * only new opens are rejected. Carries the stable {@code "server_draining"}
 * error_kind on the wire, code {@code UNAVAILABLE}, and a {@code RetryInfo} --
 * a retry is usually routed to a worker that is not draining.
 */
public class ServerDrainingError extends RuntimeException
        implements HasErrorKind, HasErrorCode, HasErrorDetails {
    /** Stable error category emitted via the {@code vgi_rpc.error_kind} metadata key. */
    public static final String ERROR_KIND = "server_draining";

    /** Retry hint sent when none is given, in seconds. */
    public static final double DEFAULT_RETRY_AFTER_SECONDS = 1.0;

    private final double retryAfterSeconds;

    /** Creates the error with a diagnostic message and the default retry hint.
     *  @param message diagnostic message */
    public ServerDrainingError(String message) { this(message, DEFAULT_RETRY_AFTER_SECONDS); }

    /** Creates the error with an explicit retry hint.
     *  @param message diagnostic message
     *  @param retryAfterSeconds seconds before a retry; non-finite or negative uses the default */
    public ServerDrainingError(String message, double retryAfterSeconds) {
        super(message);
        this.retryAfterSeconds = Double.isFinite(retryAfterSeconds) && retryAfterSeconds >= 0
                ? retryAfterSeconds : DEFAULT_RETRY_AFTER_SECONDS;
    }

    /** @return the retry hint in seconds */
    public double retryAfterSeconds() { return retryAfterSeconds; }

    @Override public String errorKind() { return ERROR_KIND; }

    @Override public Code errorCode() { return Code.UNAVAILABLE; }

    @Override
    public List<Map<String, Object>> errorDetails() {
        return List.of(new ErrorDetail.RetryInfo(retryAfterSeconds).toJson());
    }
}
