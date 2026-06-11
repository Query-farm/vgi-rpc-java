// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

/**
 * Raised when a sticky-session {@code open_session} is invoked after the
 * server has entered drain mode. Existing sessions continue to serve;
 * only new opens are rejected. Carries the stable {@code "server_draining"}
 * error_kind on the wire.
 */
public class ServerDrainingError extends RuntimeException implements HasErrorKind {
    /** Stable error category emitted via the {@code vgi_rpc.error_kind} metadata key. */
    public static final String ERROR_KIND = "server_draining";

    /** Creates the error with a diagnostic message.
     *  @param message diagnostic message */
    public ServerDrainingError(String message) { super(message); }

    @Override public String errorKind() { return ERROR_KIND; }
}
