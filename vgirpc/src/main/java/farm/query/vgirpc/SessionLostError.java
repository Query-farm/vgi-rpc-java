// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

/**
 * Raised when a sticky-session token cannot be opened (malformed,
 * tampered, expired) or refers to an entry no longer in the registry
 * (expired, evicted, explicitly closed). The framework surfaces the
 * stable {@code "session_lost"} error_kind on the wire so clients can
 * pattern-match without parsing message strings.
 */
public class SessionLostError extends RuntimeException implements HasErrorKind {
    /** Stable error category emitted via the {@code vgi_rpc.error_kind} metadata key. */
    public static final String ERROR_KIND = "session_lost";

    /** Creates the error with a diagnostic message.
     *  @param message diagnostic message */
    public SessionLostError(String message) { super(message); }
    /**
     * Creates the error wrapping the failure that invalidated the session
     * (e.g. a token-verification exception).
     *
     * @param message diagnostic message
     * @param cause the underlying cause
     */
    public SessionLostError(String message, Throwable cause) { super(message, cause); }

    @Override public String errorKind() { return ERROR_KIND; }
}
