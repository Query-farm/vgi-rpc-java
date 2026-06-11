// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

/**
 * Raised when a method exists in the protocol definition but the server
 * does not implement it. Carries the stable {@code "method_not_implemented"}
 * error_kind on the wire so capability-detecting clients pattern-match
 * cleanly across older / partial server implementations.
 */
public class MethodNotImplementedError extends RuntimeException implements HasErrorKind {
    /** Stable error category emitted via the {@code vgi_rpc.error_kind} metadata key. */
    public static final String ERROR_KIND = "method_not_implemented";

    /** Creates the error with a diagnostic message.
     *  @param message diagnostic message naming the unimplemented method */
    public MethodNotImplementedError(String message) { super(message); }

    @Override public String errorKind() { return ERROR_KIND; }
}
