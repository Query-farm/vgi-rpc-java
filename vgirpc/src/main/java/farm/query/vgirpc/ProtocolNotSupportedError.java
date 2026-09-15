// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

/**
 * Raised when a request names a protocol this server does not host, or names one protocol in its
 * HTTP path and a different one in {@code vgi_rpc.protocol}.
 *
 * <p>Over HTTP the first case is a {@code 404} -- a routing failure every proxy, WAF and load
 * balancer understands without an Arrow parser -- and the second is a {@code 400}. The
 * disagreement case is the Content-Length/Transfer-Encoding shape: left unchecked, the edge
 * applies policy to one protocol while the worker dispatches another.
 */
public class ProtocolNotSupportedError extends RuntimeException implements HasErrorKind {
    /** Stable error category emitted via the {@code vgi_rpc.error_kind} metadata key. */
    public static final String ERROR_KIND = "protocol_not_supported";

    /** Creates the error with a diagnostic message.
     *  @param message diagnostic message naming the protocol that could not be routed */
    public ProtocolNotSupportedError(String message) { super(message); }

    @Override public String errorKind() { return ERROR_KIND; }
}
