// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

/**
 * Raised when a request carries no {@code vgi_rpc.protocol} routing key on a transport where the
 * metadata is the only carrier.
 *
 * <p>Required even against a server hosting exactly one protocol. An exemption would let an
 * intermediary that rebuilds a request and drops the field land silently on whichever protocol
 * happened to be registered first, rather than being told -- and "silently landed on the wrong
 * protocol" produces plausible output, not an error.
 *
 * <p>Distinct from {@link ProtocolNotSupportedError} on purpose: "you did not say" and "this
 * server does not host that" are different problems with different fixes, and a client
 * capability-probing a fleet depends on telling them apart.
 */
public class ProtocolNotSpecifiedError extends RuntimeException implements HasErrorKind {
    /** Stable error category emitted via the {@code vgi_rpc.error_kind} metadata key. */
    public static final String ERROR_KIND = "protocol_not_specified";

    /** Creates the error with a diagnostic message.
     *  @param message diagnostic message naming the protocols this server does host */
    public ProtocolNotSpecifiedError(String message) { super(message); }

    @Override public String errorKind() { return ERROR_KIND; }
}
