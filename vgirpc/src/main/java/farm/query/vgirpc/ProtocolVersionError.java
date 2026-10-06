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
 * The client's declared application {@code protocol_version} is incompatible
 * with the server's.
 *
 * <p>Distinct from {@link VersionError}, which is about the vgi-rpc FRAMEWORK
 * request version — the envelope. This one is about the APPLICATION surface
 * riding inside it, and the two move independently.</p>
 *
 * <p>Kind {@code protocol_version_mismatch}, code {@code FAILED_PRECONDITION}, and a
 * {@code PreconditionFailure} with one {@code protocol_version} violation naming the protocol
 * whose gate refused the call.</p>
 */
public final class ProtocolVersionError extends RuntimeException
        implements HasErrorKind, HasErrorCode, HasErrorDetails {

    /** Stable error category emitted via the {@code vgi_rpc.error_kind} metadata key. */
    public static final String ERROR_KIND = "protocol_version_mismatch";

    private final String protocol;
    private final String clientVersion;
    private final String serverVersion;

    /**
     * @param message the directional diagnostic naming both versions and which side to upgrade
     */
    public ProtocolVersionError(String message) {
        this(message, "", "", "");
    }

    /**
     * @param message the directional diagnostic naming both versions and which side to upgrade
     * @param protocol the protocol whose version gate refused the call
     * @param clientVersion what the client declared, or {@code ""} when it declared none
     * @param serverVersion what the server's binding declares
     */
    public ProtocolVersionError(String message, String protocol, String clientVersion,
                                String serverVersion) {
        super(message);
        this.protocol = protocol == null ? "" : protocol;
        this.clientVersion = clientVersion == null ? "" : clientVersion;
        this.serverVersion = serverVersion == null ? "" : serverVersion;
    }

    /** @return the protocol whose gate refused the call, or {@code ""} when unknown */
    public String protocol() { return protocol; }

    @Override public String errorKind() { return ERROR_KIND; }

    @Override public Code errorCode() { return Code.FAILED_PRECONDITION; }

    @Override
    public List<Map<String, Object>> errorDetails() {
        if (protocol.isEmpty()) return List.of();
        return List.of(new ErrorDetail.PreconditionFailure(List.of(
                new ErrorDetail.PreconditionViolation("protocol_version", protocol,
                        "client declares " + (clientVersion.isEmpty() ? "<none>" : clientVersion)
                                + ", server requires " + serverVersion
                                + "; major and minor must match"))).toJson());
    }
}
