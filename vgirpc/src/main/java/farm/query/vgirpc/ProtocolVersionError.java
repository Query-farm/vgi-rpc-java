// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.errors.Code;
import farm.query.vgirpc.errors.ErrorDetail;
import farm.query.vgirpc.errors.HasErrorCode;
import farm.query.vgirpc.errors.HasErrorDetails;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The client's declared application {@code protocol_version} is outside the server's
 * supported window ({@code PROTOCOL_VERSIONS_V1.md} §4.3).
 *
 * <p>Distinct from {@link VersionError}, which is about the vgi-rpc FRAMEWORK
 * request version — the envelope. This one is about the APPLICATION surface
 * riding inside it, and the two move independently.</p>
 *
 * <p>Kind {@code protocol_version_unsupported}, code {@code FAILED_PRECONDITION}, a
 * {@code PreconditionFailure} with one {@code protocol_version} violation naming the protocol
 * whose gate refused the call, and a {@code vgi_rpc.ErrorInfo} whose metadata carries the
 * server's full advertisement ({@code vgi_rpc.protocol_versions}) and, when the client sent it,
 * the client's ({@code vgi_rpc.protocol_versions_client}). Servers released before protocol
 * version negotiation sent {@code protocol_version_mismatch}; clients still recognise it.</p>
 */
public final class ProtocolVersionError extends RuntimeException
        implements HasErrorKind, HasErrorCode, HasErrorDetails {

    /** Stable error category emitted via the {@code vgi_rpc.error_kind} metadata key. */
    public static final String ERROR_KIND = ProtocolVersions.UNSUPPORTED;

    private final String protocol;
    private final String clientVersion;
    private final String serverVersion;
    private final String serverVersions;
    private final String clientVersions;

    /**
     * @param message the diagnostic naming both windows and which side to upgrade
     */
    public ProtocolVersionError(String message) {
        this(message, "", "", "", "", "");
    }

    /**
     * @param message the diagnostic naming both windows and which side to upgrade
     * @param protocol the protocol whose version gate refused the call
     * @param clientVersion what the client declared, or {@code ""} when it declared none
     * @param serverVersion the server's window for {@code protocol}, {@code MIN-MAX}
     */
    public ProtocolVersionError(String message, String protocol, String clientVersion,
                                String serverVersion) {
        this(message, protocol, clientVersion, serverVersion, "", "");
    }

    /**
     * @param message the diagnostic naming both windows and which side to upgrade
     * @param protocol the protocol whose version gate refused the call
     * @param clientVersion what the client declared, or {@code ""} when it declared none
     * @param serverVersion the server's window for {@code protocol}, {@code MIN-MAX}
     * @param serverVersions the server's full {@code VGI-Protocol-Versions} value
     * @param clientVersions the client's own advertised windows, or {@code ""}
     */
    public ProtocolVersionError(String message, String protocol, String clientVersion,
                                String serverVersion, String serverVersions, String clientVersions) {
        super(message);
        this.protocol = protocol == null ? "" : protocol;
        this.clientVersion = clientVersion == null ? "" : clientVersion;
        this.serverVersion = serverVersion == null ? "" : serverVersion;
        this.serverVersions = serverVersions == null ? "" : serverVersions;
        this.clientVersions = clientVersions == null ? "" : clientVersions;
    }

    /** @return the protocol whose gate refused the call, or {@code ""} when unknown */
    public String protocol() { return protocol; }

    /** @return the server's full advertisement, or {@code ""} when unknown */
    public String serverVersions() { return serverVersions; }

    @Override public String errorKind() { return ERROR_KIND; }

    @Override public Code errorCode() { return Code.FAILED_PRECONDITION; }

    @Override
    public List<Map<String, Object>> errorDetails() {
        List<Map<String, Object>> out = new ArrayList<>();
        if (!protocol.isEmpty()) {
            out.add(new ErrorDetail.PreconditionFailure(List.of(
                    new ErrorDetail.PreconditionViolation("protocol_version", protocol,
                            "client declares " + (clientVersion.isEmpty() ? "<none>" : clientVersion)
                                    + ", server supports "
                                    + (serverVersion.isEmpty() ? "<unknown>" : serverVersion)))).toJson());
        }
        if (!serverVersions.isEmpty()) {
            Map<String, String> md = new LinkedHashMap<>();
            md.put(ProtocolVersions.KEY, serverVersions);
            if (!clientVersions.isEmpty()) md.put(ProtocolVersions.CLIENT_KEY, clientVersions);
            out.add(new ErrorDetail.ErrorInfo(md).toJson());
        }
        return out;
    }
}
