// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

/**
 * Thrown on the client side when the server reports an error in the response stream.
 * Servers may also throw this to signal a protocol-level error to the client.
 */
public class RpcError extends RuntimeException implements HasErrorKind {

    private final String errorType;
    private final String errorMessage;
    private final String remoteTraceback;
    private final String requestId;
    /** Stable error category surfaced via the {@code vgi_rpc.error_kind}
     *  batch metadata key; null when the server didn't emit one. */
    private final String errorKind;

    public RpcError(String errorType, String errorMessage, String remoteTraceback) {
        this(errorType, errorMessage, remoteTraceback, "", null);
    }

    public RpcError(String errorType, String errorMessage, String remoteTraceback, String requestId) {
        this(errorType, errorMessage, remoteTraceback, requestId, null);
    }

    public RpcError(String errorType, String errorMessage, String remoteTraceback,
                    String requestId, String errorKind) {
        super(errorType + ": " + errorMessage);
        this.errorType = errorType;
        this.errorMessage = errorMessage;
        this.remoteTraceback = remoteTraceback;
        this.requestId = requestId;
        this.errorKind = errorKind;
    }

    public String errorType() { return errorType; }
    public String errorMessage() { return errorMessage; }
    public String remoteTraceback() { return remoteTraceback; }
    public String requestId() { return requestId; }
    @Override public String errorKind() { return errorKind; }
}
