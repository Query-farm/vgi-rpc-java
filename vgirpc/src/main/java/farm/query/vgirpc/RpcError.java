// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

/**
 * Thrown on the client side when the server reports an error in the response stream.
 * Servers may also throw this to signal a protocol-level error to the client.
 */
public class RpcError extends RuntimeException {

    private final String errorType;
    private final String errorMessage;
    private final String remoteTraceback;
    private final String requestId;

    public RpcError(String errorType, String errorMessage, String remoteTraceback) {
        this(errorType, errorMessage, remoteTraceback, "");
    }

    public RpcError(String errorType, String errorMessage, String remoteTraceback, String requestId) {
        super(errorType + ": " + errorMessage);
        this.errorType = errorType;
        this.errorMessage = errorMessage;
        this.remoteTraceback = remoteTraceback;
        this.requestId = requestId;
    }

    public String errorType() { return errorType; }
    public String errorMessage() { return errorMessage; }
    public String remoteTraceback() { return remoteTraceback; }
    public String requestId() { return requestId; }
}
