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

    /**
     * @param errorType remote exception type name (e.g. {@code "ValueError"})
     * @param errorMessage human-readable error message
     * @param remoteTraceback remote traceback text, or {@code ""}
     */
    public RpcError(String errorType, String errorMessage, String remoteTraceback) {
        this(errorType, errorMessage, remoteTraceback, "", null);
    }

    /**
     * @param errorType remote exception type name
     * @param errorMessage human-readable error message
     * @param remoteTraceback remote traceback text, or {@code ""}
     * @param requestId id of the failed request, or {@code ""}
     */
    public RpcError(String errorType, String errorMessage, String remoteTraceback, String requestId) {
        this(errorType, errorMessage, remoteTraceback, requestId, null);
    }

    /**
     * @param errorType remote exception type name
     * @param errorMessage human-readable error message
     * @param remoteTraceback remote traceback text, or {@code ""}
     * @param requestId id of the failed request, or {@code ""}
     * @param errorKind stable error category from {@code vgi_rpc.error_kind}, or {@code null}
     */
    public RpcError(String errorType, String errorMessage, String remoteTraceback,
                    String requestId, String errorKind) {
        super(errorType + ": " + errorMessage);
        this.errorType = errorType;
        this.errorMessage = errorMessage;
        this.remoteTraceback = remoteTraceback;
        this.requestId = requestId;
        this.errorKind = errorKind;
    }

    /** @return the remote exception type name. */
    public String errorType() { return errorType; }
    /** @return the human-readable error message. */
    public String errorMessage() { return errorMessage; }
    /** @return the remote traceback text, or {@code ""}. */
    public String remoteTraceback() { return remoteTraceback; }
    /** @return the failed request's id, or {@code ""}. */
    public String requestId() { return requestId; }
    @Override public String errorKind() { return errorKind; }
}
