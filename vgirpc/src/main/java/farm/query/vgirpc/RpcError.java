// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

/**
 * Thrown on the client side when the server reports an error in the response stream.
 * Servers may also throw this to signal a protocol-level error to the client.
 */
public class RpcError extends RuntimeException implements HasErrorKind {

    /** Remote exception type name as reported by the server (e.g. {@code "ValueError"}). */
    private final String errorType;
    /** Human-readable error message from the server. */
    private final String errorMessage;
    /** Remote traceback text, or {@code ""} when the server omitted it. */
    private final String remoteTraceback;
    /** Id of the failed request, or {@code ""} when none was assigned. */
    private final String requestId;
    /** Stable error category surfaced via the {@code vgi_rpc.error_kind}
     *  batch metadata key; null when the server didn't emit one. */
    private final String errorKind;

    /**
     * Creates an error with no request id and no error kind.
     *
     * @param errorType remote exception type name (e.g. {@code "ValueError"})
     * @param errorMessage human-readable error message
     * @param remoteTraceback remote traceback text, or {@code ""}
     */
    public RpcError(String errorType, String errorMessage, String remoteTraceback) {
        this(errorType, errorMessage, remoteTraceback, "", null);
    }

    /**
     * Creates an error carrying the failed request's id but no error kind.
     *
     * @param errorType remote exception type name
     * @param errorMessage human-readable error message
     * @param remoteTraceback remote traceback text, or {@code ""}
     * @param requestId id of the failed request, or {@code ""}
     */
    public RpcError(String errorType, String errorMessage, String remoteTraceback, String requestId) {
        this(errorType, errorMessage, remoteTraceback, requestId, null);
    }

    /**
     * Creates an error with all wire-level detail, including the optional
     * stable error category from the {@code vgi_rpc.error_kind} metadata key.
     *
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

    /** Remote exception type name as reported by the server.
     *  @return the remote exception type name (e.g. {@code "ValueError"}). */
    public String errorType() { return errorType; }
    /** Human-readable error message, without the type-name prefix that
     *  {@link #getMessage()} carries.
     *  @return the human-readable error message. */
    public String errorMessage() { return errorMessage; }
    /** Traceback captured on the server side, useful for cross-process debugging.
     *  @return the remote traceback text, or {@code ""} when the server omitted it. */
    public String remoteTraceback() { return remoteTraceback; }
    /** Identifier of the request that failed, for correlating with server access logs.
     *  @return the failed request's id, or {@code ""} when none was assigned. */
    public String requestId() { return requestId; }
    @Override public String errorKind() { return errorKind; }
}
