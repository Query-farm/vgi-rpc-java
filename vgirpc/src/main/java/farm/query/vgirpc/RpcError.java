// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.errors.Code;
import farm.query.vgirpc.errors.ErrorDetail;
import farm.query.vgirpc.errors.ErrorModel;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Thrown on the client side when the server reports an error in the response stream.
 * Servers may also throw this to signal a protocol-level error to the client.
 *
 * <p>Carries the three layers of the error model (WIRE_PROTOCOL.md §8):
 * <ul>
 *   <li>{@link #errorCode()} -- the canonical code's name ({@code "UNAVAILABLE"}), or {@code ""}
 *       when the server sent none (a server older than the model). {@link #code()} reads it as a
 *       {@link Code}.</li>
 *   <li>{@link #errorKind()} -- the reason a client branches on, or {@code null}.</li>
 *   <li>{@link #errorDetails()} -- the detail objects as received, unknown types included. The
 *       typed accessors ({@link #retryInfo()}, {@link #badRequest()}, ...) return the catalog
 *       entry of that type and ignore the rest.</li>
 * </ul>
 *
 * <p>{@link #isRetryable()} classifies; nothing in this library retries an RPC error
 * automatically, because a method may not be idempotent.
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
    /** Canonical code name from {@code vgi_rpc.error_code}, or {@code ""} when absent. */
    private final String errorCode;
    /** Detail objects from {@code vgi_rpc.error_details}, as received. */
    private final transient List<Map<String, Object>> errorDetails;

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
        this(errorType, errorMessage, remoteTraceback, requestId, errorKind, "", List.of());
    }

    /**
     * Creates an error carrying the whole error model.
     *
     * @param errorType remote exception type name
     * @param errorMessage human-readable error message
     * @param remoteTraceback remote traceback text, or {@code ""} (the default over HTTP and TCP)
     * @param requestId id of the failed request, or {@code ""}
     * @param errorKind stable error category from {@code vgi_rpc.error_kind}, or {@code null}
     * @param errorCode canonical code name from {@code vgi_rpc.error_code}, or {@code ""}
     * @param errorDetails decoded {@code vgi_rpc.error_details} objects, or {@code null}
     */
    public RpcError(String errorType, String errorMessage, String remoteTraceback,
                    String requestId, String errorKind, String errorCode,
                    List<Map<String, Object>> errorDetails) {
        super(errorType + ": " + errorMessage);
        this.errorType = errorType;
        this.errorMessage = errorMessage;
        this.remoteTraceback = remoteTraceback;
        this.requestId = requestId;
        this.errorKind = errorKind;
        this.errorCode = errorCode == null ? "" : errorCode;
        List<Map<String, Object>> copy = new ArrayList<>();
        if (errorDetails != null) {
            for (Map<String, Object> d : errorDetails) {
                if (d != null) copy.add(java.util.Collections.unmodifiableMap(new LinkedHashMap<>(d)));
            }
        }
        this.errorDetails = List.copyOf(copy);
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

    /** The canonical code as sent, verbatim.
     *  @return the {@code vgi_rpc.error_code} value, or {@code ""} when the server sent none --
     *      which is a different answer from {@code "UNKNOWN"} */
    public String errorCode() { return errorCode; }

    /** The canonical code, parsed.
     *  @return the code; {@link Code#UNKNOWN} when absent or unrecognised */
    public Code code() { return Code.parse(errorCode.isEmpty() ? null : errorCode); }

    /** The detail objects as received, unknown {@code @type}s included.
     *  @return the details in wire order; empty when absent */
    public List<Map<String, Object>> errorDetails() { return errorDetails; }

    /** Whether retrying this call is warranted, by the rule in WIRE_PROTOCOL.md §8:
     *  {@code UNAVAILABLE} always; {@code RESOURCE_EXHAUSTED} only with {@code RetryInfo}.
     *  When {@link #retryInfo()} is present a retry waits at least that long.
     *  @return whether the error is retryable */
    public boolean isRetryable() { return ErrorModel.isRetryable(code(), errorDetails); }

    /** The details this client understands, in wire order; unknown types skipped.
     *  @return the typed details */
    public List<ErrorDetail> details() { return ErrorModel.typed(errorDetails); }

    private <T extends ErrorDetail> T detail(Class<T> cls) {
        for (ErrorDetail d : details()) {
            if (cls.isInstance(d)) return cls.cast(d);
        }
        return null;
    }

    /** @return the {@code vgi_rpc.ErrorInfo} detail, or {@code null} */
    public ErrorDetail.ErrorInfo errorInfo() { return detail(ErrorDetail.ErrorInfo.class); }
    /** @return the {@code vgi_rpc.RetryInfo} detail, or {@code null} */
    public ErrorDetail.RetryInfo retryInfo() { return detail(ErrorDetail.RetryInfo.class); }
    /** @return the {@code vgi_rpc.BadRequest} detail, or {@code null} */
    public ErrorDetail.BadRequest badRequest() { return detail(ErrorDetail.BadRequest.class); }
    /** @return the {@code vgi_rpc.PreconditionFailure} detail, or {@code null} */
    public ErrorDetail.PreconditionFailure preconditionFailure() {
        return detail(ErrorDetail.PreconditionFailure.class);
    }
    /** @return the {@code vgi_rpc.QuotaFailure} detail, or {@code null} */
    public ErrorDetail.QuotaFailure quotaFailure() { return detail(ErrorDetail.QuotaFailure.class); }
    /** @return the {@code vgi_rpc.ResourceInfo} detail, or {@code null} */
    public ErrorDetail.ResourceInfo resourceInfo() { return detail(ErrorDetail.ResourceInfo.class); }
    /** @return the {@code vgi_rpc.Help} detail, or {@code null} */
    public ErrorDetail.Help help() { return detail(ErrorDetail.Help.class); }
    /** @return the {@code vgi_rpc.LocalizedMessage} detail, or {@code null} */
    public ErrorDetail.LocalizedMessage localizedMessage() {
        return detail(ErrorDetail.LocalizedMessage.class);
    }
}
