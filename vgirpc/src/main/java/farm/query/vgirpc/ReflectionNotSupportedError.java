// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

/**
 * The server does not host {@code vgi_rpc.Reflection.v1}.
 *
 * <p>Thrown by {@link Introspect#listProtocols(Object)} and
 * {@link Introspect#describeProtocol(Object, String)} when the server answers
 * the reflection call with "not hosted" rather than with a listing: a Python
 * server built without {@code enable_describe=True} (its default), or any
 * server that predates reflection. Such a server still serves its own
 * protocols, so this is a statement about <em>discovery</em>, not about the
 * connection — the connection remains usable afterwards.
 *
 * <p>No listing is ever inferred in its place: only the caller knows which
 * protocol it expected to find.
 *
 * <p>A subclass of {@link RpcError} carrying the server's original error fields
 * (type, message, traceback, request id, code, kind and details), so code that
 * already catches {@code RpcError} keeps working; catch this class to branch on
 * "cannot discover" specifically.
 */
public final class ReflectionNotSupportedError extends RpcError {

    /**
     * Wrap the server's "not hosted" answer, keeping every field.
     *
     * @param error the error the reflection call raised
     */
    public ReflectionNotSupportedError(RpcError error) {
        super(error.errorType(), error.errorMessage(), error.remoteTraceback(), error.requestId(),
                error.errorKind(), error.errorCode(), error.errorDetails());
        initCause(error);
    }

    /**
     * Whether {@code error} says the server does not host reflection at all.
     *
     * <p>Only meaningful for {@code list_protocols}, which is always hosted when
     * reflection is: a "not supported" answer to it can only be about the
     * protocol. ({@code describe} answers {@code protocol_not_supported} for an
     * unknown <em>argument</em>, which is why
     * {@link Introspect#describeProtocol(Object, String)} lists first.)
     *
     * <p>A current server without reflection answers {@code protocol_not_supported};
     * one older than multi-protocol hosting ignores the routing key and answers an
     * unknown method; both carry {@code UNIMPLEMENTED} when the server sends a code
     * at all. An HTTP server older than protocol-scoped routes answers a bare 404,
     * which the HTTP client reports as a non-Arrow {@code HttpError}.
     */
    static boolean isNotHosted(RpcError error) {
        String kind = error.errorKind();
        if (ProtocolNotSupportedError.ERROR_KIND.equals(kind) || MethodNotImplementedError.ERROR_KIND.equals(kind)) {
            return true;
        }
        if ("UNIMPLEMENTED".equals(error.errorCode())) return true;
        String type = error.errorType();
        if ("ProtocolNotSupportedError".equals(type) || "MethodNotImplementedError".equals(type)) {
            return true;
        }
        String message = error.errorMessage() == null ? "" : error.errorMessage();
        return "HttpError".equals(type) && (message.startsWith("HTTP 404") || message.contains(": HTTP 404 "));
    }
}
