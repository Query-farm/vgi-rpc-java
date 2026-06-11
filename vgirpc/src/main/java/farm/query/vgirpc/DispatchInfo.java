// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import java.util.Map;

/**
 * Metadata passed to {@link DispatchHook} callbacks around RPC dispatch.
 *
 * <p>Mutable so the dispatcher can fill in fields that are not known at the
 * start of the call (e.g. {@code httpStatus}, {@code cancelled}). Hook
 * implementations should read fields only inside the hook callbacks.
 */
public final class DispatchInfo {

    /** Name of the dispatched RPC method. */
    public String method = "";
    /** Method kind: {@code "unary"} or {@code "stream"}. */
    public String methodType = "unary";
    /** Identity of the serving {@link RpcServer} instance. */
    public String serverId = "";
    /** Simple name of the service (protocol) interface being served. */
    public String protocol = "";
    /** Protocol hash from {@code Introspect.computeProtocolHash}; identifies the exact method surface. */
    public String protocolHash = "";
    /** Optional human-readable protocol version label set via {@link RpcServer#setProtocolVersion}; empty when unset. */
    public String protocolVersion = "";
    /** Client-supplied request id for log correlation; empty when none was sent. */
    public String requestId = "";
    /** Authenticated principal, or empty when the call is unauthenticated. */
    public String principal = "";
    /** Authentication domain of the principal (e.g. issuer); empty when unauthenticated. */
    public String authDomain = "";
    /** Whether the call passed authentication. */
    public boolean authenticated;
    /** Remote peer address (HTTP transport); empty for pipe and unix-socket transports. */
    public String remoteAddr = "";
    /** HTTP response status code; 0 when the call did not arrive over HTTP. */
    public int httpStatus;
    /** Self-contained Arrow IPC stream of the request batch (unary + stream init only). */
    public byte[] requestData;
    /** Stream lifecycle identifier (32-char lowercase hex); empty on unary. */
    public String streamId = "";
    /** True when the client cancelled the stream before end-of-stream. */
    public boolean cancelled;
    /** Transport-level request metadata (e.g. HTTP headers) captured by the auth scope; may be null. */
    public Map<String, ?> transportMetadata;

    /** Sticky-session id (hex), or null when no session was bound. */
    public String sessionId;
    /** Sticky-session action: {@code "none" | "resume" | "open" | "close"}. */
    public String sessionAction;
}
