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
    /**
     * Wire name of the protocol that <em>owns the dispatched method</em>.
     *
     * <p>Not a server-wide default: a call to a co-hosted framework protocol names that
     * protocol, not the application. Fill it from {@link RpcServer#protocolIdentityFor}, which
     * returns this and {@link #protocolHash} together -- the two disagreeing is worse than
     * either being wrong alone.
     */
    public String protocol = "";
    /**
     * Canonical digest of the protocol named by {@link #protocol}.
     *
     * <p>The registry key a consumer decodes archived records against, so it must be that
     * protocol's digest and computed the canonical way
     * ({@link farm.query.vgirpc.hash.ProtocolHash}) -- a legacy or primary-protocol digest here
     * produces a record that is well-formed, passes the schema and decodes wrong.
     */
    public String protocolHash = "";
    /** The version label of the protocol named by {@link #protocol}; empty when it declares none. */
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
    /**
     * Decrypted stream state the client sent, on a stream continuation; null on
     * unary and on stream init. The on-wire token is an opaque AEAD ciphertext —
     * this is the plaintext, so a log reader can decode it without holding the
     * server's token key.
     */
    public byte[] requestState;
    /**
     * Decrypted stream state handed back to the client, on stream init and on
     * any continuation that mints a fresh cursor. Null on unary and on the
     * terminal continuation that closes the stream.
     */
    public byte[] responseState;
    /** True when the client cancelled the stream before end-of-stream. */
    public boolean cancelled;
    /** Transport-level request metadata (e.g. HTTP headers) captured by the auth scope; may be null. */
    public Map<String, ?> transportMetadata;
    /** Raw claims of the authenticated principal; may be null or empty. Redacted by the emitter, not here. */
    public Map<String, Object> claims;

    /** Sticky-session id (hex), or null when no session was bound. */
    public String sessionId;
    /** Sticky-session action: {@code "none" | "resume" | "open" | "close"}. */
    public String sessionAction;
}
