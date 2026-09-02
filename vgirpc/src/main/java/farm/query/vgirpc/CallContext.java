// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.http.SessionRegistry;
import farm.query.vgirpc.http.SessionScope;
import farm.query.vgirpc.identity.PeerEvidenceSet;
import farm.query.vgirpc.log.Level;
import farm.query.vgirpc.log.Message;

import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Per-request context injected into method implementations that declare a
 * {@link CallContext} parameter. Provides authentication info, transport
 * metadata, and a callback for emitting client-directed log batches.
 */
public final class CallContext {

    private final AuthContext auth;
    private final PeerEvidenceSet peerEvidence;
    private final Consumer<Message> emitClientLog;
    private final Map<String, Object> transportMetadata;
    private final String serverId;
    private final String methodName;
    private final String protocolName;
    private final String requestId;
    private final TransportKind kind;
    private final Long responseLimitBytes;
    private final Long preferredResponseBytes;

    /**
     * Create a per-request context. Constructed by the framework at dispatch
     * time; service implementations never build one themselves — they receive
     * it by declaring a {@code CallContext} parameter on the implementation method.
     *
     * @param auth authenticated principal; {@code null} becomes {@link AuthContext#ANONYMOUS}
     * @param emitClientLog sink for client-directed log messages
     * @param transportMetadata transport-level metadata for the request; {@code null} becomes empty
     * @param serverId the serving server's id
     * @param methodName the RPC method being dispatched
     * @param protocolName the service/protocol name
     * @param requestId per-request id; {@code null} becomes {@code ""}
     */
    public CallContext(AuthContext auth,
                       Consumer<Message> emitClientLog,
                       Map<String, Object> transportMetadata,
                       String serverId,
                       String methodName,
                       String protocolName,
                       String requestId) {
        this(auth, emitClientLog, transportMetadata, serverId, methodName,
                protocolName, requestId, null);
    }

    /**
     * Create a per-request context including the selected transport kind.
     *
     * <p>The seven-argument constructor remains source-compatible and records
     * an unknown ({@code null}) kind for callers constructing contexts outside
     * the framework.</p>
     *
     * @param auth authenticated principal; {@code null} becomes {@link AuthContext#ANONYMOUS}
     * @param emitClientLog sink for client-directed log messages
     * @param transportMetadata transport-level request metadata
     * @param serverId serving server id
     * @param methodName RPC method name
     * @param protocolName service/protocol name
     * @param requestId request correlation id
     * @param kind concrete transport, or {@code null} when unknown
     */
    public CallContext(AuthContext auth,
                       Consumer<Message> emitClientLog,
                       Map<String, Object> transportMetadata,
                       String serverId,
                       String methodName,
                       String protocolName,
                       String requestId,
                       TransportKind kind) {
        this(auth, emitClientLog, transportMetadata, serverId, methodName,
                protocolName, requestId, kind, PeerEvidenceSet.EMPTY, null, null);
    }

    /** Create a context including its immutable off-wire peer evidence snapshot. */
    public CallContext(AuthContext auth,
                       Consumer<Message> emitClientLog,
                       Map<String, Object> transportMetadata,
                       String serverId,
                       String methodName,
                       String protocolName,
                       String requestId,
                       TransportKind kind,
                       PeerEvidenceSet peerEvidence) {
        this(auth, emitClientLog, transportMetadata, serverId, methodName,
                protocolName, requestId, kind, peerEvidence, null, null);
    }

    /** Create a context including negotiated off-wire response budgets. */
    public CallContext(AuthContext auth,
                       Consumer<Message> emitClientLog,
                       Map<String, Object> transportMetadata,
                       String serverId,
                       String methodName,
                       String protocolName,
                       String requestId,
                       TransportKind kind,
                       PeerEvidenceSet peerEvidence,
                       Long responseLimitBytes,
                       Long preferredResponseBytes) {
        this.auth = auth != null ? auth : AuthContext.ANONYMOUS;
        this.peerEvidence = peerEvidence != null ? peerEvidence : PeerEvidenceSet.EMPTY;
        this.emitClientLog = emitClientLog;
        this.transportMetadata = transportMetadata != null
                ? Collections.unmodifiableMap(transportMetadata)
                : Collections.emptyMap();
        this.serverId = serverId;
        this.methodName = methodName;
        this.protocolName = protocolName;
        this.requestId = requestId != null ? requestId : "";
        this.kind = kind;
        Object metadataLimit = this.transportMetadata.get(RESPONSE_LIMIT_BYTES_KEY);
        Object metadataPreferred = this.transportMetadata.get(PREFERRED_RESPONSE_BYTES_KEY);
        this.responseLimitBytes = responseLimitBytes != null ? responseLimitBytes
                : metadataLimit instanceof Number n ? n.longValue() : null;
        this.preferredResponseBytes = preferredResponseBytes != null ? preferredResponseBytes
                : metadataPreferred instanceof Number n ? n.longValue() : null;
    }

    /**
     * The authenticated principal for this request.
     *
     * @return the caller's {@link AuthContext}; {@link AuthContext#ANONYMOUS} if unauthenticated
     */
    public AuthContext auth() { return auth; }
    /** Verified or observed transport-peer evidence; empty when disabled. */
    public PeerEvidenceSet peerEvidence() { return peerEvidence; }
    /**
     * Transport-level request metadata (e.g. {@code remote_addr} on HTTP).
     *
     * @return an unmodifiable, never-{@code null} map of transport metadata
     */
    public Map<String, Object> transportMetadata() { return transportMetadata; }

    /** Transport-metadata key under which the request's parsed cookies live (HTTP only). */
    public static final String REQUEST_COOKIES_KEY = "vgi_request_cookies";
    /** Transport-metadata key for the mutable sink the transport drains into {@code Set-Cookie} (HTTP only). */
    public static final String RESPONSE_COOKIES_KEY = "vgi_response_cookies";
    /** Internal transport-metadata key for the negotiated hard response budget. */
    public static final String RESPONSE_LIMIT_BYTES_KEY = "vgi_response_limit_bytes";
    /** Internal transport-metadata key for the server's clamped batching target. */
    public static final String PREFERRED_RESPONSE_BYTES_KEY = "vgi_preferred_response_bytes";

    /**
     * Cookies presented on the request ({@code Cookie} header), name → value.
     * Empty for non-HTTP transports, so a fixture can detect "running over HTTP"
     * by a non-empty map after it has set one on a prior request.
     *
     * @return an immutable view of the request cookies; empty when none/not HTTP
     */
    @SuppressWarnings("unchecked")
    public Map<String, String> cookies() {
        Object c = transportMetadata.get(REQUEST_COOKIES_KEY);
        return c instanceof Map ? Collections.unmodifiableMap((Map<String, String>) c) : Collections.emptyMap();
    }

    /**
     * Set a response cookie (HTTP transport emits {@code Set-Cookie: name=value;
     * Path=/; HttpOnly; SameSite=Strict}). A no-op on transports without cookies.
     *
     * @param name  the cookie name
     * @param value the cookie value
     */
    @SuppressWarnings("unchecked")
    public void setCookie(String name, String value) {
        Object c = transportMetadata.get(RESPONSE_COOKIES_KEY);
        if (c instanceof Map) ((Map<String, String>) c).put(name, value);
    }
    /**
     * The serving server's id, as stamped on response batches.
     *
     * @return the server identifier
     */
    public String serverId() { return serverId; }
    /**
     * The RPC method name being dispatched.
     *
     * @return the name of the method this context was created for
     */
    public String methodName() { return methodName; }
    /**
     * The service/protocol name (the service interface's simple name).
     *
     * @return the protocol name advertised in {@code __describe__}
     */
    public String protocolName() { return protocolName; }
    /**
     * The per-request id assigned by the transport.
     *
     * @return the request id, or {@code ""} when the transport assigns none
     */
    public String requestId() { return requestId; }

    /**
     * Concrete transport handling this call.
     *
     * @return transport kind, or {@code null} only for contexts constructed
     *     through the legacy constructor outside framework dispatch
     */
    public TransportKind kind() { return kind; }

    /** Negotiated hard limit for this decoded Arrow IPC response, or null when unbounded. */
    public Long responseLimitBytes() { return responseLimitBytes; }

    /** Server batching target, clamped to {@link #responseLimitBytes()}, or null when unset. */
    public Long preferredResponseBytes() { return preferredResponseBytes; }

    /**
     * Emit a client-directed log batch.
     *
     * @param level log level
     * @param msg log message text
     */
    public void clientLog(Level level, String msg) {
        emitClientLog.accept(new Message(level, msg, null));
    }

    /**
     * Emit a client-directed log batch with structured extra fields.
     *
     * @param level log level
     * @param msg log message text
     * @param extra structured fields serialized into {@code vgi_rpc.log_extra}
     */
    public void clientLog(Level level, String msg, Map<String, Object> extra) {
        emitClientLog.accept(new Message(level, msg, extra));
    }

    /**
     * Emit a pre-built client-directed log {@link Message}.
     *
     * @param m the message to serialize as a zero-row log batch on the response stream
     */
    public void emitClientLog(Message m) { emitClientLog.accept(m); }

    // --- Sticky-session API (HTTP-only) ---------------------------------
    //
    // Bound to the {@link SessionScope} thread-local installed by the HTTP
    // sticky middleware. Outside the HTTP transport these methods raise
    // {@link RuntimeException}.

    /**
     * Register {@code state} in the per-worker sticky-session registry.
     * On response the server emits a {@code VGI-Session} header carrying
     * an AEAD-sealed token; subsequent client calls echoing the token
     * resume the same session.
     *
     * @param state user-owned state object; may implement
     *     {@link AutoCloseable} to receive an explicit close on eviction
     * @param ttlSecondsOrNull optional per-session TTL override; {@code null}
     *     uses the server's default TTL.
     * @throws RuntimeException if the server doesn't have sticky enabled or
     *     the client failed to send {@code VGI-Session-Accept: true}
     * @throws ServerDrainingError if the server is in drain mode
     */
    public void openSession(Object state, Long ttlSecondsOrNull) {
        SessionScope s = SessionScope.current();
        if (s == null || !s.stickyEnabled) {
            throw new RuntimeException("ctx.openSession requires sticky sessions to be enabled on the server");
        }
        if (!s.clientOptedIn) {
            throw new RuntimeException("ctx.openSession requires VGI-Session-Accept: true on the request");
        }
        if (s.entry() != null) {
            throw new RuntimeException("a session is already bound to this request");
        }
        // Registry holds the canonical Entry (with the per-session lock and
        // authoritative expiry) and returns it with the opening request's
        // lease already held; bind it directly into the scope.
        SessionRegistry.Entry entry = s.openLease(state, ttlSecondsOrNull);
        long now = System.currentTimeMillis() / 1000;
        s.bindEntry(entry, SessionScope.ACTION_OPEN);
        String tokenB64 = s.mintSessionToken(entry.sessionId, now, entry.expiresAtSeconds);
        s.setMintTokenB64(tokenB64);
    }

    /**
     * The state bound to this request's sticky session.
     *
     * @return the user-owned state object registered via
     *     {@link #openSession(Object, Long)}, or {@code null} when no session
     *     is active (including on non-HTTP transports)
     */
    public Object session() {
        SessionScope s = SessionScope.current();
        if (s == null) return null;
        SessionRegistry.Entry e = s.entry();
        return e == null ? null : e.state();
    }

    /**
     * The current sticky-session id.
     *
     * @return the hex-encoded 12-byte session id, or {@code null} when no
     *     session is bound to this request
     */
    public String sessionId() {
        SessionScope s = SessionScope.current();
        return s == null ? null : s.sessionIdHex();
    }

    /**
     * Evict the current sticky session from the registry and arrange for
     * the server to emit {@code VGI-Session-Close: true} on the response.
     * Idempotent: a no-op when no session is bound.
     */
    public void closeSession() {
        SessionScope s = SessionScope.current();
        if (s == null) return;
        SessionRegistry.Entry entry = s.entry();
        if (entry != null) {
            s.registry.close(entry.sessionId, s.principalKey);
        }
        s.clearEntry();
        s.setAction(SessionScope.ACTION_CLOSE);
        s.setCloseSignal(true);
        s.setMintTokenB64(null);
    }
}
