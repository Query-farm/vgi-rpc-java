// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

/**
 * Per-request sticky-session scope installed by the HTTP sticky
 * middleware before dispatch and torn down after the response. The
 * {@link farm.query.vgirpc.CallContext} session API reads from / writes
 * to this scope.
 *
 * <p>The scope tracks four pieces of state:
 * <ul>
 *   <li>the live {@link SessionRegistry.Entry} (null until a session is
 *       resumed or opened),</li>
 *   <li>the 12-byte session id (kept across close so access logs can
 *       still report it),</li>
 *   <li>the current sticky action — one of {@code "none" | "resume" |
 *       "open" | "close"} — surfaced via the access log,</li>
 *   <li>flags signalling that the response middleware must mint a fresh
 *       {@code VGI-Session} header or a {@code VGI-Session-Close} header.</li>
 * </ul>
 */
public final class SessionScope {

    public static final String ACTION_NONE = "none";
    public static final String ACTION_RESUME = "resume";
    public static final String ACTION_OPEN = "open";
    public static final String ACTION_CLOSE = "close";

    private static final ThreadLocal<SessionScope> CURRENT = new ThreadLocal<>();

    /** {@code true} if the inbound request carried {@code VGI-Session-Accept: true}. */
    public final boolean clientOptedIn;
    /** {@code true} if the sticky feature is enabled on this server. */
    public final boolean stickyEnabled;
    /** Server principal-key bound to the request (used as AAD identity). */
    public final String principalKey;
    /** Auth principal string used as AAD in seal/open. */
    public final String principal;
    /** Server id used inside session tokens. */
    public final String serverId;
    public final SessionRegistry registry;
    // The raw token key is package-private and only used through {@link #mintSessionToken}
    // — keeping the byte[] out of the public surface so user code with a
    // CallContext can't reach in and reuse the master key.
    final byte[] tokenKey;

    // Mutable per-request fields:
    private SessionRegistry.Entry entry;
    private byte[] sessionId;
    private String action = ACTION_NONE;
    private String mintTokenB64;
    private boolean closeSignal;

    /**
     * @param clientOptedIn whether the request carried {@code VGI-Session-Accept: true}
     * @param stickyEnabled whether the server has sticky sessions enabled
     * @param principalKey key binding a session to its principal
     * @param principal the authenticated principal
     * @param serverId server id embedded in minted session tokens
     * @param tokenKey AEAD key for sealing session tokens (kept off the public surface)
     * @param registry the per-worker session registry
     */
    public SessionScope(boolean clientOptedIn, boolean stickyEnabled,
                         String principalKey, String principal,
                         String serverId,
                         byte[] tokenKey, SessionRegistry registry) {
        this.clientOptedIn = clientOptedIn;
        this.stickyEnabled = stickyEnabled;
        this.principalKey = principalKey;
        this.principal = principal;
        this.serverId = serverId;
        this.tokenKey = tokenKey;
        this.registry = registry;
    }

    /** @return the {@link SessionScope} bound to the current thread, or {@code null}. */
    public static SessionScope current() { return CURRENT.get(); }

    /**
     * Bind {@code s} to the current thread, returning a handle that restores the
     * previous scope when closed (use in try-with-resources).
     *
     * @param s the scope to install
     * @return a closeable that pops the scope
     */
    public static AutoCloseable push(SessionScope s) {
        SessionScope prev = CURRENT.get();
        CURRENT.set(s);
        return () -> {
            if (prev == null) CURRENT.remove();
            else CURRENT.set(prev);
        };
    }

    /** @return the session entry bound to this request, or {@code null}. */
    public SessionRegistry.Entry entry() { return entry; }
    /** Bind a registry entry and record the dispatch action that produced it. */
    public void bindEntry(SessionRegistry.Entry e, String action) {
        this.entry = e;
        this.sessionId = e != null ? e.sessionId : null;
        this.action = action;
    }

    /** @return a copy of the bound session id, or {@code null} if none. */
    public byte[] sessionId() { return sessionId == null ? null : sessionId.clone(); }
    /** @return the hex-encoded bound session id, or {@code null} if none. */
    public String sessionIdHex() {
        return sessionId == null ? null : SessionRegistry.hex(sessionId);
    }
    /** @return the dispatch action label (open/resume/close/none). */
    public String action() { return action; }
    /** Replace just the action label without rebinding the entry. */
    public void setAction(String a) { this.action = a; }

    /** @return the base64 session token to emit on the response, or {@code null}. */
    public String mintTokenB64() { return mintTokenB64; }
    /** Set the base64 session token to emit on the response. */
    public void setMintTokenB64(String b64) { this.mintTokenB64 = b64; }

    /** @return whether the response should signal {@code VGI-Session-Close: true}. */
    public boolean closeSignal() { return closeSignal; }
    /** Set whether the response should signal session close. */
    public void setCloseSignal(boolean v) { this.closeSignal = v; }

    /** Clear the bound session after close so {@code CallContext.session()} returns null. */
    public void clearEntry() { this.entry = null; }

    /** Mint a fresh AEAD-sealed session token for the given session id.
     *  Encapsulates the master token key so user code holding a
     *  {@link farm.query.vgirpc.CallContext} can't read it directly. */
    public String mintSessionToken(byte[] sessionId, long createdAtSeconds, long expiresAtSeconds) {
        SessionToken tok = new SessionToken(serverId, sessionId, createdAtSeconds, expiresAtSeconds);
        return new String(tok.pack(tokenKey, principal), java.nio.charset.StandardCharsets.US_ASCII);
    }
}
