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

    public static SessionScope current() { return CURRENT.get(); }

    public static AutoCloseable push(SessionScope s) {
        SessionScope prev = CURRENT.get();
        CURRENT.set(s);
        return () -> {
            if (prev == null) CURRENT.remove();
            else CURRENT.set(prev);
        };
    }

    public SessionRegistry.Entry entry() { return entry; }
    public void bindEntry(SessionRegistry.Entry e, String action) {
        this.entry = e;
        this.sessionId = e != null ? e.sessionId : null;
        this.action = action;
    }

    /** Replace just the action label without rebinding the entry. */
    public byte[] sessionId() { return sessionId == null ? null : sessionId.clone(); }
    public String sessionIdHex() {
        return sessionId == null ? null : SessionRegistry.hex(sessionId);
    }
    public String action() { return action; }
    public void setAction(String a) { this.action = a; }

    public String mintTokenB64() { return mintTokenB64; }
    public void setMintTokenB64(String b64) { this.mintTokenB64 = b64; }

    public boolean closeSignal() { return closeSignal; }
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
