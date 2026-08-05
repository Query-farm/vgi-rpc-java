// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

/**
 * The closed set of machine-readable codes a 401 may carry, per
 * {@code docs/unauthorized-spec.md} §3 in the vgi-rpc reference repository.
 *
 * <p>A code names the <em>stage</em> that refused the request, never a
 * verifier's internal diagnosis: telling a caller their token expired is a
 * fact about something they hold, telling them which key id failed to resolve
 * turns the rejection into an oracle. Every proxy-proof outcome therefore
 * collapses onto {@link #PROXY_REQUIRED}.</p>
 *
 * <p>The set is closed so a client can switch on it — refresh a token on
 * {@link #EXPIRED_CREDENTIAL}, give up on {@link #INSUFFICIENT_SCOPE} — without
 * the set growing under it in a language it does not control. A failure that
 * maps onto none of these uses {@link #UNAUTHORIZED}; readers must treat an
 * unrecognised code the same way, since that means the server is newer, not
 * broken.</p>
 */
public enum AuthReason {

    /** No credential was presented at all. */
    MISSING_CREDENTIAL("missing_credential"),

    /** A credential was presented and rejected. */
    INVALID_CREDENTIAL("invalid_credential"),

    /** A well-formed credential outside its validity window. */
    EXPIRED_CREDENTIAL("expired_credential"),

    /**
     * The caller was identified but is not permitted.
     *
     * <p>Deliberately a 401 rather than a 403: the authenticator runs before
     * any method is resolved, so there is no route yet whose permissions could
     * be evaluated. A service wanting a true 403 raises it from the method
     * body.</p>
     */
    INSUFFICIENT_SCOPE("insufficient_scope"),

    /**
     * The request carried no evidence of having arrived through the trusted
     * proxy. Derived from server configuration, never from the request — see
     * {@link HttpServer.Config.Builder#proxyAuthHeaders(java.util.List)}.
     */
    PROXY_REQUIRED("proxy_required"),

    /** Refused, unclassified. The fallback. */
    UNAUTHORIZED("unauthorized");

    private final String code;

    AuthReason(String code) { this.code = code; }

    /**
     * The wire spelling carried by {@code VGI-Auth-Reason} and the JSON envelope.
     *
     * @return the lower-snake-case code
     */
    public String code() { return code; }

    @Override public String toString() { return code; }
}
