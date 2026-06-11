// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.AuthContext;
import jakarta.servlet.http.HttpServletRequest;

import java.util.Arrays;
import java.util.List;

/**
 * Authenticate an incoming HTTP request and produce an {@link AuthContext}.
 *
 * <p>Implementations extract credentials from headers / cookies / the TLS layer
 * (via proxy headers) and either return an authenticated {@link AuthContext}
 * or throw an {@link AuthException} (mapped to HTTP 401) or
 * {@link SecurityException} (mapped to HTTP 403).</p>
 *
 * <p>Non-credential exceptions propagate as HTTP 500 — the framework will not
 * masquerade bugs as authentication failures.</p>
 */
@FunctionalInterface
public interface Authenticator {

    /**
     * Authenticate the request.
     *
     * @param request the inbound servlet request to extract credentials from
     * @return the resulting context; may be {@link AuthContext#ANONYMOUS} (or any
     *         unauthenticated context) to indicate "no credentials, but not an error"
     * @throws AuthException if credentials are missing or invalid (mapped to HTTP 401)
     */
    AuthContext authenticate(HttpServletRequest request) throws AuthException;

    /** Always returns the anonymous context; the default when none is configured. */
    Authenticator ANONYMOUS = request -> AuthContext.ANONYMOUS;

    /**
     * Compose authenticators, returning the first one that yields an
     * authenticated context. Unauthenticated (anonymous) results fall through
     * to the next chain member. Mirrors Python's {@code chain_authenticate}.
     *
     * @param authenticators chain members, tried in order
     * @return a composite authenticator that returns the first authenticated
     *         context, rethrows the last {@link AuthException} if every member
     *         failed, or returns {@link AuthContext#ANONYMOUS} if all members
     *         declined without error
     */
    static Authenticator chain(Authenticator... authenticators) {
        List<Authenticator> list = Arrays.asList(authenticators);
        return request -> {
            AuthException lastFailure = null;
            for (Authenticator a : list) {
                try {
                    AuthContext ctx = a.authenticate(request);
                    if (ctx != null && ctx.authenticated()) return ctx;
                } catch (AuthException e) {
                    lastFailure = e;
                }
            }
            if (lastFailure != null) throw lastFailure;
            return AuthContext.ANONYMOUS;
        };
    }
}
