// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import java.util.Collections;
import java.util.Map;

/**
 * Authenticated principal attached to a call by the transport. Populated by
 * transport-specific {@code Authenticator}s (e.g. JWT or mTLS validation on
 * HTTP) and injected into method implementations via {@link CallContext#auth()}.
 *
 * @param domain authentication scheme that produced this context (e.g.
 *     {@code "bearer"}, {@code "mtls"}), or {@code null} for unauthenticated requests
 * @param authenticated whether the caller was successfully authenticated
 * @param principal identity of the caller (e.g. username, service account),
 *     or {@code null} if unauthenticated
 * @param claims arbitrary claims from the authentication token; never {@code null}
 */
public record AuthContext(
        String domain,
        boolean authenticated,
        String principal,
        Map<String, Object> claims) {

    /** The unauthenticated context used when a transport performs no authentication. */
    public static final AuthContext ANONYMOUS =
            new AuthContext(null, false, null, Collections.emptyMap());

    /** Defensively copies {@code claims}; {@code null} becomes an empty map. */
    public AuthContext {
        claims = claims != null ? Map.copyOf(claims) : Collections.emptyMap();
    }

    /**
     * Assert that the caller is authenticated.
     *
     * @throws SecurityException if {@link #authenticated()} is {@code false}
     */
    public void requireAuthenticated() {
        if (!authenticated) throw new SecurityException("Authentication required");
    }
}
