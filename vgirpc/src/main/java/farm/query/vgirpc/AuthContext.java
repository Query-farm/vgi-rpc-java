// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import java.util.Collections;
import java.util.Map;

/** Authenticated principal attached to a call by the transport. */
public record AuthContext(
        String domain,
        boolean authenticated,
        String principal,
        Map<String, Object> claims) {

    public static final AuthContext ANONYMOUS =
            new AuthContext(null, false, null, Collections.emptyMap());

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
