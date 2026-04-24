// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import java.util.Collections;
import java.util.Map;

public final class AuthContext {

    public static final AuthContext ANONYMOUS =
            new AuthContext(null, false, null, Collections.emptyMap());

    private final String domain;
    private final boolean authenticated;
    private final String principal;
    private final Map<String, Object> claims;

    public AuthContext(String domain, boolean authenticated,
                       String principal, Map<String, Object> claims) {
        this.domain = domain;
        this.authenticated = authenticated;
        this.principal = principal;
        this.claims = claims != null ? claims : Collections.emptyMap();
    }

    public String domain() { return domain; }
    public boolean authenticated() { return authenticated; }
    public String principal() { return principal; }
    public Map<String, Object> claims() { return claims; }

    public void requireAuthenticated() {
        if (!authenticated) throw new SecurityException("Authentication required");
    }
}
