// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http.auth;

import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.http.AuthException;
import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class BearerAuthenticatorTest {

    /** Tiny stub that returns whatever headers the test prepared. */
    private static HttpServletRequest stub(Map<String, String> headers) {
        Map<String, String> h = new HashMap<>(headers);
        return (HttpServletRequest) java.lang.reflect.Proxy.newProxyInstance(
                BearerAuthenticatorTest.class.getClassLoader(),
                new Class<?>[]{HttpServletRequest.class},
                (proxy, method, args) -> {
                    if ("getHeader".equals(method.getName()) && args != null && args.length == 1) {
                        // Servlet spec says header lookup is case-insensitive.
                        String key = (String) args[0];
                        for (Map.Entry<String, String> e : h.entrySet()) {
                            if (e.getKey().equalsIgnoreCase(key)) return e.getValue();
                        }
                        return null;
                    }
                    return switch (method.getName()) {
                        case "hashCode" -> 0;
                        case "toString" -> "stub";
                        case "equals" -> proxy == args[0];
                        default -> null;
                    };
                });
    }

    @Test
    void valid_bearer_returns_context() throws Exception {
        AuthContext ok = new AuthContext("test", true, "alice", Collections.emptyMap());
        BearerAuthenticator a = BearerAuthenticator.fromMap(Map.of("secret-token", ok));
        AuthContext out = a.authenticate(stub(Map.of("Authorization", "Bearer secret-token")));
        assertTrue(out.authenticated());
        assertEquals("alice", out.principal());
    }

    @Test
    void missing_header_throws() {
        BearerAuthenticator a = BearerAuthenticator.fromMap(Map.of("t",
                new AuthContext("test", true, "x", Collections.emptyMap())));
        AuthException e = assertThrows(AuthException.class, () -> a.authenticate(stub(Map.of())));
        assertTrue(e.getMessage().toLowerCase().contains("missing"));
    }

    @Test
    void unknown_token_throws() {
        BearerAuthenticator a = BearerAuthenticator.fromMap(Map.of("good",
                new AuthContext("test", true, "x", Collections.emptyMap())));
        assertThrows(AuthException.class,
                () -> a.authenticate(stub(Map.of("Authorization", "Bearer nope"))));
    }

    @Test
    void api_key_header_also_accepted() throws Exception {
        AuthContext ok = new AuthContext("test", true, "svc", Collections.emptyMap());
        BearerAuthenticator a = BearerAuthenticator.fromMap(Map.of("k", ok));
        AuthContext out = a.authenticate(stub(Map.of("X-API-Key", "k")));
        assertEquals("svc", out.principal());
    }

    @Test
    void wrong_scheme_throws() {
        BearerAuthenticator a = BearerAuthenticator.fromMap(Map.of("t",
                new AuthContext("test", true, "x", Collections.emptyMap())));
        assertThrows(AuthException.class,
                () -> a.authenticate(stub(Map.of("Authorization", "Basic abcd"))));
    }
}
