// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http.auth;

import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.http.AuthException;
import farm.query.vgirpc.http.HttpRequestStub;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class BearerAuthenticatorTest {

    @Test
    void valid_bearer_returns_context() throws Exception {
        AuthContext ok = new AuthContext("test", true, "alice", Collections.emptyMap());
        BearerAuthenticator a = BearerAuthenticator.fromMap(Map.of("secret-token", ok));
        AuthContext out = a.authenticate(HttpRequestStub.withBearer("secret-token"));
        assertTrue(out.authenticated());
        assertEquals("alice", out.principal());
    }

    @Test
    void missing_header_throws() {
        BearerAuthenticator a = BearerAuthenticator.fromMap(Map.of("t",
                new AuthContext("test", true, "x", Collections.emptyMap())));
        AuthException e = assertThrows(AuthException.class,
                () -> a.authenticate(HttpRequestStub.withHeaders(Map.of())));
        assertTrue(e.getMessage().toLowerCase(Locale.ROOT).contains("missing"));
    }

    @Test
    void unknown_token_throws() {
        BearerAuthenticator a = BearerAuthenticator.fromMap(Map.of("good",
                new AuthContext("test", true, "x", Collections.emptyMap())));
        assertThrows(AuthException.class, () -> a.authenticate(HttpRequestStub.withBearer("nope")));
    }

    @Test
    void api_key_header_also_accepted() throws Exception {
        AuthContext ok = new AuthContext("test", true, "svc", Collections.emptyMap());
        BearerAuthenticator a = BearerAuthenticator.fromMap(Map.of("k", ok));
        AuthContext out = a.authenticate(HttpRequestStub.withHeaders(Map.of("X-API-Key", "k")));
        assertEquals("svc", out.principal());
    }

    @Test
    void wrong_scheme_throws() {
        BearerAuthenticator a = BearerAuthenticator.fromMap(Map.of("t",
                new AuthContext("test", true, "x", Collections.emptyMap())));
        assertThrows(AuthException.class,
                () -> a.authenticate(HttpRequestStub.withHeaders(Map.of("Authorization", "Basic abcd"))));
    }
}
