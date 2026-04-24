// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.AuthContext;
import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class AuthenticatorTest {

    private static HttpServletRequest stubRequest() {
        return HttpRequestStub.withHeaders(Map.of());
    }

    @Test
    void anonymous_returns_unauthenticated() throws Exception {
        AuthContext ctx = Authenticator.ANONYMOUS.authenticate(stubRequest());
        assertFalse(ctx.authenticated());
    }

    @Test
    void chain_picks_first_authenticated() throws Exception {
        Authenticator always = req -> new AuthContext("test", true, "alice", Collections.emptyMap());
        Authenticator never = Authenticator.ANONYMOUS;
        Authenticator chained = Authenticator.chain(never, always);
        AuthContext ctx = chained.authenticate(stubRequest());
        assertTrue(ctx.authenticated());
        assertEquals("alice", ctx.principal());
    }

    @Test
    void chain_all_anonymous_yields_anonymous() throws Exception {
        AuthContext ctx = Authenticator.chain(Authenticator.ANONYMOUS, Authenticator.ANONYMOUS)
                .authenticate(stubRequest());
        assertFalse(ctx.authenticated());
    }

    @Test
    void chain_propagates_last_failure_when_nobody_authenticates() {
        Authenticator a = req -> { throw new InvalidCredentials("bad", "Bearer"); };
        Authenticator b = Authenticator.ANONYMOUS;
        AuthException e = assertThrows(AuthException.class,
                () -> Authenticator.chain(a, b).authenticate(stubRequest()));
        // The successful anonymous fall-through should NOT mask the earlier failure — once we've
        // seen a credential attempt we surface it so the caller can respond 401.
        assertEquals("bad", e.getMessage());
    }
}
