// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.identity.GrantKeys;
import farm.query.vgirpc.identity.Identity;
import farm.query.vgirpc.identity.IdentityImpl;
import farm.query.vgirpc.identity.IdentityUnavailableError;
import farm.query.vgirpc.identity.IssuedGrant;
import farm.query.vgirpc.identity.SealedGrants;
import farm.query.vgirpc.identity.TokenIdentity;
import farm.query.vgirpc.identity.TokenResolveHook;
import farm.query.vgirpc.schema.ProtocolName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Identity credentials accepted as bearers (WIRE_PROTOCOL.md §16; IDENTITY_V1_SPEC.md §9.2-9.3).
 *
 * <p>The resolver here answers for almost anything, so a grant that wrongly fell through to it
 * would turn a 401 into a success -- a loud failure, not a quiet one.
 */
@Timeout(60)
final class IdentityBearerTest {

    private static final GrantKeys KEYS = new GrantKeys(List.of(key(0x10), key(0x30)), "test", 3600);
    private static final GrantKeys STRANGER = new GrantKeys(List.of(key(0x77)), "test", 3600);

    private static byte[] key(int start) {
        byte[] k = new byte[32];
        for (int i = 0; i < 32; i++) k[i] = (byte) (start + (start == 0x77 ? 0 : i));
        return k;
    }

    /** Resolves everything except two probes; counts calls so fall-through is observable. */
    private static final class Resolver implements TokenResolveHook {
        final AtomicInteger calls = new AtomicInteger();

        @Override
        public TokenIdentity resolve(String token) {
            calls.incrementAndGet();
            if ("unknown".equals(token)) return null;
            if ("down".equals(token)) throw new IdentityUnavailableError("store down", 5, null);
            if ("auth-down".equals(token)) throw new AuthUnavailableException("authority down", 7, null);
            return new TokenIdentity("resolved@example", "named", 300);
        }
    }

    private static String grant(String principal, GrantKeys keys) {
        return SealedGrants.mint(keys, principal, List.of("read"), "test", 600).token();
    }

    private static AuthContext auth(Authenticator a, String bearer) throws AuthException {
        return a.authenticate(bearer == null ? HttpRequestStub.withHeaders(Map.of())
                : HttpRequestStub.withBearer(bearer));
    }

    // --- the composed chain -------------------------------------------------------------

    @Test
    void aGrantIsAcceptedAsItsPrincipalWithNoAuthTime() throws Exception {
        Authenticator a = IdentityBearer.compose(null, KEYS, new Resolver());
        AuthContext ctx = auth(a, grant("alice", KEYS));
        assertEquals("grant", ctx.domain());
        assertEquals("alice", ctx.principal());
        assertEquals(List.of("read"), ctx.claims().get("scopes"));
        assertEquals("test", ctx.claims().get("purpose"));
        assertTrue(!ctx.claims().containsKey("auth_time"), "grants never mint grants");
    }

    @Test
    void aBadGrantStopsTheChainAndNeverReachesTheResolver() throws Exception {
        Resolver r = new Resolver();
        Authenticator a = IdentityBearer.compose(null, KEYS, r);
        String good = grant("alice", KEYS);
        String tampered = good.substring(0, good.length() - 5) + (good.charAt(good.length() - 5) == 'A' ? 'B' : 'A')
                + good.substring(good.length() - 4);
        for (String bad : List.of(tampered, grant("x", STRANGER), good + "=")) {
            AuthFailure e = assertThrows(AuthFailure.class, () -> auth(a, bad));
            assertEquals(AuthReason.INVALID_CREDENTIAL, e.reason());
        }
        String expired = SealedGrants.mint(KEYS, "alice", List.of(), "p", 60,
                System.currentTimeMillis() / 1000 - 3000, "g", new byte[24]).token();
        assertEquals(AuthReason.EXPIRED_CREDENTIAL, assertThrows(AuthFailure.class, () -> auth(a, expired)).reason());
        assertEquals(0, r.calls.get(), "a grant-shaped token must never reach resolveToken");
    }

    @Test
    void onlyTheExactPrefixReachesTheGrantVerifier() throws Exception {
        Authenticator a = IdentityBearer.compose(null, KEYS, new Resolver());
        String body = grant("alice", KEYS).substring(SealedGrants.TOKEN_PREFIX.length());
        assertEquals("token", auth(a, "vgig2." + body).domain());
        assertEquals("token", auth(a, body).domain());
    }

    @Test
    void theResolverBacksBearers() throws Exception {
        Resolver r = new Resolver();
        Authenticator a = IdentityBearer.compose(null, null, r);
        AuthContext ctx = auth(a, "opaque");
        assertEquals("token", ctx.domain());
        assertEquals("resolved@example", ctx.principal());
        assertEquals(Map.of("token_name", "named"), ctx.claims());
        assertThrows(InvalidCredentials.class, () -> auth(a, "unknown"));
        assertEquals(5, assertThrows(AuthUnavailableException.class, () -> auth(a, "down")).retryAfterSeconds());
        assertEquals(7, assertThrows(AuthUnavailableException.class, () -> auth(a, "auth-down")).retryAfterSeconds());
        int before = r.calls.get();
        assertThrows(InvalidCredentials.class, () -> auth(a, "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhbGljZSJ9.c2lnbmF0dXJl"));
        assertThrows(InvalidCredentials.class, () -> auth(a, "x".repeat(4097)));
        // Not even without grant keys: a grant-shaped token is never handed to the resolver.
        assertThrows(InvalidCredentials.class, () -> auth(a, grant("alice", KEYS)));
        assertEquals(before, r.calls.get(), "a JWS, an over-long or a vgig1. token is never resolved");
    }

    @Test
    void noCredentialStaysAnonymousAndTheDeploymentStillComesFirst() throws Exception {
        assertEquals(false, auth(IdentityBearer.compose(null, KEYS, new Resolver()), null).authenticated());
        Authenticator deployment = req -> {
            if ("Bearer static".equals(req.getHeader("Authorization"))) {
                return new AuthContext("static", true, "carol", Map.of());
            }
            throw new InvalidCredentials("not mine");
        };
        Authenticator a = IdentityBearer.compose(deployment, KEYS, new Resolver());
        assertEquals("static", auth(a, "static").domain());
        assertEquals("grant", auth(a, grant("alice", KEYS)).domain());
        assertSame(deployment, IdentityBearer.compose(deployment, null, null));
    }

    // --- end to end over HTTP ------------------------------------------------------------

    @ProtocolName("test.Who.v1")
    public interface Who {
        String who(CallContext ctx);
    }

    public static final class WhoImpl implements Who {
        @Override public String who(CallContext ctx) {
            AuthContext a = ctx.auth();
            return a.principal() + "|" + a.domain();
        }
    }

    /** Fresh user login: an {@code X-Test-Principal} header carrying a current auth_time. */
    private static final Authenticator LOGIN = req -> {
        String p = req.getHeader("X-Test-Principal");
        if (p != null) return new AuthContext("login", true, p, Map.of("auth_time", System.currentTimeMillis() / 1000.0));
        if (req.getHeader("Authorization") != null) throw new InvalidCredentials("not mine");
        return AuthContext.ANONYMOUS;
    };

    @Test
    void aMintedGrantLogsItsOwnerInOverHttp() throws Exception {
        RpcServer rpc = new RpcServer(Who.class, new WhoImpl());
        rpc.setGrantKeys(KEYS);
        rpc.setIdentity(IdentityImpl.builder().grantKeys(KEYS).resolveToken(new Resolver())
                .introspectPrincipals("proxy").build());
        HttpServer http = new HttpServer(rpc, HttpServer.Config.builder().prefix("/vgi").authenticator(LOGIN).build());
        http.start();
        String url = "http://127.0.0.1:" + http.port() + "/vgi";
        try {
            // Minted through the hosted identity's own issue_grant -- the sealed minter -- for a
            // freshly logged-in alice.
            CallContext fresh = new CallContext(new AuthContext("login", true, "alice",
                    Map.of("auth_time", System.currentTimeMillis() / 1000.0)), m -> { }, Map.of(),
                    "srv", "issue_grant", Identity.PROTOCOL_NAME, "");
            IssuedGrant grant = rpc.identity().issue_grant("nightly", List.of("read"), 600, fresh);
            assertTrue(grant.token().startsWith(SealedGrants.TOKEN_PREFIX));
            try (HttpRpcConnection bot = HttpRpcConnection.builder(url).bearerToken(grant.token()).build()) {
                assertEquals("alice|grant", bot.proxy(Who.class).who(null));
                // The same caller asking for a grant over HTTP: no auth_time, so stale_auth.
                RpcError e = assertThrows(RpcError.class, () -> issueGrantRaw(bot, "child"));
                assertEquals("stale_auth", e.errorKind(), "a grant cannot mint a grant");
            }
            try (HttpRpcConnection alice = HttpRpcConnection.builder(url).header("X-Test-Principal", "alice").build()) {
                // And the fresh login over HTTP mints one; any failure would be an RpcError.
                issueGrantRaw(alice, "nightly");
            }
            try (HttpRpcConnection svc = HttpRpcConnection.builder(url).bearerToken("opaque").build()) {
                assertEquals("resolved@example|token", svc.proxy(Who.class).who(null));
            }
            try (HttpRpcConnection forged = HttpRpcConnection.builder(url).bearerToken(grant("alice", STRANGER)).build()) {
                assertEquals("AuthenticationError",
                        assertThrows(RpcError.class, () -> forged.proxy(Who.class).who(null)).errorType());
            }
        } finally {
            http.stop();
        }
    }

    /** {@code vgi_rpc.Identity.v1/issue_grant}, addressed by name: no Java interface may claim
     *  the reserved prefix, so the typed proxy cannot reach it. */
    private static byte[] issueGrantRaw(HttpRpcConnection conn, String purpose) throws Exception {
        org.apache.arrow.vector.types.pojo.Schema params = farm.query.vgirpc.ServiceIntrospector
                .describe(Identity.class).get("issue_grant").paramsSchema();
        Map<String, Object> row = new java.util.LinkedHashMap<>();
        row.put("purpose", purpose);
        row.put("scopes", List.of("read"));
        row.put("ttl_seconds", 60L);
        try (org.apache.arrow.vector.VectorSchemaRoot root = farm.query.vgirpc.marshal.Marshalling.encodeRow(
                params, row, farm.query.vgirpc.wire.Allocators.root())) {
            return conn.callUnaryRaw(Identity.PROTOCOL_NAME, "", "issue_grant",
                    new farm.query.vgirpc.AnnotatedBatch(root, Map.of()));
        }
    }

    @Test
    void grantKeysAloneHostIssueGrantAndANonMatchingIdentityIsRefused() {
        RpcServer rpc = new RpcServer(Who.class, new WhoImpl());
        rpc.setGrantKeys(KEYS);
        assertEquals(java.util.Set.of("issue_grant"), rpc.identityMethodTable().keySet());
        assertThrows(IllegalArgumentException.class, () -> rpc.setIdentity(
                IdentityImpl.builder().resolveToken(new Resolver()).introspectPrincipals("proxy").build()));
        rpc.setGrantKeys(null);
        assertEquals(java.util.Set.of(), rpc.identityMethodTable().keySet());
    }

    @Test
    void aProxyEvidenceGateRefusesImplicitComposition() {
        RpcServer rpc = new RpcServer(Who.class, new WhoImpl());
        rpc.setGrantKeys(KEYS);
        assertThrows(IllegalArgumentException.class, () -> new HttpServer(rpc,
                HttpServer.Config.builder().authenticator(LOGIN).proxyAuthHeaders(List.of("X-Forwarded-Client-Cert")).build()));
        // Explicit composition is the way through.
        new HttpServer(rpc, HttpServer.Config.builder()
                .authenticator(IdentityBearer.explicit(LOGIN)).proxyAuthHeaders(List.of("X-Forwarded-Client-Cert")).build());
    }
}
