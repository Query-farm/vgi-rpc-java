// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.identity;

import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.HasErrorKind;
import farm.query.vgirpc.Reflection;
import farm.query.vgirpc.RpcMethodInfo;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.http.AuthException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code vgi_rpc.Identity.v1} -- resolving a credential, and minting a grant.
 *
 * <p>The two methods are guarded very differently and the difference is the point, so most of
 * what is tested here is the <em>asymmetry</em>: introspection answers a question about somebody
 * else's credential and is therefore an oracle that has to be locked down; issuance is always
 * about the caller and therefore is not.
 *
 * <p>Ported from the Python reference's {@code tests/test_token_identity.py}. The digests in
 * {@link HashVectors} are the cross-port contract and are not negotiable: a mismatch means this
 * port and another would disagree about whether they speak the same protocol.
 */
final class IdentityProtocolTest {

    // --- fixtures ----------------------------------------------------------

    private static AuthContext auth(String principal) {
        return auth(principal, true, null);
    }

    private static AuthContext auth(String principal, boolean authenticated, Double authTime) {
        Map<String, Object> claims = new HashMap<>();
        if (authTime != null) claims.put("auth_time", authTime);
        return new AuthContext("test", authenticated, principal, claims);
    }

    private static CallContext ctx(AuthContext a) {
        return new CallContext(a, msg -> { }, Map.of(), "srv", "m", Identity.PROTOCOL_NAME, "");
    }

    private static double now() {
        return System.currentTimeMillis() / 1000.0;
    }

    /** Resolves exactly one credential; everything else is "the store answered: unknown". */
    private static final TokenResolveHook RESOLVER =
            token -> "good".equals(token) ? new TokenIdentity("bob", "ci-key") : null;

    private static final GrantMintHook MINTER =
            (principal, purpose, scopes, ttl) ->
                    new IssuedGrant("grant-for-" + principal, now() + ttl, "g1");

    private static IdentityImpl resolving() {
        return IdentityImpl.builder().resolveToken(RESOLVER).introspectPrincipals("proxy").build();
    }

    /** A service interface with no identity of its own, for the absent-by-default check. */
    interface Plain {
        default void ping() { }
    }

    // --- registration ------------------------------------------------------

    /** Absent beats routed-and-refusing. */
    @Nested
    final class Registration {

        /** A dependency upgrade must not grow an oracle on every worker. */
        @Test
        void absentByDefault() {
            RpcServer server = new RpcServer(Plain.class, new Plain() { });
            assertNull(server.identity(),
                    "identity must not be hosted unless the deployment asked for it");
        }

        /**
         * What the server hosts describes what it actually does.
         *
         * <p>A worker that resolves credentials but does not mint grants offers one method, and a
         * client learns that from reflection rather than by calling and reading an error.
         */
        @Test
        void onlyMethodsWithHooksAreHosted() {
            assertEquals(Set.of("introspect_token"), resolving().offeredMethods());
            assertEquals(Set.of("issue_grant"),
                    IdentityImpl.builder().mintGrant(MINTER).build().offeredMethods());
            assertEquals(Set.of("introspect_token", "issue_grant"),
                    IdentityImpl.builder()
                            .resolveToken(RESOLVER)
                            .mintGrant(MINTER)
                            .introspectPrincipals("proxy")
                            .build()
                            .offeredMethods());
        }

        /** Narrowing the method set narrows the protocol hash with it. */
        @Test
        void hostedBindingCarriesOnlyTheOfferedMethods() {
            RpcServer server = new RpcServer(Plain.class, new Plain() { });
            server.setIdentity(IdentityImpl.builder().mintGrant(MINTER).build());
            assertEquals(List.of("issue_grant"),
                    new ArrayList<>(RpcServer.identityMethods(server.identity()).keySet()));
        }

        /** An implementation offering nothing registers nothing at all. */
        @Test
        void anImplementationWithNoHooksIsNotRegistered() {
            RpcServer server = new RpcServer(Plain.class, new Plain() { });
            server.setIdentity(IdentityImpl.builder().build());
            assertNull(server.identity(),
                    "a binding with no methods is a routing entry that can only refuse");
        }

        /** Framework-owned, so an application cannot impersonate it. */
        @Test
        void claimsTheReservedPrefix() {
            assertTrue(Identity.PROTOCOL_NAME.startsWith("vgi_rpc."), Identity.PROTOCOL_NAME);
        }
    }

    // --- the hash vectors --------------------------------------------------

    /**
     * The cross-port contract.
     *
     * <p>These digests come from the Python reference. The two single-method ones are not
     * decoration: they prove that method-level narrowing actually narrows the fingerprint rather
     * than hosting a method that refuses.
     */
    @Nested
    final class HashVectors {

        private Map<String, RpcMethodInfo> methodsFor(IdentityImpl impl) {
            return RpcServer.identityMethods(impl);
        }

        private String hash(IdentityImpl impl) {
            return Reflection.bindingHash(Identity.PROTOCOL_NAME, methodsFor(impl));
        }

        private String preimage(IdentityImpl impl) {
            return Reflection.bindingPreimage(Identity.PROTOCOL_NAME, methodsFor(impl));
        }

        @Test
        void bothMethods() {
            IdentityImpl impl = IdentityImpl.builder()
                    .resolveToken(RESOLVER).mintGrant(MINTER).introspectPrincipals("proxy").build();
            assertEquals("8317f2ad8e2476bb99e8b94800ab79b19a8cf0c6bdd6d66c2d82bd62ffbe69d5",
                    hash(impl), () -> "preimage: " + preimage(impl));
        }

        @Test
        void introspectOnly() {
            IdentityImpl impl = resolving();
            assertEquals("27b75bef22e4c70baab92a5188a473506b89055d2cb2b58cc187f6fe7a436385",
                    hash(impl), () -> "preimage: " + preimage(impl));
        }

        @Test
        void issueOnly() {
            IdentityImpl impl = IdentityImpl.builder().mintGrant(MINTER).build();
            assertEquals("c71b12f453310139b6b6a445378064661c52711d03ae1e4fba29b8f7976ef4d8",
                    hash(impl), () -> "preimage: " + preimage(impl));
        }

        /**
         * The canonical preimage, so a digest failure is a JSON diff rather than a guess.
         *
         * <p>{@code scopes} being {@code list<item?:utf8>} -- item nullable -- is the single most
         * likely thing to get wrong; TypeScript shipped that bug once across every list type.
         */
        @Test
        void theCanonicalPreimageMatchesTheReference() {
            IdentityImpl impl = IdentityImpl.builder()
                    .resolveToken(RESOLVER).mintGrant(MINTER).introspectPrincipals("proxy").build();
            assertEquals(
                    "{\"methods\":[{\"has_header\":false,\"has_return\":true,"
                            + "\"name\":\"introspect_token\","
                            + "\"params\":[{\"name\":\"token\",\"nullable\":false,\"type\":\"utf8\"}],"
                            + "\"result\":[{\"name\":\"result\",\"nullable\":false,\"type\":\"binary\"}],"
                            + "\"type\":\"unary\"},"
                            + "{\"has_header\":false,\"has_return\":true,\"name\":\"issue_grant\","
                            + "\"params\":[{\"name\":\"purpose\",\"nullable\":false,\"type\":\"utf8\"},"
                            + "{\"name\":\"scopes\",\"nullable\":false,\"type\":\"list<item?:utf8>\"},"
                            + "{\"name\":\"ttl_seconds\",\"nullable\":false,\"type\":\"int64\"}],"
                            + "\"result\":[{\"name\":\"result\",\"nullable\":false,\"type\":\"binary\"}],"
                            + "\"type\":\"unary\"}],\"protocol\":\"vgi_rpc.Identity.v1\"}",
                    preimage(impl));
        }

        /** Narrowing is visible on the wire, which is how a client discovers it. */
        @Test
        void narrowingChangesTheHash() {
            String both = hash(IdentityImpl.builder()
                    .resolveToken(RESOLVER).mintGrant(MINTER).introspectPrincipals("proxy").build());
            assertNotEquals(both, hash(resolving()));
            assertNotEquals(both, hash(IdentityImpl.builder().mintGrant(MINTER).build()));
            assertNotEquals(hash(resolving()),
                    hash(IdentityImpl.builder().mintGrant(MINTER).build()));
        }
    }

    // --- introspection -----------------------------------------------------

    /**
     * The answer is an identity assertion the asker acts on with its own credentials.
     *
     * <p>"Trust it as much as you trust the worker" is the wrong frame: the asker trusts it
     * <em>more</em>, because it authorizes with credentials the worker does not hold.
     */
    @Nested
    final class IntrospectionIsLockedDown {

        /** The happy path, for the reverse proxy the method exists for. */
        @Test
        void resolvesForAnAllowlistedCaller() {
            TokenIdentity got = resolving().introspect_token("good", ctx(auth("proxy")));
            assertEquals("bob", got.principal());
            assertEquals("ci-key", got.token_name());
        }

        /**
         * Authentication is not the same capability as introspection.
         *
         * <p>A deployment where any valid credential may introspect lets any user test guesses of
         * any other user's credential at unlimited rate, and resolve a stolen one to its owner.
         */
        @Test
        void aCallerOffTheAllowlistIsRefused() {
            for (String caller : new String[] {"alice", "", null}) {
                assertThrows(IntrospectionRefusedError.class,
                        () -> resolving().introspect_token("good", ctx(auth(caller))),
                        "caller=" + caller);
            }
        }

        /** Subprocess and unix transports carry no authenticated principal. */
        @Test
        void anUnauthenticatedCallerIsRefused() {
            assertThrows(IntrospectionRefusedError.class,
                    () -> resolving().introspect_token("good", ctx(auth("proxy", false, null))));
        }

        /** An unauthorized caller learns nothing, including how long it took. */
        @Test
        void refusalPrecedesTheResolver() {
            List<String> seen = new ArrayList<>();
            IdentityImpl impl = IdentityImpl.builder()
                    .resolveToken(token -> { seen.add(token); return null; })
                    .introspectPrincipals("proxy")
                    .build();
            assertThrows(IntrospectionRefusedError.class,
                    () -> impl.introspect_token("secret", ctx(auth("mallory"))));
            assertTrue(seen.isEmpty(),
                    "the resolver must not see a credential from an unauthorized caller");
        }

        /**
         * Authorization runs BEFORE the subject credential is even measured.
         *
         * <p>This is the guard order the spec pins and it is load-bearing: a caller who is not an
         * introspector must get {@code introspection_refused} for an over-long or JWS-shaped
         * token, never {@code token_unresolved}. The latter would confirm that the shape check
         * ran, which is a timing and behaviour oracle on a credential the caller may not ask
         * about at all. Do not reorder these for tidiness.
         */
        @Test
        void authorizationPrecedesEveryLookAtTheSubject() {
            String oversize = "x".repeat(Identity.MAX_TOKEN_CHARS + 1);
            for (String token : new String[] {"", oversize, "aaa.bbb.ccc"}) {
                HasErrorKind err = assertThrows(IntrospectionRefusedError.class,
                        () -> resolving().introspect_token(token, ctx(auth("mallory"))),
                        "token shape must not be examined before the caller is authorized");
                assertEquals(IntrospectionRefusedError.ERROR_KIND, err.errorKind());
            }
        }

        /** The rate limit is also ahead of the subject: same reason, same order. */
        @Test
        void theRateLimitPrecedesEveryLookAtTheSubject() {
            IdentityImpl impl = IdentityImpl.builder()
                    .resolveToken(RESOLVER).introspectPrincipals("proxy")
                    .introspectRateLimit(1).build();
            assertEquals("bob", impl.introspect_token("good", ctx(auth("proxy"))).principal());
            IntrospectionRefusedError err = assertThrows(IntrospectionRefusedError.class,
                    () -> impl.introspect_token("aaa.bbb.ccc", ctx(auth("proxy"))));
            assertTrue(err.getMessage().contains("rate limit"), err.getMessage());
        }

        /**
         * Unknown, malformed and over-long are one answer.
         *
         * <p>Distinguishing them would confirm that a guessed credential exists.
         */
        @Test
        void rejectionsAreUniform() {
            String oversize = "x".repeat(Identity.MAX_TOKEN_CHARS + 1);
            for (String token : new String[] {"", "unknown", oversize}) {
                TokenUnresolvedError err = assertThrows(TokenUnresolvedError.class,
                        () -> resolving().introspect_token(token, ctx(auth("proxy"))));
                assertEquals("unresolved", err.getMessage(),
                        "every rejection must read identically");
                assertEquals(TokenUnresolvedError.ERROR_KIND, err.errorKind());
            }
        }

        /**
         * Routing a JWS onward hands a third party a token the asker may have rejected.
         *
         * <p>A JWS is validated locally against a key set. Forwarding one the asker already
         * refused -- expired, wrong audience -- to something that might accept it turns this
         * method into a laundering step.
         */
        @Test
        void aJwsNeverReachesTheResolver() {
            List<String> seen = new ArrayList<>();
            IdentityImpl impl = IdentityImpl.builder()
                    .resolveToken(token -> { seen.add(token); return new TokenIdentity("bob"); })
                    .introspectPrincipals("proxy")
                    .build();
            assertThrows(TokenUnresolvedError.class,
                    () -> impl.introspect_token("aaa.bbb.ccc", ctx(auth("proxy"))));
            assertTrue(seen.isEmpty());
        }

        /**
         * A caller that negative-caches "unknown" must not cache this.
         *
         * <p>Cache an outage and a worker restart takes the fleet down for the cache's lifetime;
         * retry a rejection and the worker is hammered.
         */
        @Test
        void unavailableIsTransientNotDefinitive() {
            IdentityImpl impl = IdentityImpl.builder()
                    .resolveToken(token -> { throw new IdentityUnavailableError("store is down"); })
                    .introspectPrincipals("proxy")
                    .build();
            IdentityUnavailableError err = assertThrows(IdentityUnavailableError.class,
                    () -> impl.introspect_token("good", ctx(auth("proxy"))));
            assertTrue(err.retryAfterSeconds() > 0);
        }

        /** Bounds, rather than closes, the oracle an allowlisted caller still has. */
        @Test
        void rateLimited() {
            IdentityImpl impl = IdentityImpl.builder()
                    .resolveToken(RESOLVER).introspectPrincipals("proxy")
                    .introspectRateLimit(2).build();
            CallContext c = ctx(auth("proxy"));
            assertEquals("bob", impl.introspect_token("good", c).principal());
            assertEquals("bob", impl.introspect_token("good", c).principal());
            IntrospectionRefusedError err = assertThrows(IntrospectionRefusedError.class,
                    () -> impl.introspect_token("good", c));
            assertTrue(err.getMessage().contains("rate limit"), err.getMessage());
        }

        /** There is no permissive default, so it cannot be reached by omission. */
        @Test
        void anAllowlistIsMandatory() {
            IllegalArgumentException missing = assertThrows(IllegalArgumentException.class,
                    () -> IdentityImpl.builder().resolveToken(RESOLVER).build());
            assertTrue(missing.getMessage().contains("at least one principal"), missing.getMessage());

            IllegalArgumentException empty = assertThrows(IllegalArgumentException.class,
                    () -> IdentityImpl.builder().resolveToken(RESOLVER)
                            .introspectPrincipals(List.of()).build());
            assertTrue(empty.getMessage().contains("at least one principal"), empty.getMessage());
        }

        /**
         * Validated at construction, not at first call.
         *
         * <p>A worker that would refuse every introspection should fail to start rather than
         * serve traffic until someone tries.
         */
        @Test
        void theAllowlistIsValidatedBeforeAnyTrafficIsServed() {
            assertThrows(IllegalArgumentException.class,
                    () -> IdentityImpl.builder().resolveToken(RESOLVER)
                            .introspectPrincipals("", null).build(),
                    "blank entries are dropped, and dropping them all is an empty allowlist");
        }
    }

    // --- issuance ----------------------------------------------------------

    /** Issuance is always about the caller, so it needs neither allowlist nor limit. */
    @Nested
    final class IssuanceIsNotAnOracle {

        /** The happy path: a present user minting their own standing grant. */
        @Test
        void mintsForTheCaller() {
            IdentityImpl impl = IdentityImpl.builder().mintGrant(MINTER).build();
            IssuedGrant grant = impl.issue_grant(
                    "reports", List.of("read"), 3600, ctx(auth("alice", true, now())));
            assertEquals("grant-for-alice", grant.token());
            assertTrue(grant.expires_at() > now());
        }

        /**
         * Cross-subject minting is closed by construction, not by a check.
         *
         * <p>A check is something one of seven ports can forget; a missing parameter is not.
         */
        @Test
        void theSubjectIsTheCallerAndIsNotAParameter() {
            Method m = null;
            for (Method candidate : Identity.class.getMethods()) {
                if ("issue_grant".equals(candidate.getName())) m = candidate;
            }
            assertTrue(m != null, "issue_grant must exist");
            List<String> names = new ArrayList<>();
            for (Parameter p : m.getParameters()) names.add(p.getName());
            assertFalse(names.contains("subject"), names.toString());
            assertFalse(names.contains("principal"), names.toString());
        }

        /** Unlike introspection -- and the asymmetry is the whole design. */
        @Test
        void needsNoAllowlist() {
            assertEquals(Set.of("issue_grant"),
                    IdentityImpl.builder().mintGrant(MINTER).build().offeredMethods());
        }
    }

    // --- freshness ---------------------------------------------------------

    /** A credential with no verifiable auth_time cannot mint. */
    @Nested
    final class Freshness {

        private IdentityImpl impl() {
            return IdentityImpl.builder().mintGrant(MINTER).maxAuthAge(900.0).build();
        }

        /** A static bearer proves a machine holds a secret, never that a human just logged in. */
        @Test
        void absentAuthTimeIsRefused() {
            StaleAuthError err = assertThrows(StaleAuthError.class,
                    () -> impl().issue_grant("p", List.of(), 60, ctx(auth("alice"))));
            assertTrue(err.getMessage().contains("no auth_time"), err.getMessage());
        }

        /**
         * Naming the reason leaks nothing here: it is always about the caller.
         *
         * <p>A console that cannot tell "your login is too old" from "no" cannot know to
         * re-prompt.
         */
        @Test
        void staleAuthTimeIsRefusedActionably() {
            StaleAuthError err = assertThrows(StaleAuthError.class,
                    () -> impl().issue_grant("p", List.of(), 60,
                            ctx(auth("alice", true, now() - 5000))));
            assertTrue(err.getMessage().contains("re-authenticate"), err.getMessage());
        }

        /** The ceiling is a ceiling, not an equality. */
        @Test
        void freshAuthTimeIsAccepted() {
            IssuedGrant grant = impl().issue_grant("p", List.of(), 60,
                    ctx(auth("alice", true, now() - 10)));
            assertEquals("grant-for-alice", grant.token());
        }

        /**
         * The lineage cannot escape the identity provider.
         *
         * <p>A grant is not an IdP-issued token, so it carries no {@code auth_time}, so
         * presenting one here fails the freshness check. That single rule is what stops
         * indefinite self-renewal.
         */
        @Test
        void aGrantCannotMintAnotherGrant() {
            // No auth_time: this is exactly what a grant bearer looks like.
            assertThrows(StaleAuthError.class,
                    () -> impl().issue_grant("p", List.of(), 60, ctx(auth("alice"))));
        }

        /** Subprocess and unix have no authenticated principal at all. */
        @Test
        void unauthenticatedTransportFailsClosed() {
            StaleAuthError err = assertThrows(StaleAuthError.class,
                    () -> impl().issue_grant("p", List.of(), 60, ctx(auth(null, false, null))));
            assertTrue(err.getMessage().contains("not authenticated"), err.getMessage());
        }

        /** A claim that is present but not a number is no evidence of anything. */
        @Test
        void anUnparseableAuthTimeIsRefused() {
            AuthContext a = new AuthContext("test", true, "alice", Map.of("auth_time", "yesterday"));
            StaleAuthError err = assertThrows(StaleAuthError.class,
                    () -> impl().issue_grant("p", List.of(), 60, ctx(a)));
            assertTrue(err.getMessage().contains("unusable auth_time"), err.getMessage());
        }

        /** An IdP that sends auth_time as a JSON string is still saying a number. */
        @Test
        void aNumericStringAuthTimeIsAccepted() {
            AuthContext a = new AuthContext("test", true, "alice",
                    Map.of("auth_time", String.valueOf((long) now() - 10)));
            assertDoesNotThrow(() -> impl().issue_grant("p", List.of(), 60, ctx(a)));
        }
    }

    // --- absent hooks ------------------------------------------------------

    /** Calling a method the deployment did not configure. */
    @Nested
    final class AbsentHooks {

        /** Refused rather than crashing, for a caller that reached it anyway. */
        @Test
        void introspectionWithoutAResolver() {
            IntrospectionRefusedError err = assertThrows(IntrospectionRefusedError.class,
                    () -> IdentityImpl.builder().mintGrant(MINTER).build()
                            .introspect_token("good", ctx(auth("proxy"))));
            assertTrue(err.getMessage().contains("does not resolve"), err.getMessage());
        }

        /** Same, on the other side. */
        @Test
        void issuanceWithoutAMinter() {
            GrantRefusedError err = assertThrows(GrantRefusedError.class,
                    () -> resolving().issue_grant("p", List.of(), 60,
                            ctx(auth("alice", true, now()))));
            assertTrue(err.getMessage().contains("does not mint"), err.getMessage());
        }

        /**
         * The absent hook is checked before the caller is, on both methods.
         *
         * <p>"This worker does not do that" is not a secret and does not depend on the request,
         * so answering it costs nothing -- whereas making it depend on the caller would turn a
         * capability question into an authorization question a preflight cannot ask.
         */
        @Test
        void theMissingHookIsRefusedRegardlessOfTheCaller() {
            assertThrows(IntrospectionRefusedError.class,
                    () -> IdentityImpl.builder().mintGrant(MINTER).build()
                            .introspect_token("good", ctx(auth(null, false, null))));
            assertThrows(GrantRefusedError.class,
                    () -> resolving().issue_grant("p", List.of(), 60, ctx(auth(null, false, null))));
        }
    }

    // --- diagnostics -------------------------------------------------------

    /** The credential must never reach a log, a span, or an error message. */
    @Nested
    final class Diagnostics {

        /** Stable enough to correlate one credential's failures; not the credential. */
        @Test
        void theDigestIsNotTheToken() {
            assertNotEquals("secret", IdentityImpl.tokenDigest("secret"));
            assertEquals(IdentityImpl.tokenDigest("secret"), IdentityImpl.tokenDigest("secret"));
            assertNotEquals(IdentityImpl.tokenDigest("secret"), IdentityImpl.tokenDigest("other"));
            assertEquals(64, IdentityImpl.tokenDigest("secret").length());
        }

        /**
         * The error kinds are the only definitive/transient signal a caller has.
         *
         * <p>These were an HTTP route whose callers classified on the status code (404 vs 503).
         * As protocol methods every handler exception surfaces the same way, so
         * {@code error_kind} carries the whole distinction.
         */
        @Test
        void errorKindsAreStable() {
            assertEquals("introspection_refused", new IntrospectionRefusedError("x").errorKind());
            assertEquals("token_unresolved", new TokenUnresolvedError("x").errorKind());
            assertEquals("stale_auth", new StaleAuthError("x").errorKind());
            assertEquals("grant_refused", new GrantRefusedError("x").errorKind());
            assertEquals("identity_unavailable", new IdentityUnavailableError("x").errorKind());
        }

        /**
         * A transient failure must not be catchable as a definitive one.
         *
         * <p>This port's authenticator chain advances to the next member on a rejection, so an
         * outage raised as a rejection reads as "not my credential, try the next" and arrives at
         * the client as a 401 from the end of the chain -- restarting every session in the fleet
         * over a thirty-second blip. {@code AuthUnavailableException} stays outside that
         * hierarchy for the same reason, and this must too.
         */
        @Test
        void unavailableIsOutsideTheRejectionHierarchies() {
            assertFalse(IllegalArgumentException.class.isAssignableFrom(IdentityUnavailableError.class),
                    "identity_unavailable must not be catchable as an invalid argument");
            assertFalse(SecurityException.class.isAssignableFrom(IdentityUnavailableError.class),
                    "identity_unavailable is not a permission decision");
            assertFalse(AuthException.class.isAssignableFrom(IdentityUnavailableError.class),
                    "identity_unavailable must stay outside the authentication-rejection hierarchy");
            assertTrue(IllegalArgumentException.class.isAssignableFrom(TokenUnresolvedError.class),
                    "an unresolved credential IS a definitive, invalid-argument-shaped answer");
        }

        /** No guard's message may carry the subject credential. */
        @Test
        void noRefusalRepeatsTheCredential() {
            String secret = "super-secret-credential";
            List<String> messages = new ArrayList<>();
            messages.add(assertThrows(IntrospectionRefusedError.class,
                    () -> resolving().introspect_token(secret, ctx(auth("mallory")))).getMessage());
            messages.add(assertThrows(TokenUnresolvedError.class,
                    () -> resolving().introspect_token(secret, ctx(auth("proxy")))).getMessage());
            for (String m : messages) {
                assertFalse(m.contains(secret), m);
            }
        }
    }

    // --- the JWS shape test ------------------------------------------------

    /**
     * Whitespace must not be a way to walk a JWS past the guard.
     *
     * <p>The shape test runs against the trimmed credential while the resolver still receives
     * what the caller sent, so trimming can only add refusals.
     *
     * <p>This exists because the ports diverged here and the reference was the accident:
     * Python's {@code $} matches before a single trailing newline, so {@code "a.b.c\n"} was
     * refused there -- while Go's {@code \A..\z}, JavaScript's unflagged {@code $} and this
     * port's {@code matches()} routed it straight to the resolver, the one outcome the guard
     * exists to prevent. Python was not even self-consistent about it, refusing one trailing
     * newline and admitting two. Trimming first is the rule that means the same thing in seven
     * regex dialects, because it depends on none of them.
     */
    @Nested
    final class JwsShapeTestSurvivesTranslation {

        /** No amount of surrounding whitespace makes a JWS resolvable. */
        @Test
        void paddingDoesNotSmuggleAJwsPastTheGuard() {
            String[] padded = {
                "aaa.bbb.ccc",
                "aaa.bbb.ccc\n",
                "aaa.bbb.ccc\n\n",
                "  aaa.bbb.ccc  ",
                "\taaa.bbb.ccc\r\n",
            };
            for (String token : padded) {
                assertThrows(TokenUnresolvedError.class,
                        () -> IdentityImpl.rejectJwsShaped(token),
                        () -> "must be refused: " + escape(token));
            }
        }

        /**
         * Including the non-breaking spaces {@link String#strip()} would have left in place.
         *
         * <p>The reference's {@code str.strip()} removes these, so leaving them would be the same
         * divergence one layer down: refused by Python, routed onward here.
         */
        @Test
        void nonBreakingPaddingDoesNotSmuggleAJwsEither() {
            String[] padded = {"\u00A0aaa.bbb.ccc\u00A0", "\u2007aaa.bbb.ccc", "aaa.bbb.ccc\u202F"};
            for (String token : padded) {
                assertThrows(TokenUnresolvedError.class,
                        () -> IdentityImpl.rejectJwsShaped(token),
                        () -> "must be refused: " + escape(token));
            }
        }

        /** Whitespace-only never reaches a resolver either: it is not a credential. */
        @Test
        void aBlankCredentialIsNotACredential() {
            for (String token : new String[] {"", "   ", "\n", "\t\r\n", "\u00A0"}) {
                assertThrows(TokenUnresolvedError.class,
                        () -> IdentityImpl.rejectJwsShaped(token),
                        () -> "must be refused: " + escape(token));
            }
        }

        /** Trimming tightens the JWS test; it must not refuse ordinary tokens. */
        @Test
        void anOpaqueCredentialStillReachesTheResolver() {
            for (String token
                    : new String[] {"opaque-token", "a.b.c.d", "two.segments", "sk_live_abc123"}) {
                assertDoesNotThrow(() -> IdentityImpl.rejectJwsShaped(token),
                        () -> "must be routed onward: " + escape(token));
            }
        }

        /**
         * Trimming is for the shape test only -- never for what is resolved.
         *
         * <p>Rewriting a credential before resolving it would make the worker answer about a
         * string the caller never sent.
         */
        @Test
        void theResolverReceivesTheCredentialUnmodified() {
            List<String> seen = new ArrayList<>();
            IdentityImpl impl = IdentityImpl.builder()
                    .resolveToken(token -> { seen.add(token); return new TokenIdentity("p"); })
                    .introspectPrincipals("proxy")
                    .build();
            impl.introspect_token("  padded-opaque-token  ", ctx(auth("proxy")));
            assertEquals(List.of("  padded-opaque-token  "), seen);
        }

        /**
         * The length cap is measured against the original, not the trimmed form.
         *
         * <p>The megabytes a caller sent are the megabytes the framework had to hold, so padding
         * is not a way to buy a larger credential.
         */
        @Test
        void theLengthCapAppliesToWhatWasActuallySent() {
            String padded = " ".repeat(Identity.MAX_TOKEN_CHARS) + "short-token";
            assertThrows(TokenUnresolvedError.class,
                    () -> IdentityImpl.rejectJwsShaped(padded));
        }

        private String escape(String s) {
            return "\"" + s.replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t") + "\"";
        }
    }

    // --- the limiter -------------------------------------------------------

    /** Fixed-window, because the state is one integer rather than an aged float. */
    @Nested
    final class Limiter {

        /** Within a window. */
        @Test
        void admitsUpToTheLimit() {
            RateLimiter limiter = new RateLimiter(3);
            assertEquals(List.of(true, true, true, false),
                    List.of(limiter.allow("a", 100.0), limiter.allow("a", 100.0),
                            limiter.allow("a", 100.0), limiter.allow("a", 100.0)));
        }

        /** A new window resets the count. */
        @Test
        void windowRolls() {
            RateLimiter limiter = new RateLimiter(1);
            assertTrue(limiter.allow("a", 100.0));
            assertFalse(limiter.allow("a", 100.5));
            assertTrue(limiter.allow("a", 101.5));
        }

        /** One caller exhausting its budget must not refuse another. */
        @Test
        void callersAreIndependent() {
            RateLimiter limiter = new RateLimiter(1);
            assertTrue(limiter.allow("a", 100.0));
            assertTrue(limiter.allow("b", 100.0));
            assertFalse(limiter.allow("a", 100.0));
        }

        /**
         * Whole-map reset rather than per-key ageing, so an attacker cannot grow the map.
         *
         * <p>Per-key ageing would let a caller cycling keys grow the map without bound between
         * sweeps.
         */
        @Test
        void cyclingKeysCannotGrowTheMap() {
            RateLimiter limiter = new RateLimiter(1);
            for (int i = 0; i < 1000; i++) limiter.allow("k" + i, 100.0);
            limiter.allow("fresh", 200.0);
            assertEquals(1, limiter.trackedKeys());
        }

        /**
         * A limiter that can be beaten by racing it is not a limiter.
         *
         * <p>A worker serves concurrent requests from a pool, so two threads reading the same
         * count and both admitting is not a hypothetical.
         */
        @Test
        void isSafeUnderConcurrency() throws Exception {
            int threads = 8;
            int perThread = 250;
            RateLimiter limiter = new RateLimiter(threads * perThread * 2);
            List<Thread> workers = new ArrayList<>();
            java.util.concurrent.atomic.AtomicInteger admitted =
                    new java.util.concurrent.atomic.AtomicInteger();
            for (int t = 0; t < threads; t++) {
                Thread th = new Thread(() -> {
                    for (int i = 0; i < perThread; i++) {
                        if (limiter.allow("shared", 100.0)) admitted.incrementAndGet();
                    }
                });
                workers.add(th);
                th.start();
            }
            for (Thread th : workers) th.join();
            // Every request fits under the ceiling, so a correct limiter admits
            // all of them -- and a lost update would show up as a short count.
            assertEquals(threads * perThread, admitted.get());
            assertEquals(1, limiter.trackedKeys());
        }
    }
}
