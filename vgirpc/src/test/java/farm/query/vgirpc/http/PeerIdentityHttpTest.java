// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0
package farm.query.vgirpc.http;

import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.identity.IdentityAssurance;
import farm.query.vgirpc.identity.PeerAuthenticationPolicies;
import farm.query.vgirpc.identity.PeerIdentity;
import farm.query.vgirpc.identity.PeerIdentityProvider;
import farm.query.vgirpc.identity.PeerIdentityResult;
import farm.query.vgirpc.identity.PeerResolutionContext;
import farm.query.vgirpc.identity.PeerSubjectKind;
import farm.query.vgirpc.identity.SubjectStability;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

final class PeerIdentityHttpTest {
    public interface IdentityService { String who(CallContext context); }
    public static final class IdentityImpl implements IdentityService {
        @Override public String who(CallContext context) {
            if (context.peerEvidence().eligibleSubjects("spiffe").isEmpty()) {
                return context.auth().domain() + ":none";
            }
            return context.auth().domain() + ":"
                    + context.peerEvidence().uniqueVerifiedSubject("spiffe").subjectKey();
        }
    }

    private HttpServer server;
    private HttpRpcConnection connection;

    @AfterEach
    void stop() throws Exception {
        if (connection != null) connection.close();
        if (server != null) server.stop();
    }

    private static PeerIdentity identity() {
        return new PeerIdentity("spiffe", "mtls", IdentityAssurance.CRYPTOGRAPHIC_PEER,
                "spiffe://example.org", "http", PeerSubjectKind.WORKLOAD,
                "spiffe://example.org/workload", SubjectStability.STABLE, true,
                Map.of(), Map.of(), false, null, null);
    }

    private void start(HttpServer.Config config) throws Exception {
        server = new HttpServer(new RpcServer(IdentityService.class, new IdentityImpl()), config);
        server.start();
    }

    private String endpoint() { return "http://127.0.0.1:" + server.port(); }

    @Test
    void peerPrimaryExposesEvidenceToWorkerAndKeepsAuthoritySeparateFromDestination() throws Exception {
        AtomicReference<PeerResolutionContext> seen = new AtomicReference<>();
        PeerIdentityProvider provider = new PeerIdentityProvider() {
            @Override public String provider() { return "spiffe"; }
            @Override public PeerIdentityResult resolve(PeerResolutionContext context) {
                seen.set(context);
                return PeerIdentityResult.available(identity());
            }
        };
        start(HttpServer.Config.builder().prefix("/vgi")
                .peerServiceName("svc:vgi-worker")
                .peerIdentityProviders(List.of(provider))
                .peerAuthenticationPolicy(PeerAuthenticationPolicies.primary("spiffe"))
                .build());
        connection = HttpRpcConnection.builder(endpoint() + "/vgi").build();

        assertEquals("spiffe:spiffe://example.org/workload", connection.proxy(IdentityService.class).who(null));
        assertNotNull(seen.get().authority());
        assertNotNull(seen.get().destinationAddress());
        assertEquals("127.0.0.1", seen.get().immediatePeer());
        assertTrue(seen.get().sourceEndpoint().matches("127\\.0\\.0\\.1:[0-9]+"));
        assertNull(seen.get().assertedPeer());
        assertEquals("svc:vgi-worker", seen.get().serviceName());
    }

    @Test
    void invalidApplicationCredentialNeverFallsBackToPeerPrimary() throws Exception {
        AtomicBoolean called = new AtomicBoolean();
        PeerIdentityProvider provider = new PeerIdentityProvider() {
            @Override public String provider() { return "spiffe"; }
            @Override public PeerIdentityResult resolve(PeerResolutionContext context) {
                called.set(true);
                return PeerIdentityResult.available(identity());
            }
        };
        start(HttpServer.Config.builder().prefix("/vgi")
                .authenticator(request -> { throw new InvalidCredentials("bad token"); })
                .peerIdentityProviders(List.of(provider))
                .peerAuthenticationPolicy(PeerAuthenticationPolicies.primary("spiffe"))
                .build());
        HttpResponse<String> response = rawPost();
        assertEquals(401, response.statusCode());
        assertEquals("invalid_credential", response.headers().firstValue(HttpHeaders.VGI_AUTH_REASON).orElseThrow());
        assertFalse(called.get());
    }

    @Test
    void observationDoesNotEraseMissingApplicationCredentials() throws Exception {
        start(HttpServer.Config.builder().prefix("/vgi")
                .authenticator(request -> { throw new MissingCredentials("bearer required"); })
                .peerIdentityProviders(List.of(provider(context -> PeerIdentityResult.available(identity()))))
                .peerAuthenticationPolicy(PeerAuthenticationPolicies::observe)
                .build());
        assertEquals(401, rawPost().statusCode());
    }

    @Test
    void providerConcurrencyMustFitOneWholeResolutionFanout() {
        PeerIdentityProvider first = namedProvider("first");
        PeerIdentityProvider second = namedProvider("second");
        assertThrows(IllegalArgumentException.class, () -> HttpServer.Config.builder()
                .peerIdentityProviders(List.of(first, second))
                .peerProviderConcurrency(1)
                .build());
    }

    @Test
    void providerTimeoutIsRetryable503() throws Exception {
        start(HttpServer.Config.builder().prefix("/vgi")
                .peerResolutionTimeoutMs(5)
                .peerIdentityProviders(List.of(provider(context -> {
                    try { Thread.sleep(Duration.ofSeconds(10)); }
                    catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    return PeerIdentityResult.available(identity());
                })))
                .peerAuthenticationPolicy(PeerAuthenticationPolicies.primary("spiffe"))
                .build());
        HttpResponse<String> response = rawPost();
        assertEquals(503, response.statusCode());
        assertEquals("5", response.headers().firstValue("Retry-After").orElseThrow());
    }

    @Test
    void timedOutProviderRetainsItsBoundedSlotUntilItActuallyExits() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        PeerIdentityProvider provider = new PeerIdentityProvider() {
            @Override public String provider() { return "spiffe"; }
            @Override public PeerIdentityResult resolve(PeerResolutionContext context) {
                calls.incrementAndGet();
                boolean done = false;
                while (!done) {
                    try {
                        release.await();
                        done = true;
                    } catch (InterruptedException ignored) {
                        // Model a dependency that does not honor cancellation. The
                        // executor must retain this slot instead of spawning an
                        // unbounded replacement for each timed-out request.
                    }
                }
                return PeerIdentityResult.available(identity());
            }
        };
        start(HttpServer.Config.builder().prefix("/vgi")
                .peerResolutionTimeoutMs(10)
                .peerProviderConcurrency(1)
                .peerIdentityProviders(List.of(provider))
                .peerAuthenticationPolicy(PeerAuthenticationPolicies.primary("spiffe"))
                .build());
        try {
            assertEquals(503, rawPost().statusCode());
            assertEquals(503, rawPost().statusCode());
            assertEquals(1, calls.get());
        } finally {
            release.countDown();
        }
    }

    @Test
    void unavailableProviderIsPolicyInputForValidApplicationAnyOf() throws Exception {
        start(HttpServer.Config.builder().prefix("/vgi")
                .authenticator(request -> new AuthContext("bearer", true, "alice", Map.of()))
                .peerResolutionTimeoutMs(5)
                .peerIdentityProviders(List.of(provider(context -> {
                    try { Thread.sleep(Duration.ofSeconds(10)); }
                    catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    return PeerIdentityResult.available(identity());
                })))
                .peerAuthenticationPolicy(PeerAuthenticationPolicies.anyOf("spiffe"))
                .build());
        connection = HttpRpcConnection.builder(endpoint() + "/vgi").build();
        assertEquals("bearer:none", connection.proxy(IdentityService.class).who(null));
    }

    @Test
    void completedInvalidEvidenceIsNotDowngradedBehindSlowProvider() throws Exception {
        PeerIdentityProvider slow = new PeerIdentityProvider() {
            @Override public String provider() { return "slow"; }
            @Override public PeerIdentityResult resolve(PeerResolutionContext context) {
                try { Thread.sleep(Duration.ofSeconds(10)); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                return new PeerIdentityResult("slow", farm.query.vgirpc.identity.PeerIdentityStatus.NO_MATCH);
            }
        };
        PeerIdentityProvider invalid = new PeerIdentityProvider() {
            @Override public String provider() { return "invalid"; }
            @Override public PeerIdentityResult resolve(PeerResolutionContext context) {
                return new PeerIdentityResult("invalid", farm.query.vgirpc.identity.PeerIdentityStatus.INVALID);
            }
        };
        start(HttpServer.Config.builder().prefix("/vgi")
                .authenticator(request -> new AuthContext("bearer", true, "alice", Map.of()))
                .peerResolutionTimeoutMs(20)
                .peerIdentityProviders(List.of(slow, invalid))
                .peerAuthenticationPolicy(PeerAuthenticationPolicies.anyOf("slow", "invalid"))
                .build());
        assertEquals(401, rawPost().statusCode());
    }

    @Test
    void customPolicyDetailsAreRedactedFromResponses() throws Exception {
        String secret = "raw-capability-policy-secret";
        start(HttpServer.Config.builder().prefix("/vgi")
                .peerAuthenticationPolicy((evidence, auth) -> {
                    throw new farm.query.vgirpc.identity.PeerIdentityRejectedException(secret);
                })
                .build());
        HttpResponse<String> rejected = rawPost();
        assertEquals(401, rejected.statusCode());
        assertFalse(rejected.body().contains(secret));
        stop();
        connection = null;
        server = null;

        start(HttpServer.Config.builder().prefix("/vgi")
                .peerAuthenticationPolicy((evidence, auth) -> {
                    throw new farm.query.vgirpc.identity.PeerIdentityUnavailableException(secret, 17);
                })
                .build());
        HttpResponse<String> unavailable = rawPost();
        assertEquals(503, unavailable.statusCode());
        assertEquals("17", unavailable.headers().firstValue("Retry-After").orElseThrow());
        assertFalse(unavailable.body().contains(secret));
        stop();
        connection = null;
        server = null;

        start(HttpServer.Config.builder().prefix("/vgi")
                .peerAuthenticationPolicy((evidence, auth) -> {
                    throw new IllegalStateException(secret);
                })
                .build());
        HttpResponse<String> failed = rawPost();
        assertEquals(500, failed.statusCode());
        assertFalse(failed.body().contains(secret));
    }

    private HttpResponse<String> rawPost() throws Exception {
        try (HttpClient client = HttpClient.newHttpClient()) {
            return client.send(HttpRequest.newBuilder(URI.create(endpoint() + "/vgi/who"))
                    .timeout(Duration.ofSeconds(5)).POST(HttpRequest.BodyPublishers.noBody()).build(),
                    HttpResponse.BodyHandlers.ofString());
        }
    }

    private static PeerIdentityProvider provider(
            java.util.function.Function<PeerResolutionContext, PeerIdentityResult> resolver) {
        return new PeerIdentityProvider() {
            @Override public String provider() { return "spiffe"; }
            @Override public PeerIdentityResult resolve(PeerResolutionContext context) { return resolver.apply(context); }
        };
    }

    private static PeerIdentityProvider namedProvider(String name) {
        return new PeerIdentityProvider() {
            @Override public String provider() { return name; }
            @Override public PeerIdentityResult resolve(PeerResolutionContext context) {
                return new PeerIdentityResult(name, farm.query.vgirpc.identity.PeerIdentityStatus.NO_MATCH);
            }
        };
    }
}
