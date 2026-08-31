// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0
package farm.query.vgirpc.identity;

import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.CallContext;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

final class PeerIdentityTest {
    private static PeerIdentity identity(String provider, String subject) {
        return new PeerIdentity(provider, "test", IdentityAssurance.CRYPTOGRAPHIC_PEER,
                "spiffe://example.org", "tcp", PeerSubjectKind.WORKLOAD, subject,
                SubjectStability.STABLE, true, Map.of(), Map.of(), false, null, null);
    }

    @Test
    void matchesSharedPrincipalAndBindingVector() {
        PeerIdentity identity = identity("spiffe", "spiffe://example.org/workload");
        PeerEvidenceSet evidence = new PeerEvidenceSet(List.of(PeerIdentityResult.available(identity)));
        assertEquals("peer/spiffe/spiffe%3A%2F%2Fexample.org/spiffe%3A%2F%2Fexample.org%2Fworkload",
                identity.canonicalPrincipal());
        assertEquals("948ce118ddd5f212e7bfd62e13ffdba0675397c56a43060e98656965389e5367",
                evidence.bindingDigest(List.of("spiffe")));
    }

    @Test
    void bindingIgnoresRoutingAddressesButIncludesCapabilities() {
        PeerIdentity first = new PeerIdentity("spiffe", "test", IdentityAssurance.CRYPTOGRAPHIC_PEER,
                "spiffe://example.org", "tcp", PeerSubjectKind.WORKLOAD,
                "spiffe://example.org/workload", SubjectStability.STABLE, true,
                Map.of(), Map.of("run", List.of(Map.of("queue", "a"))), true,
                "100.64.0.1", "10.0.0.1");
        PeerIdentity moved = new PeerIdentity("spiffe", "test", IdentityAssurance.CRYPTOGRAPHIC_PEER,
                "spiffe://example.org", "tcp", PeerSubjectKind.WORKLOAD,
                "spiffe://example.org/workload", SubjectStability.STABLE, true,
                Map.of(), Map.of("run", List.of(Map.of("queue", "a"))), true,
                "100.64.0.99", "10.0.0.42");
        PeerIdentity changedCapability = new PeerIdentity("spiffe", "test",
                IdentityAssurance.CRYPTOGRAPHIC_PEER, "spiffe://example.org", "tcp",
                PeerSubjectKind.WORKLOAD, "spiffe://example.org/workload",
                SubjectStability.STABLE, true, Map.of(),
                Map.of("run", List.of(Map.of("queue", "b"))), true,
                "100.64.0.1", "10.0.0.1");
        String original = new PeerEvidenceSet(List.of(PeerIdentityResult.available(first)))
                .bindingDigest(List.of("spiffe"));
        assertEquals(original, new PeerEvidenceSet(List.of(PeerIdentityResult.available(moved)))
                .bindingDigest(List.of("spiffe")));
        assertNotEquals(original, new PeerEvidenceSet(List.of(PeerIdentityResult.available(changedCapability)))
                .bindingDigest(List.of("spiffe")));
    }

    @Test
    void deeplySnapshotsStructuredEvidence() {
        List<Object> roles = new ArrayList<>(List.of("reader"));
        Map<String, Object> attributes = new LinkedHashMap<>();
        attributes.put("roles", roles);
        PeerIdentity identity = new PeerIdentity("test", "test", IdentityAssurance.LOCAL_DAEMON,
                "test://issuer", "tcp", PeerSubjectKind.UNKNOWN, null, SubjectStability.NONE,
                false, attributes, Map.of(), false, null, null);
        roles.set(0, "writer");
        assertEquals(List.of("reader"), identity.attributes().get("roles"));
        assertThrows(UnsupportedOperationException.class,
                () -> ((List<Object>) identity.attributes().get("roles")).add("admin"));
    }

    @Test
    void rejectsMutableNumbersAndMalformedUtf16() {
        assertThrows(IllegalArgumentException.class, () -> new PeerIdentity("test", "test",
                IdentityAssurance.LOCAL_DAEMON, "test://issuer", "tcp", PeerSubjectKind.UNKNOWN,
                null, SubjectStability.NONE, false, Map.of("count", new AtomicInteger(1)),
                Map.of(), false, null, null));
        assertThrows(IllegalArgumentException.class, () -> identity("spiffe", "bad\ud800subject"));
        assertThrows(IllegalArgumentException.class, () -> new PeerIdentity("test", "test",
                IdentityAssurance.LOCAL_DAEMON, "test://issuer", "tcp", PeerSubjectKind.UNKNOWN,
                null, SubjectStability.NONE, false, Map.of("bad\udfffkey", true),
                Map.of(), false, null, null));
        assertThrows(IllegalArgumentException.class, () -> new PeerIdentity("test", "test",
                IdentityAssurance.LOCAL_DAEMON, "test://issuer", "tcp", PeerSubjectKind.UNKNOWN,
                null, SubjectStability.NONE, false, Map.of("oversized", "x".repeat(65_537)),
                Map.of(), false, null, null));
        assertThrows(IllegalArgumentException.class, () -> new PeerIdentity("test", "test",
                IdentityAssurance.LOCAL_DAEMON, "test://issuer", "tcp", PeerSubjectKind.UNKNOWN,
                null, SubjectStability.NONE, false, Map.of("not_finite", Double.POSITIVE_INFINITY),
                Map.of(), false, null, null));
    }

    @Test
    void rejectsJsonDepthAndValueCountLimits() {
        Object nested = "leaf";
        for (int index = 0; index < 17; index++) nested = List.of(nested);
        Object tooDeep = nested;
        assertThrows(IllegalArgumentException.class, () -> new PeerIdentity("test", "test",
                IdentityAssurance.LOCAL_DAEMON, "test://issuer", "tcp", PeerSubjectKind.UNKNOWN,
                null, SubjectStability.NONE, false, Map.of("nested", tooDeep),
                Map.of(), false, null, null));

        List<Object> tooMany = new ArrayList<>();
        for (int index = 0; index < 4_096; index++) tooMany.add(true);
        assertThrows(IllegalArgumentException.class, () -> new PeerIdentity("test", "test",
                IdentityAssurance.LOCAL_DAEMON, "test://issuer", "tcp", PeerSubjectKind.UNKNOWN,
                null, SubjectStability.NONE, false, Map.of("many", tooMany),
                Map.of(), false, null, null));
    }

    @Test
    void anyOfIsOrderedAndRejectsAmbiguityBeforeApplicationFallback() {
        PeerEvidenceSet ordered = new PeerEvidenceSet(List.of(
                new PeerIdentityResult("first", PeerIdentityStatus.UNAVAILABLE),
                PeerIdentityResult.available(identity("second", "spiffe://example.org/second"))));
        AuthContext auth = PeerAuthenticationPolicies.anyOf("first", "second")
                .evaluate(ordered, AuthContext.ANONYMOUS);
        assertEquals("second", auth.domain());

        PeerEvidenceSet ambiguous = new PeerEvidenceSet(List.of(new PeerIdentityResult(
                "spiffe", PeerIdentityStatus.AVAILABLE,
                List.of(identity("spiffe", "spiffe://example.org/one"),
                        identity("spiffe", "spiffe://example.org/two")))));
        assertThrows(PeerIdentityRejectedException.class,
                () -> PeerAuthenticationPolicies.anyOf("spiffe")
                        .evaluate(ambiguous, new AuthContext("bearer", true, "alice", Map.of())));
    }

    @Test
    void anyOfLeavesPeerEvidenceObservationOnlyWhenApplicationAuthWins() {
        PeerEvidenceSet evidence = new PeerEvidenceSet(List.of(
                PeerIdentityResult.available(identity("spiffe", "spiffe://example.org/workload"))));
        AuthContext application = new AuthContext("bearer", true, "alice", Map.of("role", "reader"));
        AuthContext result = PeerAuthenticationPolicies.anyOf("spiffe").evaluate(evidence, application);
        assertSame(application, result);
        assertFalse(result.claims().containsKey("peer_evidence_binding"));
    }

    @Test
    void allOfBindsApplicationIdentity() {
        PeerEvidenceSet evidence = new PeerEvidenceSet(List.of(
                PeerIdentityResult.available(identity("spiffe", "spiffe://example.org/workload"))));
        PeerAuthenticationPolicy policy = PeerAuthenticationPolicies.allOf(List.of("spiffe"), (auth, peers) -> {});
        AuthContext alice = policy.evaluate(evidence, new AuthContext("bearer", true, "alice", Map.of()));
        AuthContext bob = policy.evaluate(evidence, new AuthContext("bearer", true, "bob", Map.of()));
        assertNotEquals(alice.claims().get("peer_evidence_binding"), bob.claims().get("peer_evidence_binding"));
    }

    @Test
    void requireAcceptsCapabilityOnlyEvidenceButPrimaryRejectsIt() {
        PeerIdentity capabilityOnly = new PeerIdentity("tailscale", "serve", IdentityAssurance.CONFIGURED_PROXY,
                "tailnet:test", "http", PeerSubjectKind.UNKNOWN, null, SubjectStability.NONE,
                false, Map.of(), Map.of("query.farm/can-run", List.of(Map.of("worker", "analytics"))),
                true, null, null);
        PeerEvidenceSet evidence = new PeerEvidenceSet(List.of(PeerIdentityResult.available(capabilityOnly)));
        AuthContext application = new AuthContext("bearer", true, "alice", Map.of());
        AuthContext required = PeerAuthenticationPolicies.require("tailscale").evaluate(evidence, application);
        assertTrue(required.authenticated());
        assertEquals("alice", required.principal());
        assertThrows(PeerIdentityRejectedException.class,
                () -> PeerAuthenticationPolicies.primary("tailscale").evaluate(evidence, AuthContext.ANONYMOUS));
    }

    @Test
    void legacyCallContextGetsEmptyEvidenceAndNewContextSnapshotsIt() {
        CallContext legacy = new CallContext(null, ignored -> {}, Map.of(), "s", "m", "p", "r");
        assertSame(PeerEvidenceSet.EMPTY, legacy.peerEvidence());
        PeerEvidenceSet evidence = new PeerEvidenceSet(List.of(
                PeerIdentityResult.available(identity("spiffe", "spiffe://example.org/workload"))));
        CallContext current = new CallContext(null, ignored -> {}, Map.of(), "s", "m", "p", "r", null, evidence);
        assertSame(evidence, current.peerEvidence());
    }
}
