// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.tailnet;

import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.TransportKind;
import farm.query.vgirpc.identity.IdentityAssurance;
import farm.query.vgirpc.identity.PeerAuthenticationPolicies;
import farm.query.vgirpc.identity.PeerEvidenceSet;
import farm.query.vgirpc.identity.PeerIdentity;
import farm.query.vgirpc.identity.PeerIdentityResult;
import farm.query.vgirpc.identity.PeerSubjectKind;
import farm.query.vgirpc.identity.SubjectStability;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class MainTest {
    @Test
    void parsesRequiredProxyV2QualificationFlags() {
        Main.Args args = Main.Args.parse(new String[] {
            "--proxy-protocol-v2", "--trusted-proxy-address", "127.0.0.1"
        });
        assertTrue(args.flag("--proxy-protocol-v2"));
        assertEquals("127.0.0.1", args.required("--trusted-proxy-address"));
    }

    private static Main.Expectation tcpExpectation() {
        return new Main.Expectation(
                "tailnet:test", "localapi", IdentityAssurance.LOCAL_DAEMON,
                PeerSubjectKind.TAGGED_NODE, SubjectStability.STABLE,
                "query.farm/cap", "destination_ip", null, "tag:vgi-client",
                true, false, null);
    }

    private static String tcpSnapshot() {
        return """
                {
                  "provider_status":{"tailscale":"available"},
                  "identities":[{
                    "provider":"tailscale","issuer":"tailnet:test",
                    "evidence_source":"localapi","assurance":"local_daemon",
                    "subject_kind":"tagged_node","subject_stability":"stable",
                    "subject_verified":true,"subject_fingerprint":"%s",
                    "tags":["tag:vgi-client"],"capability_names":["query.farm/cap"],
                    "capabilities_verified":true,
                    "capability_target":{"kind":"destination_ip"},"proxy_present":false
                  }],
                  "auth":{"authenticated":true,"domain":"tailscale",
                    "principal_fingerprint":"%s","principal_matches_identity":true,
                    "peer_evidence_binding_present":true}
                }
                """.formatted("a".repeat(64), "b".repeat(64));
    }

    @Test
    void snapshotRequiresIssuerDestinationAndBoundCanonicalAuth() {
        String valid = tcpSnapshot();
        assertDoesNotThrow(() -> Main.validateSnapshot(valid, tcpExpectation()));
        assertThrows(SecurityException.class, () -> Main.validateSnapshot(
                valid.replace("tailnet:test", "tailnet:other"), tcpExpectation()));
        assertThrows(SecurityException.class, () -> Main.validateSnapshot(
                valid.replace("\"domain\":\"tailscale\"", "\"domain\":\"bearer\""), tcpExpectation()));
        assertThrows(SecurityException.class, () -> Main.validateSnapshot(
                valid.replace("\"principal_matches_identity\":true",
                        "\"principal_matches_identity\":false"), tcpExpectation()));
        assertThrows(SecurityException.class, () -> Main.validateSnapshot(
                valid.replace("\"kind\":\"destination_ip\"", "\"kind\":\"node\""), tcpExpectation()));
    }

    @Test
    void snapshotRejectsServeIdentityDerivedFromSpoofedLogin() {
        Main.Expectation expected = new Main.Expectation(
                "tailnet:test", "serve_proxy", IdentityAssurance.CONFIGURED_PROXY,
                PeerSubjectKind.USER, SubjectStability.LOGIN, "query.farm/cap",
                null, null, null, false, true, "c".repeat(64));
        String snapshot = """
                {"provider_status":{"tailscale":"available"},"identities":[{
                  "provider":"tailscale","issuer":"tailnet:test","evidence_source":"serve_proxy",
                  "assurance":"configured_proxy","subject_kind":"user","subject_stability":"login",
                  "subject_verified":true,"subject_fingerprint":"%s",
                  "capability_names":["query.farm/cap"],"capabilities_verified":true,
                  "capability_target":null,"proxy_present":true}],
                 "auth":{"authenticated":false,"domain":null,"principal_fingerprint":null,
                  "principal_matches_identity":false,"peer_evidence_binding_present":true}}
                """.formatted("c".repeat(64));
        assertThrows(SecurityException.class, () -> Main.validateSnapshot(snapshot, expected));
    }

    @Test
    void serverRequiresExactPolicyDerivedPrincipalClaimsAndBinding() {
        PeerIdentity identity = new PeerIdentity(
                "tailscale", "localapi", IdentityAssurance.LOCAL_DAEMON,
                "tailnet:test", "tcp", PeerSubjectKind.TAGGED_NODE, "node:stable-id",
                SubjectStability.STABLE, true,
                Map.of("tags", List.of("tag:vgi-client"), "capability_target",
                        Map.of("kind", "destination_ip", "value", "100.64.0.9")),
                Map.of("query.farm/cap", List.of()), true, "100.64.0.10", null);
        PeerEvidenceSet evidence = new PeerEvidenceSet(List.of(PeerIdentityResult.available(identity)));
        AuthContext auth = PeerAuthenticationPolicies.primary("tailscale")
                .evaluate(evidence, AuthContext.ANONYMOUS);
        CallContext valid = context(auth, evidence);
        assertDoesNotThrow(() -> Main.validateContext(valid, tcpExpectation()));

        AuthContext wrong = new AuthContext("bearer", true, auth.principal(), auth.claims());
        assertThrows(SecurityException.class,
                () -> Main.validateContext(context(wrong, evidence), tcpExpectation()));
    }

    private static CallContext context(AuthContext auth, PeerEvidenceSet evidence) {
        return new CallContext(auth, ignored -> {}, Map.of(), "server", "echo_string",
                "ConformanceService", "request", TransportKind.TCP, evidence);
    }
}
