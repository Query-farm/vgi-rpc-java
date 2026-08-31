// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0
package farm.query.vgirpc.http;

import farm.query.vgirpc.identity.PeerIdentityResult;
import farm.query.vgirpc.identity.PeerIdentityStatus;
import farm.query.vgirpc.identity.PeerResolutionContext;
import farm.query.vgirpc.identity.PeerSubjectKind;
import farm.query.vgirpc.identity.SubjectStability;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class TailscalePeerIdentityProvidersTest {
    @Test void serveProducesLoginSubjectAndVerifiedOpaqueCapabilities() {
        var provider = TailscalePeerIdentityProviders.serve("tailnet:example", Set.of("127.0.0.1"));
        PeerIdentityResult result = provider.resolve(context("127.0.0.1", Map.of(
                "Tailscale-User-Login", List.of("alice@example.com"),
                "Tailscale-User-Name", List.of("=?UTF-8?Q?Alice_=E2=98=83?="),
                "Tailscale-App-Capabilities", List.of("{\"query.farm/cap\":[{\"role\":\"reader\"}]}"))));
        assertEquals(PeerIdentityStatus.AVAILABLE, result.status());
        var identity = result.identities().getFirst();
        assertEquals(PeerSubjectKind.USER, identity.subjectKind());
        assertEquals(SubjectStability.LOGIN, identity.subjectStability());
        assertEquals("login:alice@example.com", identity.subjectKey());
        assertEquals("Alice ☃", identity.attributes().get("user_display_name"));
        assertEquals(true, identity.capabilitiesVerified());
        assertNull(identity.sourceAddress());
    }

    @Test void capabilitiesSupportRfc2047QEncodingAndVerificationTracksHeaderPresence() {
        var provider = TailscalePeerIdentityProviders.serve("tailnet:example", Set.of("127.0.0.1"));
        String encoded = "=?UTF-8?Q?=7B=22query.farm/cap=22=3A=5B=7B=22role=22=3A=22reader=22=7D=5D=7D?=";
        var encodedResult = provider.resolve(context("127.0.0.1", Map.of(
                "Tailscale-App-Capabilities", List.of(encoded))));
        assertEquals(PeerIdentityStatus.AVAILABLE, encodedResult.status());
        assertEquals(Map.of("query.farm/cap", List.of(Map.of("role", "reader"))),
                encodedResult.identities().getFirst().capabilities());
        assertEquals(true, encodedResult.identities().getFirst().capabilitiesVerified());

        var loginOnly = provider.resolve(context("127.0.0.1",
                Map.of("Tailscale-User-Login", List.of("alice@example.com"))));
        assertEquals(PeerIdentityStatus.AVAILABLE, loginOnly.status());
        assertEquals(false, loginOnly.identities().getFirst().capabilitiesVerified());
        assertEquals(PeerIdentityStatus.NO_MATCH, provider.resolve(context("127.0.0.1",
                Map.of("Tailscale-App-Capabilities", List.of("{}")))).status());
    }

    @Test void capabilityOnlyEvidenceRemainsSubjectless() {
        var provider = TailscalePeerIdentityProviders.serve("tailnet:example", Set.of("127.0.0.1"));
        var result = provider.resolve(context("127.0.0.1",
                Map.of("Tailscale-App-Capabilities", List.of("{\"query.farm/cap\":[]}"))));
        assertEquals(PeerIdentityStatus.AVAILABLE, result.status());
        assertEquals(PeerSubjectKind.UNKNOWN, result.identities().getFirst().subjectKind());
        assertNull(result.identities().getFirst().subjectKey());
    }

    @Test void serveFailsClosedForTrustDuplicatesAndMalformedCapabilities() {
        var provider = TailscalePeerIdentityProviders.serve("tailnet:example", Set.of("127.0.0.1"));
        assertEquals(PeerIdentityStatus.UNTRUSTED_PROXY, provider.resolve(context("192.0.2.99",
                Map.of("Tailscale-User-Login", List.of("alice@example.com")))).status());
        assertEquals(PeerIdentityStatus.INVALID, provider.resolve(context("127.0.0.1",
                Map.of("Tailscale-User-Login", List.of("alice", "mallory")))).status());
        assertEquals(PeerIdentityStatus.NOT_APPLICABLE, provider.resolve(context("127.0.0.1", Map.of(
                "Tailscale-Funnel-Request", List.of("?1"),
                "Tailscale-User-Login", List.of("spoof@example.com")))).status());
        assertEquals(PeerIdentityStatus.INVALID, provider.resolve(context("127.0.0.1", Map.of(
                "Tailscale-Funnel-Request", List.of("true"),
                "Tailscale-User-Login", List.of("spoof@example.com")))).status());
        assertEquals(PeerIdentityStatus.INVALID, provider.resolve(context("127.0.0.1",
                Map.of("Tailscale-User-Name", List.of("Alice")))).status());
        for (String json : List.of("[]", "{\"cap\":{}}", "{\"cap\":[],\"cap\":[]}",
                "{\"cap\":[\"\ud800\"]}")) {
            assertEquals(PeerIdentityStatus.INVALID, provider.resolve(context("127.0.0.1",
                    Map.of("Tailscale-App-Capabilities", List.of(json)))).status());
        }
        String many = "0,".repeat(4_100) + "0";
        assertEquals(PeerIdentityStatus.INVALID, provider.resolve(context("127.0.0.1",
                Map.of("Tailscale-App-Capabilities", List.of("{\"cap\":[" + many + "]}")))).status());
    }

    @Test void trustedProxyConfigurationRequiresUniqueNormalizedIpLiterals() {
        Map<String, List<String>> login = Map.of(
                "Tailscale-User-Login", List.of("alice@example.com"));
        var ipv6 = TailscalePeerIdentityProviders.serve("tailnet:example", Set.of("0:0:0:0:0:0:0:1"));
        assertEquals(PeerIdentityStatus.AVAILABLE, ipv6.resolve(context("::1", login)).status());
        var mapped = TailscalePeerIdentityProviders.serve("tailnet:example", Set.of("::ffff:192.0.2.10"));
        assertEquals(PeerIdentityStatus.AVAILABLE, mapped.resolve(context("192.0.2.10", login)).status());

        for (String invalid : List.of("proxy.internal", "127.0.0.0/8", "127.0.0.1:9400",
                "[::1]", "::1%lo0", "999.0.0.1", "127.00.0.1")) {
            assertThrows(IllegalArgumentException.class,
                    () -> TailscalePeerIdentityProviders.serve("tailnet:example", Set.of(invalid)));
        }
        Set<String> ipv6Aliases = new LinkedHashSet<>(List.of("::1", "0:0:0:0:0:0:0:1"));
        assertThrows(IllegalArgumentException.class,
                () -> TailscalePeerIdentityProviders.serve("tailnet:example", ipv6Aliases));
        Set<String> mappedAlias = new LinkedHashSet<>(List.of("192.0.2.10", "::ffff:c000:020a"));
        assertThrows(IllegalArgumentException.class,
                () -> TailscalePeerIdentityProviders.serve("tailnet:example", mappedAlias));
        assertEquals(PeerIdentityStatus.UNTRUSTED_PROXY,
                mapped.resolve(context("192.0.2.10:9400", login)).status());
    }

    private static PeerResolutionContext context(String peer, Map<String, List<String>> headers) {
        return new PeerResolutionContext("http", peer, "client", null, null, null,
                headers, Map.of(), null);
    }
}
