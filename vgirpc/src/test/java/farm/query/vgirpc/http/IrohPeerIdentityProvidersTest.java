// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0
package farm.query.vgirpc.http;

import farm.query.vgirpc.identity.IdentityAssurance;
import farm.query.vgirpc.identity.PeerIdentityStatus;
import farm.query.vgirpc.identity.PeerResolutionContext;
import farm.query.vgirpc.identity.PeerSubjectKind;
import farm.query.vgirpc.identity.SubjectStability;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class IrohPeerIdentityProvidersTest {
    private static final String ENDPOINT = "000102030405060708090a0b0c0d0e0f"
            + "101112131415161718191a1b1c1d1e1f";

    @Test
    void trustedSanitizedHeaderProducesStableNamespacedEndpoint() {
        var provider = IrohPeerIdentityProviders.forwarded(
                "production-mesh", Set.of("127.0.0.1"));
        var result = provider.resolve(context("127.0.0.1", Map.of(
                IrohPeerIdentityProviders.FORWARDED_ENDPOINT_HEADER, List.of(ENDPOINT))));

        assertEquals(PeerIdentityStatus.AVAILABLE, result.status());
        var identity = result.identities().getFirst();
        assertEquals("production-mesh", identity.issuer());
        assertEquals(ENDPOINT, identity.subjectKey());
        assertEquals(PeerSubjectKind.ENDPOINT, identity.subjectKind());
        assertEquals(SubjectStability.STABLE, identity.subjectStability());
        assertEquals(IdentityAssurance.CONFIGURED_PROXY, identity.assurance());
        assertEquals("cryptographic_peer", identity.attributes().get("original_assurance"));
        assertEquals(ENDPOINT, identity.sourceAddress());
        assertEquals("127.0.0.1", identity.proxyAddress());
    }

    @Test
    void headerFailsClosedForUntrustedDuplicateOrNonCanonicalValues() {
        var provider = IrohPeerIdentityProviders.forwarded(
                "production-mesh", Set.of("127.0.0.1"));
        Map<String, List<String>> header = Map.of(
                IrohPeerIdentityProviders.FORWARDED_ENDPOINT_HEADER, List.of(ENDPOINT));
        assertEquals(PeerIdentityStatus.UNTRUSTED_PROXY,
                provider.resolve(context("192.0.2.1", header)).status());
        assertEquals(PeerIdentityStatus.NO_MATCH,
                provider.resolve(context("127.0.0.1", Map.of())).status());

        for (String invalid : List.of(ENDPOINT.toUpperCase(), ENDPOINT + " ", ENDPOINT.substring(1))) {
            assertEquals(PeerIdentityStatus.INVALID,
                    provider.resolve(context("127.0.0.1", Map.of(
                            IrohPeerIdentityProviders.FORWARDED_ENDPOINT_HEADER,
                            List.of(invalid)))).status());
        }
        assertEquals(PeerIdentityStatus.INVALID,
                provider.resolve(context("127.0.0.1", Map.of(
                        IrohPeerIdentityProviders.FORWARDED_ENDPOINT_HEADER,
                        List.of(ENDPOINT, ENDPOINT)))).status());
    }

    @Test
    void configurationUsesExactNormalizedProxyTrustAndRejectsCaseVariedHeaders() {
        var provider = IrohPeerIdentityProviders.forwarded(
                "production-mesh", Set.of("::ffff:192.0.2.10"));
        assertEquals(PeerIdentityStatus.AVAILABLE,
                provider.resolve(context("192.0.2.10", Map.of(
                        IrohPeerIdentityProviders.FORWARDED_ENDPOINT_HEADER,
                        List.of(ENDPOINT)))).status());
        assertThrows(IllegalArgumentException.class,
                () -> IrohPeerIdentityProviders.forwarded("mesh", Set.of("proxy.internal")));
        assertThrows(IllegalArgumentException.class,
                () -> IrohPeerIdentityProviders.forwarded("bad\nissuer", Set.of("127.0.0.1")));

        Map<String, List<String>> duplicates = new LinkedHashMap<>();
        duplicates.put(IrohPeerIdentityProviders.FORWARDED_ENDPOINT_HEADER, List.of(ENDPOINT));
        duplicates.put("vgi-forwarded-iroh-endpoint", List.of(ENDPOINT));
        assertThrows(farm.query.vgirpc.identity.PeerIdentityRejectedException.class,
                () -> context("127.0.0.1", duplicates));
    }

    private static PeerResolutionContext context(
            String peer, Map<String, List<String>> headers) {
        return new PeerResolutionContext(
                "http", peer, "client", null, null, null, headers, Map.of(), null);
    }
}
