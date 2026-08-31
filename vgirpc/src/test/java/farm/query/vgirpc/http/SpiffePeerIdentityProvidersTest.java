// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0
package farm.query.vgirpc.http;

import farm.query.vgirpc.identity.IdentityAssurance;
import farm.query.vgirpc.identity.PeerIdentityProvider;
import farm.query.vgirpc.identity.PeerIdentityResult;
import farm.query.vgirpc.identity.PeerIdentityStatus;
import farm.query.vgirpc.identity.PeerResolutionContext;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class SpiffePeerIdentityProvidersTest {
    private static final String PEM = """
            -----BEGIN CERTIFICATE-----
            MIIDazCCAlOgAwIBAgIUG0eLA1ht8L3mAXNQqWfPUZc/Ee4wDQYJKoZIhvcNAQEL
            BQAwEzERMA8GA1UEAwwIdmdpLXRlc3QwHhcNMjYwODMwMjM1MDMwWhcNMzYwODI3
            MjM1MDMwWjATMREwDwYDVQQDDAh2Z2ktdGVzdDCCASIwDQYJKoZIhvcNAQEBBQAD
            ggEPADCCAQoCggEBALSDZQA4+r/bFdEMHAPoiap59VUZLjc2SsJ73dg0lwgdbK2j
            hSH73t+5pGGMcDcByMVRvvwW03rYlCMKonD5R3sddR0N9pGDZotJlBpGxHj0FojS
            Jw/PnVu8HuarrSah8QDLGmaSVOzKtpCaPaEg2HqoTt9mG0GLK9UJ/uYiV3vGyRH7
            opRB3vlReaL2hY3et+CqDGzTMDrBbc/M249mRmKgurHZFF5Pdmb9DGGcLuZKa7Uq
            FLHiKvl3eo/iwy1K9W9s2bG1VQOl4fYPiBhfUFgNDcP2/5haIPerr2owMGf4O0kj
            cJ0KwSNui2OEnePmaht/MYi/wl9ZsRtYyXlv1NsCAwEAAaOBtjCBszAdBgNVHQ4E
            FgQU63dspjRyaZNwyAURajyyM0ASIEswHwYDVR0jBBgwFoAU63dspjRyaZNwyAUR
            ajyyM0ASIEswNAYDVR0RBC0wK4Ypc3BpZmZlOi8vZXhhbXBsZS5vcmcvbnMvZGVm
            YXVsdC9zYS9jbGllbnQwDAYDVR0TAQH/BAIwADAOBgNVHQ8BAf8EBAMCB4AwHQYD
            VR0lBBYwFAYIKwYBBQUHAwIGCCsGAQUFBwMBMA0GCSqGSIb3DQEBCwUAA4IBAQAg
            1WAv5NFHDk/oGOYFQYAaArss02gmHecu6qk8BjZlBx5l8X+ZP9XP4RFN/y1q8FQ6
            nTaxoI5EvBCHHD/RwqO6VzqJoaRvS4gbBuFJj3PeVt3GnAYimBFCkU1z9ckIF4Pb
            AMFiL2NemMcrwZ14FJiH2S+PoBXfJnVQTU912O46kH5rnH53TgNoybg+duCtx46w
            IXPTMNrejCQFvrlag1vSyhybTLqaNf20+0eA4u9CNb2n4jUf2JL7ffOyEKoyXuuh
            FubCM2PL2iXOqdnlDBtza/WP8oh6l55p38nnkApuo068QRsbTwrmMWfPRFSpctnX
            HKiLgbaVBM1fvPmoSdLy
            -----END CERTIFICATE-----
            """;

    @Test void nginxAndCloudCertificateProfilesProduceConfiguredProxyEvidence() {
        String certificate = percentEncode(PEM);
        assertAvailable(SpiffePeerIdentityProviders.nginx(Set.of("example.org"), Set.of("127.0.0.1")),
                headers("X-SSL-Client-Cert", certificate, "X-SSL-Client-Verify", "SUCCESS"), "nginx_mtls");
        assertAvailable(SpiffePeerIdentityProviders.azureApplicationGateway(Set.of("example.org"), Set.of("127.0.0.1")),
                headers("X-Client-Certificate", certificate, "X-Client-Certificate-Verification", "SUCCESS"),
                "azure_application_gateway_mtls_strict");
        assertAvailable(SpiffePeerIdentityProviders.awsAlb(Set.of("example.org"), Set.of("127.0.0.1")),
                Map.of("X-Amzn-Mtls-Clientcert-Leaf", List.of(certificate)), "aws_alb_mtls_verify");
    }

    @Test void certificateProfilesFailClosedAtTrustAndHeaderBoundaries() {
        PeerIdentityProvider provider = SpiffePeerIdentityProviders.nginx(Set.of("example.org"), Set.of("127.0.0.1"));
        Map<String, List<String>> valid = headers("X-SSL-Client-Cert", percentEncode(PEM),
                "X-SSL-Client-Verify", "SUCCESS");
        assertEquals(PeerIdentityStatus.UNTRUSTED_PROXY, provider.resolve(context("192.0.2.99", valid)).status());
        assertEquals(PeerIdentityStatus.INVALID, provider.resolve(context("127.0.0.1",
                headers("X-SSL-Client-Cert", percentEncode(PEM), "X-SSL-Client-Verify", "FAILED"))).status());
        assertEquals(PeerIdentityStatus.INVALID, provider.resolve(context("127.0.0.1", Map.of(
                "X-SSL-Client-Cert", List.of(percentEncode(PEM), percentEncode(PEM)),
                "X-SSL-Client-Verify", List.of("SUCCESS")))).status());
        assertEquals(PeerIdentityStatus.INVALID, provider.resolve(context("127.0.0.1", headers(
                "X-SSL-Client-Cert", percentEncode(PEM) + ",duplicate",
                "X-SSL-Client-Verify", "SUCCESS"))).status());
    }

    @Test void gcpRequiresAllFrontendMtlsSignalsAndOneCanonicalId() {
        PeerIdentityProvider provider = SpiffePeerIdentityProviders.gcpLoadBalancer(
                Set.of("example.org"), Set.of("127.0.0.1"));
        Map<String, List<String>> valid = Map.of(
                "X-Client-Cert-Present", List.of("true"),
                "X-Client-Cert-Chain-Verified", List.of("true"),
                "X-Client-Cert-Spiffe-Id", List.of("spiffe://example.org/ns/default/sa/client"));
        assertAvailable(provider, valid, "gcp_load_balancer_mtls");
        assertEquals(PeerIdentityStatus.INVALID, provider.resolve(context("127.0.0.1", Map.of(
                "X-Client-Cert-Present", List.of("true"),
                "X-Client-Cert-Chain-Verified", List.of("false"),
                "X-Client-Cert-Spiffe-Id", List.of("spiffe://example.org/ns/default/sa/client")))).status());
        assertEquals(PeerIdentityStatus.NO_MATCH, provider.resolve(context("127.0.0.1",
                Map.of("X-Client-Cert-Present", List.of("false")))).status());
        assertEquals(PeerIdentityStatus.INVALID, provider.resolve(context("127.0.0.1", Map.of(
                "X-Client-Cert-Present", List.of("true"),
                "X-Client-Cert-Chain-Verified", List.of("true"),
                "X-Client-Cert-Spiffe-Id", List.of("spiffe://example.org/a%2Fb")))).status());
    }

    @Test void envoyRequiresOneSanitizeSetElementUriAndHash() {
        PeerIdentityProvider provider = SpiffePeerIdentityProviders.envoyXfcc(
                Set.of("example.org"), Set.of("127.0.0.1"));
        String valid = "By=spiffe://mesh.example/proxy;Hash=" + "a".repeat(64)
                + ";URI=spiffe://example.org/ns/default/sa/client";
        assertAvailable(provider, Map.of("X-Forwarded-Client-Cert", List.of(valid)),
                "envoy_xfcc_sanitize_set");
        for (String invalid : List.of(
                valid + ",Hash=" + "b".repeat(64) + ";URI=spiffe://example.org/other",
                "Hash=" + "a".repeat(64) + ";Hash=" + "b".repeat(64) + ";URI=spiffe://example.org/client",
                "Hash=" + "a".repeat(64) + ";URI=spiffe://other.org/client",
                "Unknown=x;Hash=" + "a".repeat(64) + ";URI=spiffe://example.org/client",
                "Hash=abc;URI=spiffe://example.org/client")) {
            assertEquals(PeerIdentityStatus.INVALID, provider.resolve(context("127.0.0.1",
                    Map.of("X-Forwarded-Client-Cert", List.of(invalid)))).status());
        }
    }

    @Test void spiffeValidationRejectsAliasesAndInvalidConfiguration() {
        assertEquals("example.org", SpiffePeerIdentityProviders.validateSpiffeId(
                "spiffe://example.org/ns/default/sa/client", Set.of("example.org")));
        for (String invalid : List.of("spiffe://example.org/a%2Fb", "spiffe://example.org/a//b",
                "spiffe://example.org/a/../b", "spiffe://example.org/a/", "spiffe://Example.org/a",
                "spiffe://example.org:443/a", "spiffe://example.org/a?x=1")) {
            assertThrows(IllegalArgumentException.class,
                    () -> SpiffePeerIdentityProviders.validateSpiffeId(invalid, Set.of("example.org")));
        }
        assertThrows(IllegalArgumentException.class,
                () -> SpiffePeerIdentityProviders.nginx(Set.of(), Set.of("127.0.0.1")));
    }

    @Test void trustedProxyConfigurationNormalizesOnlyExactIpLiterals() {
        String valid = "Hash=" + "a".repeat(64)
                + ";URI=spiffe://example.org/ns/default/sa/client";
        var ipv6 = SpiffePeerIdentityProviders.envoyXfcc(
                Set.of("example.org"), Set.of("0:0:0:0:0:0:0:1"));
        assertEquals(PeerIdentityStatus.AVAILABLE, ipv6.resolve(context("::1",
                Map.of("X-Forwarded-Client-Cert", List.of(valid)))).status());
        var mapped = SpiffePeerIdentityProviders.envoyXfcc(
                Set.of("example.org"), Set.of("::ffff:192.0.2.10"));
        assertEquals(PeerIdentityStatus.AVAILABLE, mapped.resolve(context("192.0.2.10",
                Map.of("X-Forwarded-Client-Cert", List.of(valid)))).status());

        for (String invalid : List.of("proxy.internal", "10.0.0.0/8", "127.0.0.1:9400",
                "[::1]", "fe80::1%eth0", "1.2.3.999", "01.2.3.4")) {
            assertThrows(IllegalArgumentException.class,
                    () -> SpiffePeerIdentityProviders.envoyXfcc(
                            Set.of("example.org"), Set.of(invalid)));
        }
        Set<String> compressedAlias = new LinkedHashSet<>(
                List.of("2001:db8::1", "2001:0db8:0:0:0:0:0:1"));
        assertThrows(IllegalArgumentException.class,
                () -> SpiffePeerIdentityProviders.envoyXfcc(
                        Set.of("example.org"), compressedAlias));
        Set<String> mappedAlias = new LinkedHashSet<>(
                List.of("192.0.2.10", "0:0:0:0:0:ffff:c000:20a"));
        assertThrows(IllegalArgumentException.class,
                () -> SpiffePeerIdentityProviders.envoyXfcc(
                        Set.of("example.org"), mappedAlias));
        assertEquals(PeerIdentityStatus.UNTRUSTED_PROXY,
                mapped.resolve(context("192.0.2.10:443",
                        Map.of("X-Forwarded-Client-Cert", List.of(valid)))).status());
    }

    private static void assertAvailable(PeerIdentityProvider provider, Map<String, List<String>> headers,
            String evidenceSource) {
        PeerIdentityResult result = provider.resolve(context("127.0.0.1", headers));
        assertEquals(PeerIdentityStatus.AVAILABLE, result.status());
        assertEquals("spiffe://example.org/ns/default/sa/client", result.identities().getFirst().subjectKey());
        assertEquals(evidenceSource, result.identities().getFirst().evidenceSource());
        assertEquals(IdentityAssurance.CONFIGURED_PROXY, result.identities().getFirst().assurance());
    }

    private static PeerResolutionContext context(String immediatePeer, Map<String, List<String>> headers) {
        return new PeerResolutionContext("http", immediatePeer, "client", null, null, null,
                headers, Map.of(), null);
    }

    private static Map<String, List<String>> headers(String firstName, String firstValue,
            String secondName, String secondValue) {
        return Map.of(firstName, List.of(firstValue), secondName, List.of(secondValue));
    }

    private static String percentEncode(String value) {
        StringBuilder encoded = new StringBuilder();
        for (byte raw : value.getBytes(StandardCharsets.UTF_8)) {
            int octet = raw & 0xff;
            if ((octet >= 'a' && octet <= 'z') || (octet >= 'A' && octet <= 'Z')
                    || (octet >= '0' && octet <= '9') || "-._~".indexOf(octet) >= 0) {
                encoded.append((char) octet);
            } else {
                encoded.append('%').append(Character.toUpperCase(Character.forDigit(octet >>> 4, 16)))
                        .append(Character.toUpperCase(Character.forDigit(octet & 15, 16)));
            }
        }
        return encoded.toString();
    }
}
