// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0
package farm.query.vgirpc.http;

import farm.query.vgirpc.identity.IdentityAssurance;
import farm.query.vgirpc.identity.PeerIdentity;
import farm.query.vgirpc.identity.PeerIdentityProvider;
import farm.query.vgirpc.identity.PeerIdentityRejectedException;
import farm.query.vgirpc.identity.PeerIdentityResult;
import farm.query.vgirpc.identity.PeerIdentityStatus;
import farm.query.vgirpc.identity.PeerResolutionContext;
import farm.query.vgirpc.identity.PeerSubjectKind;
import farm.query.vgirpc.identity.SubjectStability;

import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** Trusted HTTP forwarding of bridge-verified Iroh EndpointIds. */
public final class IrohPeerIdentityProviders {
    public static final String FORWARDED_ENDPOINT_HEADER = "VGI-Forwarded-Iroh-Endpoint";

    private static final String PROVIDER = "iroh";
    private static final Pattern CANONICAL_ENDPOINT = Pattern.compile("[0-9a-f]{64}");

    private IrohPeerIdentityProviders() {}

    /** Resolve one sanitized EndpointId only from an exact trusted bridge address. */
    public static PeerIdentityProvider forwarded(
            String issuer, Set<String> trustedProxyAddresses) {
        if (issuer == null || issuer.isBlank() || containsControl(issuer)) {
            throw new IllegalArgumentException("Iroh issuer must be non-empty text without controls");
        }
        Set<String> proxies = ExactIpAddresses.trusted(trustedProxyAddresses);
        return new PeerIdentityProvider() {
            @Override public String provider() { return PROVIDER; }

            @Override public PeerIdentityResult resolve(PeerResolutionContext context) {
                if (!ExactIpAddresses.contains(proxies, context.immediatePeer())) {
                    return result(PeerIdentityStatus.UNTRUSTED_PROXY);
                }
                try {
                    String endpointId = context.header(FORWARDED_ENDPOINT_HEADER);
                    if (endpointId == null) return result(PeerIdentityStatus.NO_MATCH);
                    if (!CANONICAL_ENDPOINT.matcher(endpointId).matches()) {
                        return result(PeerIdentityStatus.INVALID);
                    }
                    PeerIdentity identity = new PeerIdentity(
                            PROVIDER, "http_proxy", IdentityAssurance.CONFIGURED_PROXY,
                            issuer, "http", PeerSubjectKind.ENDPOINT, endpointId,
                            SubjectStability.STABLE, true,
                            Map.of("original_assurance",
                                    IdentityAssurance.CRYPTOGRAPHIC_PEER.wireValue()),
                            Map.of(), false, endpointId, context.immediatePeer());
                    return PeerIdentityResult.available(identity);
                } catch (IllegalArgumentException | PeerIdentityRejectedException e) {
                    return result(PeerIdentityStatus.INVALID);
                }
            }
        };
    }

    private static PeerIdentityResult result(PeerIdentityStatus status) {
        return new PeerIdentityResult(PROVIDER, status);
    }

    private static boolean containsControl(String value) {
        return value.codePoints().anyMatch(code -> code <= 0x1f || code == 0x7f);
    }
}
