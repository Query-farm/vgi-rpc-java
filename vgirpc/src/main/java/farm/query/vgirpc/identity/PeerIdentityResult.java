// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0
package farm.query.vgirpc.identity;

import java.util.List;

/** One provider's definitive resolution outcome. */
public record PeerIdentityResult(String provider, PeerIdentityStatus status, List<PeerIdentity> identities) {
    public PeerIdentityResult {
        if (provider == null || provider.isBlank() || status == null) {
            throw new IllegalArgumentException("provider and status are required");
        }
        identities = identities != null ? List.copyOf(identities) : List.of();
        if ((status == PeerIdentityStatus.AVAILABLE) != !identities.isEmpty()) {
            throw new IllegalArgumentException("only an available result may carry identities");
        }
        if (identities.stream().anyMatch(identity -> !provider.equals(identity.provider()))) {
            throw new IllegalArgumentException("peer result provider mismatch");
        }
    }

    public PeerIdentityResult(String provider, PeerIdentityStatus status) { this(provider, status, List.of()); }
    public static PeerIdentityResult available(PeerIdentity identity) {
        return new PeerIdentityResult(identity.provider(), PeerIdentityStatus.AVAILABLE, List.of(identity));
    }
}
