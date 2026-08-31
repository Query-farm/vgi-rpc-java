// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0
package farm.query.vgirpc.identity;

/** Adapter from transport/runtime facts to verified peer evidence. */
public interface PeerIdentityProvider {
    String provider();
    PeerIdentityResult resolve(PeerResolutionContext context);
}
