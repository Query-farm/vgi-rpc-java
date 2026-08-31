// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0
package farm.query.vgirpc.identity;

/** Peer evidence was present but invalid, ambiguous, or untrusted. */
public final class PeerIdentityRejectedException extends SecurityException {
    public PeerIdentityRejectedException(String message) { super(message); }
}
