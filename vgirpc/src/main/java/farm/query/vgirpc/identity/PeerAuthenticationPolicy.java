// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0
package farm.query.vgirpc.identity;

import farm.query.vgirpc.AuthContext;

/** Composes application authentication with verified transport evidence. */
@FunctionalInterface
public interface PeerAuthenticationPolicy {
    AuthContext evaluate(PeerEvidenceSet evidence, AuthContext existingAuth);
}
