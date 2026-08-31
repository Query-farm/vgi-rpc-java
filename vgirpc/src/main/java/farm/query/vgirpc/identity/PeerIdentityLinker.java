// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0
package farm.query.vgirpc.identity;

import farm.query.vgirpc.AuthContext;
import java.util.Map;

/** Application callback that rejects conflicting identities in all-of mode. */
@FunctionalInterface
public interface PeerIdentityLinker {
    void verify(AuthContext applicationAuth, Map<String, PeerIdentity> identities);
}
