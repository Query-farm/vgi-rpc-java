// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.identity;

import java.util.List;

/**
 * What a verified sealed grant says.
 *
 * @param principal whose standing delegation this is -- the caller it was minted for
 * @param scopes what it may do; the worker interprets them
 * @param purpose why it was minted, for the audit trail
 * @param grantId correlation handle
 * @param issuedAt seconds since the Unix epoch
 * @param expiresAt seconds since the Unix epoch
 */
public record GrantClaims(String principal, List<String> scopes, String purpose, String grantId,
                          long issuedAt, long expiresAt) {
    /** Freeze the scopes. */
    public GrantClaims {
        scopes = scopes == null ? List.of() : List.copyOf(scopes);
    }
}
