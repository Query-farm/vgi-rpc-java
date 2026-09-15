// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.identity;

import java.util.List;

/**
 * Worker-supplied policy for {@code issue_grant}: whether, and what, to mint.
 *
 * <p>The framework has already established that the caller is who they say they are and
 * authenticated recently enough. Everything past that -- whether this principal may hold a grant
 * for this purpose, what the scopes mean, how long the grant really lives -- is the worker's.
 */
@FunctionalInterface
public interface GrantMintHook {

    /**
     * Mint a standing grant for {@code principal}.
     *
     * <p>{@code principal} is the <em>caller's</em> authenticated principal, never a request
     * parameter: cross-subject minting is closed by construction rather than by a check that one
     * of seven ports could forget.
     *
     * @param principal the caller, who is always the subject
     * @param purpose why the grant is being minted, for the audit trail
     * @param scopes what the grant may do. The worker decides what these mean; the framework
     *     neither interprets nor validates them
     * @param ttlSeconds requested lifetime. A request, not an instruction -- a shorter one may be
     *     returned, and the returned {@code expires_at} is authoritative
     * @return the minted grant
     * @throws GrantRefusedError when policy declines the mint
     */
    IssuedGrant mint(String principal, String purpose, List<String> scopes, long ttlSeconds);
}
