// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

/**
 * The identity an opaque credential authenticates as — the entire answer token
 * introspection may give.
 *
 * <p>Three fields, and no fourth. A claims field would let a worker choose its
 * caller's tenant routing, its row scope, and its policy branch, which is the
 * single most dangerous thing this feature could grow; askers derive what they
 * need from the principal alone. The conformance group asserts the response key
 * set is closed, so a field added here has to come through that test.
 *
 * @param principal the canonical principal, in the exact form this worker would
 *        derive itself. An asker that normalises differently would otherwise
 *        authorize as one identity while the worker serves another.
 * @param tokenName human-readable name for the credential, for audit trails.
 *        Never the credential.
 * @param ttlSeconds how long the answer may be cached; {@code 0} takes the
 *        server-configured default. The <em>caller</em> does the caching — this
 *        endpoint holds none. Treat it as an authorization window, because for
 *        any path the asker serves without re-presenting the credential that is
 *        exactly what it is.
 */
public record TokenIdentity(String principal, String tokenName, long ttlSeconds) {

    /** Validates the principal and normalises the optional fields. */
    public TokenIdentity {
        if (principal == null || principal.isEmpty()) {
            throw new IllegalArgumentException("principal must not be empty");
        }
        tokenName = tokenName != null ? tokenName : "";
        if (ttlSeconds < 0) throw new IllegalArgumentException("ttlSeconds must be >= 0");
    }

    /**
     * An identity taking the server's configured TTL.
     *
     * @param principal the canonical principal
     * @param tokenName display name for the credential
     */
    public TokenIdentity(String principal, String tokenName) { this(principal, tokenName, 0); }
}
