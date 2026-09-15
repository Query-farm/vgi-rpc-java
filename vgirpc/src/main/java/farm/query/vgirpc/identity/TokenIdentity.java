// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.identity;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/**
 * The identity an opaque credential authenticates as.
 *
 * <p><strong>Never carries claims.</strong> A pass-through claims field would let a worker
 * choose its caller's tenant routing, its row scope, and its policy branch, and the asker
 * derives everything it needs from the principal alone. Do not add one.
 *
 * <p>Component names are snake_case and their declaration order is fixed, because both are part
 * of the derived Arrow schema and therefore of the protocol hash every port must agree on.
 *
 * @param principal the canonical principal, in the exact form the worker itself would derive --
 *     so an asker that normalises differently does not authorize as one identity while the
 *     worker serves another
 * @param token_name human-readable name for the credential, for audit trails. Never the
 *     credential
 * @param ttl_seconds how long the answer may be cached. The caller does the caching. Treat it as
 *     an authorization window: for any path the asker serves without re-presenting the
 *     credential it is exactly that, and therefore also the revocation lag
 */
public record TokenIdentity(String principal, String token_name, long ttl_seconds)
        implements ArrowSerializableRecord {

    /** The cache lifetime assumed when a worker names none. */
    public static final long DEFAULT_TTL_SECONDS = 300;

    /** Normalises an absent name to {@code ""}: the column is non-nullable on the wire. */
    public TokenIdentity {
        if (token_name == null) token_name = "";
    }

    /**
     * Resolve to {@code principal} with the default name and TTL.
     *
     * @param principal the canonical principal
     */
    public TokenIdentity(String principal) {
        this(principal, "", DEFAULT_TTL_SECONDS);
    }

    /**
     * Resolve to {@code principal} with an audit name and the default TTL.
     *
     * @param principal the canonical principal
     * @param token_name human-readable credential name, for audit trails
     */
    public TokenIdentity(String principal, String token_name) {
        this(principal, token_name, DEFAULT_TTL_SECONDS);
    }
}
