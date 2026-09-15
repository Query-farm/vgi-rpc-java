// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.identity;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/**
 * A standing delegation credential.
 *
 * <p>Component names are snake_case and their declaration order is fixed: both are part of the
 * derived Arrow schema and therefore of the protocol hash every port must agree on.
 *
 * @param token the credential. <strong>Opaque to the framework</strong> -- the worker owns the
 *     format entirely (a sealed envelope, a database row, or a credential brokered from the IdP
 *     are all equally valid and equally invisible here). Never parsed, never logged
 * @param expires_at unix timestamp after which the worker will stop honouring the grant.
 *     Required <em>because</em> the framework cannot enforce it: the real lifetime lives inside
 *     the opaque token, so this is a declaration rather than an enforcement. A worker that must
 *     state a lifetime has thought about one
 * @param grant_id correlation handle for the audit trail. Not a credential and not secret -- it
 *     is what ties a mint record to later use
 */
public record IssuedGrant(String token, double expires_at, String grant_id)
        implements ArrowSerializableRecord {

    /** Normalises an absent correlation handle to {@code ""}: the column is non-nullable. */
    public IssuedGrant {
        if (grant_id == null) grant_id = "";
    }

    /**
     * A grant with no correlation handle.
     *
     * @param token the opaque credential
     * @param expires_at declared expiry, as a unix timestamp
     */
    public IssuedGrant(String token, double expires_at) {
        this(token, expires_at, "");
    }
}
