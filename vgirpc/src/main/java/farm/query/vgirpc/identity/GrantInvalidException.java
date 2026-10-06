// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.identity;

/**
 * A token carrying the {@code vgig1.} prefix could not be accepted.
 *
 * <p>One type for every cause on purpose -- malformed, wrong key, wrong audience, tampered,
 * expired -- so a caller cannot tell a forged token from a stale one except by {@link #expired()},
 * which is only true once the token was proven authentic and so tells a forger nothing.
 */
public final class GrantInvalidException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final boolean expired;

    GrantInvalidException(String detail, boolean expired) {
        super(detail);
        this.expired = expired;
    }

    GrantInvalidException(String detail) {
        this(detail, false);
    }

    /** @return {@code true} for an authentic grant outside its lifetime (expired or not yet valid) */
    public boolean expired() { return expired; }
}
