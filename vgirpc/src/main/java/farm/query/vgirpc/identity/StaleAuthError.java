// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.identity;

import farm.query.vgirpc.HasErrorKind;

/**
 * The caller has not authenticated recently enough to mint a grant.
 *
 * <p>Definitive but <em>actionable</em>, unlike the introspection rejections: this is always
 * about the caller themselves, so naming the reason leaks nothing and is the only way a console
 * learns to re-prompt. A console that cannot tell "your login is too old" from "no" cannot know
 * to send the user back to the identity provider.
 */
public final class StaleAuthError extends SecurityException implements HasErrorKind {

    /** The stable wire category for a caller whose authentication is too old or unverifiable. */
    public static final String ERROR_KIND = "stale_auth";

    /**
     * Refuse the mint, naming the reason.
     *
     * @param message what the caller must do differently
     */
    public StaleAuthError(String message) {
        super(message);
    }

    @Override
    public String errorKind() {
        return ERROR_KIND;
    }
}
