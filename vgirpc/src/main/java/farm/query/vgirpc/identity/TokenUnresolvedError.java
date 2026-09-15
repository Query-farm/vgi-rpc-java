// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.identity;

import farm.query.vgirpc.HasErrorKind;

/**
 * The subject credential did not resolve.
 *
 * <p>Definitive, and deliberately uniform: unknown, expired and malformed are one answer,
 * because reporting which would confirm that a guessed credential exists. The message is
 * therefore always {@code "unresolved"} -- there is nothing else it may say.
 */
public final class TokenUnresolvedError extends IllegalArgumentException implements HasErrorKind {

    /** The stable wire category for a credential that did not resolve. */
    public static final String ERROR_KIND = "token_unresolved";

    /**
     * Refuse the subject credential.
     *
     * @param message always {@code "unresolved"}; a more specific message would leak which of
     *     unknown, expired or malformed applied
     */
    public TokenUnresolvedError(String message) {
        super(message);
    }

    @Override
    public String errorKind() {
        return ERROR_KIND;
    }
}
