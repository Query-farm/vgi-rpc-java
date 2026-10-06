// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.identity;

import farm.query.vgirpc.HasErrorKind;

/**
 * The worker declined to mint this grant.
 *
 * <p>Definitive. The worker holds the policy; the framework only asked.
 */
public final class GrantRefusedError extends SecurityException implements HasErrorKind, farm.query.vgirpc.errors.HasErrorCode {

    /** The stable wire category for a refused mint. */
    public static final String ERROR_KIND = "grant_refused";

    /**
     * Refuse the mint.
     *
     * @param message why the worker declined
     */
    public GrantRefusedError(String message) {
        super(message);
    }

    @Override
    public String errorKind() {
        return ERROR_KIND;
    }

    /** {@code PERMISSION_DENIED}, fixed where this kind is defined (WIRE_PROTOCOL.md §8). */
    @Override
    public farm.query.vgirpc.errors.Code errorCode() {
        return farm.query.vgirpc.errors.Code.PERMISSION_DENIED;
    }
}
