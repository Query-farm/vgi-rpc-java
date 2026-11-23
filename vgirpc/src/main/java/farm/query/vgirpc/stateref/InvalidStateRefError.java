// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.stateref;

import farm.query.vgirpc.errors.Code;

/**
 * Unknown form, a tag that does not open (tampered, another stream, another principal or protocol), a bad layout, an expired tag, a size/digest mismatch, or an object that does not open. Answered with HTTP 400, like every other token failure.
 *
 * <p>Kind {@code invalid_state_ref}, code {@code INVALID_ARGUMENT}.</p>
 */
public final class InvalidStateRefError extends StateRefError {
    private static final long serialVersionUID = 1L;

    /** The {@code error_kind} this error carries. */
    public static final String KIND = "invalid_state_ref";

    /**
     * @param message what went wrong (no URLs, tokens or values)
     */
    public InvalidStateRefError(String message) {
        super(KIND, Code.INVALID_ARGUMENT, message);
    }
}
