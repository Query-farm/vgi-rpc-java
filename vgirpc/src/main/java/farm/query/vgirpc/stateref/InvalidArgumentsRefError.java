// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.stateref;

import farm.query.vgirpc.errors.Code;

/**
 * An argument reference that does not open for the caller, does not match the size and SHA-256 sent with it, or cannot be resolved by this server.
 *
 * <p>Kind {@code invalid_arguments_ref}, code {@code INVALID_ARGUMENT}.</p>
 */
public final class InvalidArgumentsRefError extends StateRefError {
    private static final long serialVersionUID = 1L;

    /** The {@code error_kind} this error carries. */
    public static final String KIND = "invalid_arguments_ref";

    /**
     * @param message what went wrong (no URLs, tokens or values)
     */
    public InvalidArgumentsRefError(String message) {
        super(KIND, Code.INVALID_ARGUMENT, message);
    }
}
