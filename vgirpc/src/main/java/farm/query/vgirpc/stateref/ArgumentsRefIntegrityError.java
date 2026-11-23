// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.stateref;

import farm.query.vgirpc.errors.Code;

/**
 * The fetched argument object's length or SHA-256 differs from the sealed values.
 *
 * <p>Kind {@code arguments_ref_integrity}, code {@code DATA_LOSS}.</p>
 */
public final class ArgumentsRefIntegrityError extends StateRefError {
    private static final long serialVersionUID = 1L;

    /** The {@code error_kind} this error carries. */
    public static final String KIND = "arguments_ref_integrity";

    /**
     * @param message what went wrong (no URLs, tokens or values)
     */
    public ArgumentsRefIntegrityError(String message) {
        super(KIND, Code.DATA_LOSS, message);
    }
}
