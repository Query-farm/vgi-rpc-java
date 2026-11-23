// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.stateref;

import farm.query.vgirpc.errors.Code;

/**
 * Storage reports an argument object absent (deleted by lifecycle, or never uploaded).
 *
 * <p>Kind {@code arguments_ref_missing}, code {@code NOT_FOUND}.</p>
 */
public final class ArgumentsRefMissingError extends StateRefError {
    private static final long serialVersionUID = 1L;

    /** The {@code error_kind} this error carries. */
    public static final String KIND = "arguments_ref_missing";

    /**
     * @param message what went wrong (no URLs, tokens or values)
     */
    public ArgumentsRefMissingError(String message) {
        super(KIND, Code.NOT_FOUND, message);
    }

    /** Create the error with its normative message. */
    public ArgumentsRefMissingError() {
        this("the uploaded argument object is gone (expired or deleted)");
    }
}
