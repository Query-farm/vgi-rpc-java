// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.stateref;

import farm.query.vgirpc.errors.Code;

/**
 * {@code state_ref()} was called anywhere but on the context handed to a stream method (the handling of the stream's init request).
 *
 * <p>Kind {@code state_ref_misuse}, code {@code FAILED_PRECONDITION}.</p>
 */
public final class StateRefMisuseError extends StateRefError {
    private static final long serialVersionUID = 1L;

    /** The {@code error_kind} this error carries. */
    public static final String KIND = "state_ref_misuse";

    /**
     * @param message what went wrong (no URLs, tokens or values)
     */
    public StateRefMisuseError(String message) {
        super(KIND, Code.FAILED_PRECONDITION, message);
    }
}
