// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.stateref;

import farm.query.vgirpc.errors.Code;

/**
 * Storage reports a stored {@link StateRef}'s object absent.
 *
 * <p>Kind {@code state_ref_missing}, code {@code NOT_FOUND}.</p>
 */
public final class StateRefMissingError extends StateRefError {
    private static final long serialVersionUID = 1L;

    /** The {@code error_kind} this error carries. */
    public static final String KIND = "state_ref_missing";

    /**
     * @param message what went wrong (no URLs, tokens or values)
     */
    public StateRefMissingError(String message) {
        super(KIND, Code.NOT_FOUND, message);
    }

    /** Create the error with its normative message. */
    public StateRefMissingError() {
        this("stream state object is gone (StateRef object expired or deleted); restart the query");
    }
}
