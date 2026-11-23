// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.stateref;

import farm.query.vgirpc.errors.Code;

/**
 * A stored object exceeds the server's {@code max_fetch_bytes}, at store or at resolve.
 *
 * <p>Kind {@code state_ref_too_large}, code {@code RESOURCE_EXHAUSTED}.</p>
 */
public final class StateRefTooLargeError extends StateRefError {
    private static final long serialVersionUID = 1L;

    /** The {@code error_kind} this error carries. */
    public static final String KIND = "state_ref_too_large";

    /**
     * @param message what went wrong (no URLs, tokens or values)
     */
    public StateRefTooLargeError(String message) {
        super(KIND, Code.RESOURCE_EXHAUSTED, message);
    }
}
