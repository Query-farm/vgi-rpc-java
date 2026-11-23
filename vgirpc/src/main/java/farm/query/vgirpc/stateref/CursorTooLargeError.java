// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.stateref;

import farm.query.vgirpc.errors.Code;

/**
 * A sealed cursor exceeds the server's turn budget under the {@code ERROR} cursor rule.
 *
 * <p>Kind {@code cursor_too_large}, code {@code RESOURCE_EXHAUSTED}.</p>
 */
public final class CursorTooLargeError extends StateRefError {
    private static final long serialVersionUID = 1L;

    /** The {@code error_kind} this error carries. */
    public static final String KIND = "cursor_too_large";

    /**
     * @param message what went wrong (no URLs, tokens or values)
     */
    public CursorTooLargeError(String message) {
        super(KIND, Code.RESOURCE_EXHAUSTED, message);
    }
}
