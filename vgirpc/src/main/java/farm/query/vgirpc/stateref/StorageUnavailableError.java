// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.stateref;

import farm.query.vgirpc.errors.Code;

/**
 * A transient storage failure (5xx, timeout, transport error) while fetching an object.
 *
 * <p>Kind {@code storage_unavailable}, code {@code UNAVAILABLE}.</p>
 */
public final class StorageUnavailableError extends StateRefError {
    private static final long serialVersionUID = 1L;

    /** The {@code error_kind} this error carries. */
    public static final String KIND = "storage_unavailable";

    /**
     * @param message what went wrong (no URLs, tokens or values)
     */
    public StorageUnavailableError(String message) {
        super(KIND, Code.UNAVAILABLE, message);
    }
}
