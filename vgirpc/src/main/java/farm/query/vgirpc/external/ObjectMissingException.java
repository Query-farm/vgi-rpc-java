// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.external;

/**
 * Storage reports an object absent (HTTP 404, vendor {@code not_found} / {@code not_uploaded}).
 *
 * <p>Distinguishable from a transient failure on purpose: a missing object is final
 * ({@code state_ref_missing} / {@code arguments_ref_missing}), while a 5xx or a timeout is
 * {@code UNAVAILABLE} and may succeed on retry. Messages never contain signed URLs.</p>
 */
public final class ObjectMissingException extends Exception {
    private static final long serialVersionUID = 1L;

    /**
     * @param message a description without signed URLs
     */
    public ObjectMissingException(String message) {
        super(message);
    }
}
