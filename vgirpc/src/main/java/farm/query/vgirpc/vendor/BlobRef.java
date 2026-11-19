// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.vendor;

import java.util.Objects;

/**
 * A reference to one vendored blob, small enough to store inline.
 *
 * @param objectId the vendor's object id
 * @param size     exact byte length of the blob
 * @param sha256   lowercase hex SHA-256 of the blob bytes
 */
public record BlobRef(String objectId, long size, String sha256) {
    /**
     * Validates the fields.
     *
     * @param objectId the vendor's object id
     * @param size     exact byte length of the blob
     * @param sha256   lowercase hex SHA-256 of the blob bytes
     */
    public BlobRef {
        Objects.requireNonNull(objectId, "objectId");
        Objects.requireNonNull(sha256, "sha256");
        if (size < 0) throw new IllegalArgumentException("size must be >= 0");
    }
}
