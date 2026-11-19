// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.vendor;

import java.util.Objects;

/**
 * The execution a blob belongs to, for scoped clean-up
 * ({@link VendorBlobClient#deleteScope}).
 *
 * <p>The vendor accepts 1-128 characters of {@code [A-Za-z0-9._:-]} for each
 * field, so encode binary ids as hex.</p>
 *
 * @param shard       the storage shard key
 * @param executionId the execution identifier (e.g. hex of the raw id bytes)
 */
public record BlobScope(String shard, String executionId) {
    /**
     * Validates that neither field is null.
     *
     * @param shard       the storage shard key
     * @param executionId the execution identifier
     */
    public BlobScope {
        Objects.requireNonNull(shard, "shard");
        Objects.requireNonNull(executionId, "executionId");
    }
}
