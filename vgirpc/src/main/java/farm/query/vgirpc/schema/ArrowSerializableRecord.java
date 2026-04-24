// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.schema;

/**
 * Marker interface for Java records that should be auto-serialized to/from
 * Arrow batches. The framework derives an Arrow {@code Schema} from the
 * record components via reflection.
 *
 * <p>Implementing records may use {@link ArrowField} to override default type
 * inference and {@link Nullable} to mark optional components.</p>
 */
public interface ArrowSerializableRecord {
}
