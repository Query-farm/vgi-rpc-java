// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

/**
 * Schema derivation: maps Java types to Arrow schemas
 * ({@link farm.query.vgirpc.schema.SchemaDerivation}) and provides the annotations
 * that refine that mapping ({@link farm.query.vgirpc.schema.ArrowField},
 * {@link farm.query.vgirpc.schema.ArrowFieldType},
 * {@link farm.query.vgirpc.schema.Nullable}), plus enum dictionary support and
 * streaming header metadata.
 */
package farm.query.vgirpc.schema;
