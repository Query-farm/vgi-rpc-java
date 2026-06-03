// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

/**
 * External-location support for oversized batches: an {@link farm.query.vgirpc.external.Externalizer}
 * spills a large batch to a pluggable {@link farm.query.vgirpc.external.ExternalStorage}
 * backend (S3/GCS) and replaces it on the wire with a pointer batch that the peer
 * resolves via {@link farm.query.vgirpc.external.LocationResolver} /
 * {@link farm.query.vgirpc.external.ExternalFetcher}.
 */
package farm.query.vgirpc.external;
