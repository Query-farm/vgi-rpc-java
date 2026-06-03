// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

/**
 * Structured log messages carried on the RPC stream. A {@link farm.query.vgirpc.log.Message}
 * at a given {@link farm.query.vgirpc.log.Level} is serialized as a zero-row batch
 * whose {@code vgi_rpc.log_*} custom metadata carries the level, message, and extra
 * fields — surfaced to the caller without disturbing the data stream.
 */
package farm.query.vgirpc.log;
