// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

/**
 * The error model: a canonical code, an open reason, and typed details.
 *
 * <p>Every EXCEPTION batch carries three layers, adopted from gRPC's {@code google.rpc.Status}
 * (WIRE_PROTOCOL.md §8):</p>
 *
 * <ul>
 *   <li><b>Code</b> — {@code vgi_rpc.error_code}, a {@link farm.query.vgirpc.errors.Code} sent by
 *       <em>name</em> ({@code "UNAVAILABLE"}). Closed set; what generic handling keys on.</li>
 *   <li><b>Reason</b> — {@code vgi_rpc.error_kind}, open, unique within the raising protocol;
 *       what a client branches on.</li>
 *   <li><b>Details</b> — {@code vgi_rpc.error_details}, a JSON array of typed objects from the
 *       fixed catalog in {@link farm.query.vgirpc.errors.ErrorDetail}, capped at 4 KiB and dropped
 *       <em>whole</em> when over.</li>
 * </ul>
 *
 * <p>Servers throw a {@link farm.query.vgirpc.errors.StatusError} (or any exception implementing
 * {@link farm.query.vgirpc.errors.HasErrorCode} / {@link farm.query.vgirpc.errors.HasErrorDetails}
 * / {@link farm.query.vgirpc.HasErrorKind}); clients read
 * {@link farm.query.vgirpc.RpcError#errorCode()} and its typed accessors.</p>
 */
package farm.query.vgirpc.errors;
