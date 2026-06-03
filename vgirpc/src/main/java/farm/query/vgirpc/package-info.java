// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

/**
 * vgi-rpc: a transport-agnostic RPC framework built on Apache Arrow IPC.
 *
 * <p>Services are defined as ordinary Java interfaces; Arrow schemas are derived
 * from method signatures and {@code record} component types, and calls flow over
 * pipe, subprocess, Unix-socket, shared-memory, or HTTP transports as sequential
 * Arrow IPC streams. This package holds the framework core:</p>
 *
 * <ul>
 *   <li>{@link farm.query.vgirpc.RpcServer} — dispatches unary and streaming calls.</li>
 *   <li>{@link farm.query.vgirpc.RpcConnection} — client-side typed proxy factory.</li>
 *   <li>{@link farm.query.vgirpc.RpcStream} / {@link farm.query.vgirpc.StreamState} /
 *       {@link farm.query.vgirpc.ProducerState} / {@link farm.query.vgirpc.ExchangeState}
 *       — streaming primitives.</li>
 *   <li>{@link farm.query.vgirpc.CallContext} / {@link farm.query.vgirpc.AuthContext}
 *       — request-scoped context injected into method implementations.</li>
 * </ul>
 *
 * <p>This is the Java port of the Python reference implementation; the wire format
 * is byte-compatible so the two interoperate.</p>
 */
package farm.query.vgirpc;
