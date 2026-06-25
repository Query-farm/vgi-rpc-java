// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

/**
 * Transport implementations of {@link farm.query.vgirpc.transport.RpcTransport}:
 * stdio ({@link farm.query.vgirpc.transport.StdioTransport}), client subprocess
 * ({@link farm.query.vgirpc.transport.SubprocessTransport}), Unix domain socket
 * ({@link farm.query.vgirpc.transport.UnixSocketTransport}), and raw TCP socket
 * ({@link farm.query.vgirpc.transport.TcpSocketTransport} — no auth/TLS, trusted
 * networks only). HTTP lives in {@link farm.query.vgirpc.http}.
 */
package farm.query.vgirpc.transport;
