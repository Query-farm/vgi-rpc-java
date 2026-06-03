// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

/**
 * Jetty 12 based HTTP transport. Maps RPC onto {@code POST /vgi/{method}} (unary),
 * {@code .../init} and {@code .../exchange} (streaming). Streaming state is held
 * stateless server-side: an HMAC-signed {@link farm.query.vgirpc.http.StateToken}
 * rides in Arrow custom metadata between calls. Includes the
 * {@link farm.query.vgirpc.http.HttpServer}, request pre-handling, and the
 * {@link farm.query.vgirpc.http.Authenticator} hook.
 */
package farm.query.vgirpc.http;
