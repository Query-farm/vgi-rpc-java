// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

/**
 * Thrown by {@link StateToken#unpack(byte[], byte[], long)} when the token's
 * creation timestamp is older than the configured TTL. Maps to HTTP 400 at the
 * request boundary (matches the Python reference).
 */
public class TokenExpiredException extends IllegalArgumentException {
    public TokenExpiredException(String message) { super(message); }
}
