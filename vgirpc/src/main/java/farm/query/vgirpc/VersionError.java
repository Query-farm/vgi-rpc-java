// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

/**
 * Thrown when a request's {@code vgi_rpc.request_version} does not match the
 * version this server accepts. The server rejects the call rather than risk
 * misinterpreting an incompatible wire layout.
 */
public class VersionError extends RuntimeException {
    public VersionError(String message) { super(message); }
}
