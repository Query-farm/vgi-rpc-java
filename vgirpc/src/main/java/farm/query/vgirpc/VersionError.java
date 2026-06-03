// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

/**
 * Thrown when a request batch's {@code vgi_rpc.request_version} is missing or does
 * not match the version this server accepts. The server rejects the call rather
 * than risk misinterpreting an incompatible wire layout.
 */
public class VersionError extends RuntimeException {
    /** @param message diagnostic message */
    public VersionError(String message) { super(message); }
}
