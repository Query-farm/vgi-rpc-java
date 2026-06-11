// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

/**
 * Thrown when a request batch's {@code vgi_rpc.request_version} is missing or does
 * not match the version this server accepts. The server rejects the call rather
 * than risk misinterpreting an incompatible wire layout.
 */
public class VersionError extends RuntimeException {
    /** Creates the error with a diagnostic message.
     *  @param message diagnostic message describing the version mismatch */
    public VersionError(String message) { super(message); }
}
