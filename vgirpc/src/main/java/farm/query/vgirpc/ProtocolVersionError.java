// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

/**
 * The client's declared application {@code protocol_version} is incompatible
 * with the server's.
 *
 * <p>Distinct from {@link VersionError}, which is about the vgi-rpc FRAMEWORK
 * request version — the envelope. This one is about the APPLICATION surface
 * riding inside it, and the two move independently.</p>
 */
public final class ProtocolVersionError extends RuntimeException {

    /**
     * @param message the directional diagnostic naming both versions and which side to upgrade
     */
    public ProtocolVersionError(String message) {
        super(message);
    }
}
