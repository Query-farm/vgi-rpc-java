// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.errors;

/**
 * An exception that declares its canonical code ({@code vgi_rpc.error_code}).
 *
 * <p>Every error kind the framework defines names its code where the kind is defined, so a
 * classified error never reaches the wire as {@link Code#UNKNOWN}. An exception that implements
 * neither this nor anything else is sent as {@code UNKNOWN}, which is honest: it was not
 * classified.
 */
public interface HasErrorCode {
    /**
     * The canonical code this error is sent with.
     *
     * @return the code; never {@code null}
     */
    Code errorCode();
}
