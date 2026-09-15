// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.identity;

import farm.query.vgirpc.HasErrorKind;

/**
 * The caller may not introspect.
 *
 * <p>Definitive: a caller may cache this. Authentication is not the same capability as
 * introspection -- a deployment where any valid credential may introspect lets any user test
 * guesses of any other user's credential at unlimited rate, and resolve a stolen one to its
 * owner.
 *
 * <p>A {@link SecurityException}, which is this port's permission-denied shape, and deliberately
 * not an {@link IllegalArgumentException}: refusing the caller and failing to resolve the
 * subject are different answers and a caller branches on the difference.
 */
public final class IntrospectionRefusedError extends SecurityException implements HasErrorKind {

    /** The stable wire category for this refusal. */
    public static final String ERROR_KIND = "introspection_refused";

    /**
     * Refuse the caller.
     *
     * @param message why, in terms of the caller only -- never the subject credential
     */
    public IntrospectionRefusedError(String message) {
        super(message);
    }

    @Override
    public String errorKind() {
        return ERROR_KIND;
    }
}
