// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import java.nio.charset.StandardCharsets;

/**
 * The protocol-name grammar, shared by the routing key and the HTTP path segment.
 *
 * <p>{@code name ::= [A-Za-z_] [A-Za-z0-9_.]*}, at most 255 bytes UTF-8. The major version is
 * part of the name ({@code vgi_rpc.Reflection.v1}), following gRPC's AIP-185, Kubernetes API
 * groups and D-Bus: an incompatible major is then a <em>routing</em> failure rather than a parse
 * failure, and {@code .v1} and {@code .v2} can be served side by side while clients migrate.
 *
 * <p>A candidate is checked against the grammar <em>before</em> it is looked up, so a
 * request-supplied string never reaches an error message, a log field or a metric label.
 */
public final class ProtocolNames {

    private ProtocolNames() {}

    /** Maximum length of a protocol name, in UTF-8 bytes. */
    public static final int MAX_BYTES = 255;

    /**
     * The prefix reserved for protocols the framework itself defines ({@code vgi_rpc.Reflection.v1},
     * {@code vgi_rpc.Identity.v1}).
     *
     * <p>An application protocol that claimed it could shadow one of those and make it unroutable
     * on the server that hosts both -- reflection in particular, which is the one endpoint a
     * confused client reaches for to find out what went wrong.
     */
    public static final String RESERVED_PREFIX = "vgi_rpc.";

    /**
     * Whether {@code name} claims the framework-reserved prefix.
     *
     * <p>Not part of {@link #isValid}: the framework's own names are valid and must stay routable,
     * so this is a separate question asked only where an <em>application</em> declares a name.
     *
     * @param name the candidate, already known to match the grammar
     * @return whether it is under {@link #RESERVED_PREFIX}
     */
    public static boolean isReserved(String name) {
        return name != null && name.startsWith(RESERVED_PREFIX);
    }

    /**
     * Whether {@code name} can be a protocol name at all.
     *
     * <p>Deliberately a predicate rather than a thrower: every caller here answers an unroutable
     * name with the same 404 it answers an unknown one with, and neither wants the candidate
     * echoed back.
     *
     * @param name the candidate, as received
     * @return whether it matches the grammar and the length bound
     */
    public static boolean isValid(String name) {
        if (name == null || name.isEmpty()) return false;
        char first = name.charAt(0);
        if (!(isAsciiLetter(first) || first == '_')) return false;
        for (int i = 1; i < name.length(); i++) {
            char c = name.charAt(i);
            if (!(isAsciiLetter(c) || (c >= '0' && c <= '9') || c == '_' || c == '.')) return false;
        }
        return name.getBytes(StandardCharsets.UTF_8).length <= MAX_BYTES;
    }

    private static boolean isAsciiLetter(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
    }
}
