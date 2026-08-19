// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

/** Coarse transport selected for a server dispatch. */
public enum TransportKind {
    /** Standard input/output or another pipe-like local transport. */
    PIPE("pipe"),
    /** HTTP request/response transport. */
    HTTP("http"),
    /** Unix-domain socket transport. */
    UNIX("unix"),
    /** Raw TCP socket transport. */
    TCP("tcp");

    private final String wireName;

    TransportKind(String wireName) { this.wireName = wireName; }

    /** Lowercase language-neutral name used by conformance and diagnostics. */
    public String wireName() { return wireName; }
}
