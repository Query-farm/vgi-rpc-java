// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

/** Kind of RPC method: a single request/response, or a streaming exchange. */
public enum MethodType {
    /** Single params batch in, one result/error batch out. */
    UNARY("unary"),
    /** Lockstep streaming (producer or exchange) until end-of-stream. */
    STREAM("stream");

    private final String wireValue;
    MethodType(String wireValue) { this.wireValue = wireValue; }
    /** @return the wire string for this method type ({@code "unary"} / {@code "stream"}). */
    public String wireValue() { return wireValue; }
}
