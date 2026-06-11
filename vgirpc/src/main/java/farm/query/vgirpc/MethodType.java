// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

/**
 * The kind of an RPC method: {@link #UNARY} (one request batch, one result batch)
 * or {@link #STREAM} (an initial exchange followed by lockstep ticks). Each constant
 * carries the {@code wireValue()} token used in {@code __describe__} responses, which
 * matches the Python reference.
 */
public enum MethodType {
    /** Single params batch in, one result/error batch out. */
    UNARY("unary"),
    /** Lockstep streaming (producer or exchange) until end-of-stream. */
    STREAM("stream");

    private final String wireValue;
    MethodType(String wireValue) { this.wireValue = wireValue; }
    /**
     * The token identifying this method type in {@code __describe__} responses.
     *
     * @return the wire string for this method type ({@code "unary"} / {@code "stream"})
     */
    public String wireValue() { return wireValue; }
}
