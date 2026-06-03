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
    UNARY("unary"),
    STREAM("stream");

    private final String wireValue;
    MethodType(String wireValue) { this.wireValue = wireValue; }
    public String wireValue() { return wireValue; }
}
