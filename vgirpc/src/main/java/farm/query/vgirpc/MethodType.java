// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

public enum MethodType {
    UNARY("unary"),
    STREAM("stream");

    private final String wireValue;
    MethodType(String wireValue) { this.wireValue = wireValue; }
    public String wireValue() { return wireValue; }
}
