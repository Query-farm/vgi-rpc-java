// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0
package farm.query.vgirpc.identity;

/** Stability promised by a peer subject identifier. */
public enum SubjectStability {
    STABLE("stable"), LOGIN("login"), NONE("none");
    private final String wireValue;
    SubjectStability(String wireValue) { this.wireValue = wireValue; }
    public String wireValue() { return wireValue; }
}
