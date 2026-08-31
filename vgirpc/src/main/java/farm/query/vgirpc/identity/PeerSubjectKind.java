// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0
package farm.query.vgirpc.identity;

/** Semantic kind of a transport peer's subject. */
public enum PeerSubjectKind {
    USER("user"), TAGGED_NODE("tagged_node"), WORKLOAD("workload"), ENDPOINT("endpoint"), UNKNOWN("unknown");
    private final String wireValue;
    PeerSubjectKind(String wireValue) { this.wireValue = wireValue; }
    public String wireValue() { return wireValue; }
}
