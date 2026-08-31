// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0
package farm.query.vgirpc.identity;

/** Outcome of one transport-peer identity provider. */
public enum PeerIdentityStatus {
    OFF("off"), NOT_APPLICABLE("not_applicable"), AVAILABLE("available"),
    UNAVAILABLE("unavailable"), PERMISSION_DENIED("permission_denied"),
    NO_MATCH("no_match"), INVALID("invalid"), UNTRUSTED_PROXY("untrusted_proxy");

    private final String wireValue;
    PeerIdentityStatus(String wireValue) { this.wireValue = wireValue; }
    public String wireValue() { return wireValue; }
}
