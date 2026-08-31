// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0
package farm.query.vgirpc.identity;

/** How transport-peer evidence was established. */
public enum IdentityAssurance {
    CRYPTOGRAPHIC_PEER("cryptographic_peer"), LOCAL_DAEMON("local_daemon"), CONFIGURED_PROXY("configured_proxy");
    private final String wireValue;
    IdentityAssurance(String wireValue) { this.wireValue = wireValue; }
    public String wireValue() { return wireValue; }
}
