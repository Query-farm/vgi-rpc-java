// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.transport;

/** Portable category for an Iroh transport failure. */
public enum IrohErrorCategory {
    INVALID_INPUT, UNSUPPORTED, UNAVAILABLE, TIMEOUT, PROTOCOL, CONNECTION_RESET,
    CANCELLED, AUTHENTICATION, RESOURCE_EXHAUSTED, INTERNAL
}
