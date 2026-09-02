// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.transport;

/** Portable stage at which an Iroh transport operation failed. */
public enum IrohErrorStage {
    PARSE, BIND, RESOLVE, CONNECT, ALPN, OPEN_STREAM, WRITE, READ, CANCEL, CLOSE, INTERNAL
}
