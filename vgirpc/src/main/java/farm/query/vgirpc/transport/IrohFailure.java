// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.transport;

/** Structured dimensions common to parse and transport failures. */
public interface IrohFailure {
    IrohErrorStage stage();
    IrohErrorCategory category();
    IrohDispatchCertainty dispatchCertainty();
}
