// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.transport;

/** Unchecked invalid native-Iroh option with portable error dimensions. */
public final class IrohConfigurationException extends IllegalArgumentException implements IrohFailure {
    public IrohConfigurationException(String message) { super(message); }
    @Override public IrohErrorStage stage() { return IrohErrorStage.PARSE; }
    @Override public IrohErrorCategory category() { return IrohErrorCategory.INVALID_INPUT; }
    @Override public IrohDispatchCertainty dispatchCertainty() {
        return IrohDispatchCertainty.NOT_SENT;
    }
}
