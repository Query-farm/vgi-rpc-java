// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.transport;

import java.io.IOException;

/** Checked native-Iroh failure with portable retry-safety information. */
public final class IrohTransportException extends IOException implements IrohFailure {
    private final IrohErrorStage stage;
    private final IrohErrorCategory category;
    private final IrohDispatchCertainty dispatchCertainty;

    public IrohTransportException(String message, IrohErrorStage stage,
            IrohErrorCategory category, IrohDispatchCertainty dispatchCertainty) {
        this(message, stage, category, dispatchCertainty, null);
    }

    public IrohTransportException(String message, IrohErrorStage stage,
            IrohErrorCategory category, IrohDispatchCertainty dispatchCertainty, Throwable cause) {
        super(message, cause);
        this.stage = stage;
        this.category = category;
        this.dispatchCertainty = dispatchCertainty;
    }

    @Override public IrohErrorStage stage() { return stage; }
    @Override public IrohErrorCategory category() { return category; }
    @Override public IrohDispatchCertainty dispatchCertainty() { return dispatchCertainty; }
}
