// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

/**
 * Convenience base for exchange (bi-directional) streams. Subclasses must emit
 * exactly one output batch per call and may not call {@link OutputCollector#finish()}.
 */
public abstract class ExchangeState extends StreamState {

    /**
     * Handle one input batch and emit exactly one output batch via {@code out}.
     *
     * @param input the client-supplied input batch for this tick
     * @param out collector that must receive exactly one data batch
     * @param ctx request-scoped call context (auth, metadata, logging)
     */
    public abstract void exchange(AnnotatedBatch input, OutputCollector out, CallContext ctx);

    /** {@inheritDoc} Routes the tick to {@link #exchange(AnnotatedBatch, OutputCollector, CallContext)}. */
    @Override
    public final void process(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
        exchange(input, out, ctx);
    }
}
