// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

/**
 * Convenience base class for producer-only stream states. Subclasses implement
 * {@link #produce(OutputCollector, CallContext)}; the input batch is ignored.
 */
public abstract class ProducerState extends StreamState {

    public abstract void produce(OutputCollector out, CallContext ctx);

    @Override
    public final void process(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
        produce(out, ctx);
    }
}
