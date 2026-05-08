// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

/**
 * Convenience base class for producer-only stream states. Subclasses implement
 * {@link #produce(OutputCollector, CallContext)}; the input batch's row data
 * is ignored, but its {@link AnnotatedBatch#customMetadata custom_metadata}
 * is exposed via the {@link #produce(AnnotatedBatch, OutputCollector,
 * CallContext)} overload for producers that need per-tick framework
 * messages (dynamic filter updates, cancel requests, etc.).
 */
public abstract class ProducerState extends StreamState {

    public abstract void produce(OutputCollector out, CallContext ctx);

    /**
     * Per-tick variant that exposes the framework's tick {@code custom_metadata}.
     * Default delegates to {@link #produce(OutputCollector, CallContext)} so
     * existing producers keep working without change. Producers that consume
     * dynamic filter updates (e.g. Top-N tightening) override this method.
     */
    public void produce(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
        produce(out, ctx);
    }

    @Override
    public final void process(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
        produce(input, out, ctx);
    }
}
