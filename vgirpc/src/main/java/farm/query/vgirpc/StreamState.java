// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

/**
 * Base class for stream state objects. Subclasses implement
 * {@link #process(AnnotatedBatch, OutputCollector, CallContext)} which is
 * called once per input batch. State mutates in-place across calls.
 */
public abstract class StreamState {

    /**
     * Process one input batch and emit output via the collector.
     * For producer streams the input is a zero-row tick batch and may be ignored;
     * call {@link OutputCollector#finish()} to end the stream. For exchange streams
     * the input carries real data and exactly one output batch must be emitted.
     */
    public abstract void process(AnnotatedBatch input, OutputCollector out, CallContext ctx);

    /** Hook invoked when the client cancels the stream. Default no-op. */
    public void onCancel(CallContext ctx) {}
}
