// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

/**
 * Convenience base for exchange (bi-directional) streams. Subclasses must emit
 * exactly one output batch per call and may not call {@link OutputCollector#finish()}.
 */
public abstract class ExchangeState extends StreamState {

    public abstract void exchange(AnnotatedBatch input, OutputCollector out, CallContext ctx);

    @Override
    public final void process(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
        exchange(input, out, ctx);
    }
}
