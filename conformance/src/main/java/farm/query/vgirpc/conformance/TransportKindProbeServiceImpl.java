// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.conformance;

import farm.query.vgirpc.CallContext;

/** Reference implementation of {@link TransportKindProbeService}. */
public final class TransportKindProbeServiceImpl implements TransportKindProbeService {
    @Override
    public String report_transport_kind(CallContext ctx) {
        if (ctx.kind() == null) throw new IllegalStateException("transport kind was not bound");
        return ctx.kind().wireName();
    }
}
