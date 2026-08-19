// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.conformance;

import farm.query.vgirpc.CallContext;

/** Versionless opt-in service used only by transport-kind conformance. */
public interface TransportKindProbeService {
    /** Return the lowercase kind observed by this dispatch. */
    String report_transport_kind(CallContext ctx);
}
