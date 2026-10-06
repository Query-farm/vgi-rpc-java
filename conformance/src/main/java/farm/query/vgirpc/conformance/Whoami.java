// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.conformance;

import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.schema.ProtocolName;

/**
 * {@code conformance.Whoami.v1} -- hosted only by the grant worker (IDENTITY_CONFORMANCE_FIXTURE.md
 * §10): reports what authentication decided, so a test can read back how a bearer was accepted.
 *
 * <p>Pinned hash: {@value #PROTOCOL_HASH}.
 */
@ProtocolName(Whoami.PROTOCOL_NAME)
public interface Whoami {

    /** The routing key. */
    String PROTOCOL_NAME = "conformance.Whoami.v1";

    /** The pinned canonical digest every port must report for this protocol. */
    String PROTOCOL_HASH = "a280333ba72432020e162cab388a78355969a30aa74f0665ad9d2932d7a10b8f";

    /**
     * The caller's {@code AuthContext} as compact JSON with sorted keys:
     * {@code {"authenticated":bool,"claims":{...},"domain":str,"principal":str}}, with
     * {@code ""} for an absent domain or principal.
     *
     * @param ctx the call context the framework injects (not on the wire)
     * @return the JSON text
     */
    String whoami(CallContext ctx);
}
