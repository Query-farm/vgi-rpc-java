// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.conformance;

import farm.query.vgirpc.schema.ProtocolName;

/**
 * {@code conformance.Secondary.v1} -- the second application protocol every conformance worker
 * hosts, registered after {@link ConformanceService} through {@code RpcServer.addProtocol}.
 *
 * <p>Normative in the reference's {@code tools/cross-port/specs/MULTI_PROTOCOL_HOSTING.md} §2. It
 * makes three things observable that a single-protocol worker hides:
 *
 * <ul>
 *   <li><b>Routing by pair.</b> {@code echo_string} repeats the name and signature of
 *       {@link ConformanceService#echo_string}; its reply is prefixed, so a mis-route is a wrong
 *       value rather than a coincidentally right one.</li>
 *   <li><b>A per-binding version gate.</b> No {@code @ProtocolVersion} on purpose, while the
 *       primary declares {@code 2.0.0}: a server that gates every call against the primary's
 *       version refuses this protocol's calls.</li>
 *   <li><b>The error model.</b> {@code fail} raises whatever code and kind the caller names, with
 *       fixed details; {@code fail_oversized} raises details over the 4 KiB cap.</li>
 * </ul>
 *
 * <p>Pinned hash: {@value #PROTOCOL_HASH}.
 */
@ProtocolName(Secondary.PROTOCOL_NAME)
public interface Secondary {

    /** The routing key. */
    String PROTOCOL_NAME = "conformance.Secondary.v1";

    /** The pinned canonical digest every port must report for this protocol. */
    String PROTOCOL_HASH = "58557cf1611546ad22d1c379bc3ce1b04166082f78375e9fc959f0086347eab6";

    /** What {@code echo_string} prepends. */
    String ECHO_PREFIX = "secondary:";

    /**
     * Echo with the prefix that makes a mis-route visible.
     *
     * @param value the text to echo
     * @return {@code "secondary:" + value}
     */
    String echo_string(String value);

    /**
     * Always raise: with {@code code}, {@code kind} (absent when empty) and the fixed details --
     * {@code ErrorInfo}, then {@code RetryInfo} when the delay is positive, then a probe type no
     * client knows. A {@code code} outside the closed set is itself refused, with
     * {@code INVALID_ARGUMENT} / {@code invalid_code} and a {@code BadRequest}.
     *
     * @param code a canonical code name
     * @param kind the reason; empty means none
     * @param retry_delay_seconds the {@code RetryInfo} delay; none is sent unless positive
     */
    void fail(String code, String kind, double retry_delay_seconds);

    /** Always raise {@code RESOURCE_EXHAUSTED} / {@code details_oversized} with details over the
     *  4 KiB cap, so the wire carries no details at all. */
    void fail_oversized();
}
