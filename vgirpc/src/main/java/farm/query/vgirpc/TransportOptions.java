// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.shm.Shm;
import farm.query.vgirpc.shm.ShmFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The {@code __transport_options__} RPC method — a framework-level capability
 * handshake, parallel to {@link Introspect} ({@code __describe__}). The client
 * calls it once per worker (before {@code init}) to discover which transport
 * features the worker supports; SHM (and, later, compression / AEAD) is used
 * only when both peers advertise support.
 *
 * <p>Capabilities ride as request/response <em>metadata</em> under the
 * {@link #CAP_PREFIX} namespace (not as batch rows) — consistent with how the
 * SHM segment is already advertised, and decode-safe (the params batch stays
 * empty, so the server's kwargs decode is a no-op). Each capability is one
 * {@code vgi_rpc.transport.<name>} key with a string value; unknown keys are
 * ignored, so the set is open-ended.</p>
 */
public final class TransportOptions {

    public static final String METHOD_NAME = "__transport_options__";

    /** Metadata-key namespace for transport capabilities (request and response). */
    public static final String CAP_PREFIX = "vgi_rpc.transport.";

    /** Shared-memory side-channel capability key; value is {@code "true"}/{@code "false"}. */
    public static final String CAP_SHM = CAP_PREFIX + "shm";

    private TransportOptions() {}

    /**
     * This worker's transport capabilities. SHM is offered only when the FFM
     * implementation is actually loadable on this runtime — true on JDK&nbsp;&ge;&nbsp;22
     * (the {@code java22} multi-release overlay), false on Java&nbsp;21 / non-POSIX —
     * and not turned off via the {@code VGI_RPC_SHM_DISABLE} kill-switch.
     */
    public static Map<String, String> workerCapabilities() {
        Map<String, String> caps = new LinkedHashMap<>();
        boolean shm = ShmFactory.available() && !Shm.disabledByEnv();
        caps.put(CAP_SHM, Boolean.toString(shm));
        return caps;
    }

    /** Extract the {@code vgi_rpc.transport.*} capabilities from a metadata map. */
    public static Map<String, String> parse(Map<String, String> meta) {
        Map<String, String> caps = new LinkedHashMap<>();
        if (meta != null) {
            for (Map.Entry<String, String> e : meta.entrySet()) {
                if (e.getKey().startsWith(CAP_PREFIX)) {
                    caps.put(e.getKey().substring(CAP_PREFIX.length()), e.getValue());
                }
            }
        }
        return caps;
    }
}
