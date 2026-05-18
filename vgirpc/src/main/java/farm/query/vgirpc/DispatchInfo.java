// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import java.util.Map;

/**
 * Metadata passed to {@link DispatchHook} callbacks around RPC dispatch.
 *
 * <p>Mutable so the dispatcher can fill in fields that are not known at the
 * start of the call (e.g. {@code httpStatus}, {@code cancelled}). Hook
 * implementations should read fields only inside the hook callbacks.
 */
public final class DispatchInfo {
    public String method = "";
    public String methodType = "unary";
    public String serverId = "";
    public String protocol = "";
    public String protocolHash = "";
    public String protocolVersion = "";
    public String requestId = "";
    public String principal = "";
    public String authDomain = "";
    public boolean authenticated;
    public String remoteAddr = "";
    public int httpStatus;
    /** Self-contained Arrow IPC stream of the request batch (unary + stream init only). */
    public byte[] requestData;
    /** Stream lifecycle identifier (32-char lowercase hex); empty on unary. */
    public String streamId = "";
    public boolean cancelled;
    public Map<String, ?> transportMetadata;

    /** Sticky-session id (hex), or null when no session was bound. */
    public String sessionId;
    /** Sticky-session action: {@code "none" | "resume" | "open" | "close"}. */
    public String sessionAction;
}
