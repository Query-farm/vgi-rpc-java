// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.log.Level;
import farm.query.vgirpc.log.Message;

import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Per-request context injected into method implementations that declare a
 * {@link CallContext} parameter. Provides authentication info, transport
 * metadata, and a callback for emitting client-directed log batches.
 */
public final class CallContext {

    private final AuthContext auth;
    private final Consumer<Message> emitClientLog;
    private final Map<String, Object> transportMetadata;
    private final String serverId;
    private final String methodName;
    private final String protocolName;
    private final String requestId;

    public CallContext(AuthContext auth,
                       Consumer<Message> emitClientLog,
                       Map<String, Object> transportMetadata,
                       String serverId,
                       String methodName,
                       String protocolName,
                       String requestId) {
        this.auth = auth != null ? auth : AuthContext.ANONYMOUS;
        this.emitClientLog = emitClientLog;
        this.transportMetadata = transportMetadata != null
                ? Collections.unmodifiableMap(transportMetadata)
                : Collections.emptyMap();
        this.serverId = serverId;
        this.methodName = methodName;
        this.protocolName = protocolName;
        this.requestId = requestId != null ? requestId : "";
    }

    public AuthContext auth() { return auth; }
    public Map<String, Object> transportMetadata() { return transportMetadata; }
    public String serverId() { return serverId; }
    public String methodName() { return methodName; }
    public String protocolName() { return protocolName; }
    public String requestId() { return requestId; }

    public void clientLog(Level level, String msg) {
        emitClientLog.accept(new Message(level, msg, null));
    }

    public void clientLog(Level level, String msg, Map<String, Object> extra) {
        emitClientLog.accept(new Message(level, msg, extra));
    }

    public void emitClientLog(Message m) { emitClientLog.accept(m); }
}
