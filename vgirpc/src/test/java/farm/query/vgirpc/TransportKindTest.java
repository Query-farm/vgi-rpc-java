// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class TransportKindTest {

    interface PingService { long ping(long value); }

    static final class PingImpl implements PingService {
        @Override public long ping(long value) { return value; }
    }

    @Test
    void failedServeStartHookLeavesKindUnsetAndRetries() {
        RpcServer server = new RpcServer(PingService.class, new PingImpl());
        AtomicInteger calls = new AtomicInteger();
        server.setServeStartHook(kind -> {
            if (calls.incrementAndGet() == 1) throw new IllegalStateException("transient");
        });

        assertThrows(IllegalStateException.class,
                () -> server.notifyTransport(TransportKind.HTTP));
        assertNull(server.transportKind());

        server.notifyTransport(TransportKind.HTTP);
        server.notifyTransport(TransportKind.HTTP);
        assertEquals(2, calls.get());
        assertEquals(TransportKind.HTTP, server.transportKind());
    }

    @Test
    void legacyCallContextConstructorRemainsSourceCompatible() {
        CallContext legacy = new CallContext(null, ignored -> {}, Map.of(),
                "server", "method", "protocol", "request");
        assertNull(legacy.kind());

        CallContext current = new CallContext(null, ignored -> {}, Map.of(),
                "server", "method", "protocol", "request", TransportKind.TCP);
        assertEquals(TransportKind.TCP, current.kind());
    }
}
