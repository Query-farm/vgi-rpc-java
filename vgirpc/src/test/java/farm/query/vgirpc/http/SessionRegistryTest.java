// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class SessionRegistryTest {

    private static final String PRINCIPAL = "\1domain\0principal";

    @Test
    @Timeout(10)
    void explicitCloseWaitsForTheInFlightLease() throws Exception {
        SessionRegistry registry = new SessionRegistry(60);
        ClosingState state = new ClosingState();
        SessionRegistry.Entry entry = registry.open(state, null, PRINCIPAL);
        assertFalse(entry.lock().isHeldByCurrentThread(), "public open must preserve its unlocked contract");
        SessionRegistry.Entry lease = registry.acquire(entry.sessionId(), PRINCIPAL);

        try (var executor = Executors.newSingleThreadExecutor()) {
            Future<Boolean> close = executor.submit(
                    () -> registry.close(entry.sessionId(), PRINCIPAL));
            awaitQueued(entry);
            assertFalse(state.closed.await(100, TimeUnit.MILLISECONDS),
                    "state closed while a request still held its lease");

            registry.release(lease);
            assertTrue(close.get(2, TimeUnit.SECONDS));
            assertTrue(state.closed.await(1, TimeUnit.SECONDS));
        }

        assertEquals(1, state.closeCalls.get());
        assertNull(registry.acquire(entry.sessionId(), PRINCIPAL),
                "a removed session must never be reacquired");
        registry.shutdown();
        assertEquals(1, state.closeCalls.get(), "shutdown must not double-close state");
    }

    @Test
    @Timeout(10)
    void expiryWaitsForTheInFlightLease() throws Exception {
        SessionRegistry registry = new SessionRegistry(1);
        ClosingState state = new ClosingState();
        SessionRegistry.Entry entry = registry.open(state, 1L, PRINCIPAL);
        SessionRegistry.Entry lease = registry.acquire(entry.sessionId(), PRINCIPAL);
        awaitExpired(entry);

        try (var executor = Executors.newSingleThreadExecutor()) {
            Future<Integer> reap = executor.submit(registry::drainExpired);
            awaitQueued(entry);
            assertFalse(state.closed.await(100, TimeUnit.MILLISECONDS),
                    "reaper closed state while a request still held its lease");

            registry.release(lease);
            assertEquals(1, reap.get(2, TimeUnit.SECONDS));
            assertTrue(state.closed.await(1, TimeUnit.SECONDS));
        }
        assertEquals(1, state.closeCalls.get());
    }

    @Test
    @Timeout(10)
    void shutdownReturnsWhileTheInFlightLeaseDefersClose() throws Exception {
        SessionRegistry registry = new SessionRegistry(60);
        ClosingState state = new ClosingState();
        SessionRegistry.Entry entry = registry.openLease(state, null, PRINCIPAL);
        SessionRegistry.Entry lease = entry;

        try (var executor = Executors.newSingleThreadExecutor()) {
            Future<?> shutdown = executor.submit(registry::shutdown);
            shutdown.get(1, TimeUnit.SECONDS);
            assertFalse(state.closed.await(100, TimeUnit.MILLISECONDS),
                    "shutdown closed state while a request still held its lease");
            assertNull(registry.acquire(entry.sessionId(), PRINCIPAL),
                    "shutdown must remove an active entry before returning");

            registry.release(lease);
            assertTrue(state.closed.await(1, TimeUnit.SECONDS));
        }
        assertEquals(1, state.closeCalls.get());
    }

    private static void awaitQueued(SessionRegistry.Entry entry) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (!entry.lock().hasQueuedThreads() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertTrue(entry.lock().hasQueuedThreads(), "closure did not queue behind the active lease");
    }

    private static void awaitExpired(SessionRegistry.Entry entry) throws InterruptedException {
        while (System.currentTimeMillis() / 1000 <= entry.expiresAtSeconds()) {
            Thread.sleep(20);
        }
    }

    private static final class ClosingState implements AutoCloseable {
        final AtomicInteger closeCalls = new AtomicInteger();
        final CountDownLatch closed = new CountDownLatch(1);

        @Override public void close() {
            closeCalls.incrementAndGet();
            closed.countDown();
        }
    }
}
