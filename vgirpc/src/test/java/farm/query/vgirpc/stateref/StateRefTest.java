// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.stateref;

import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.external.ObjectMissingException;
import farm.query.vgirpc.external.ObjectStore;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Language-local StateRef, argument-reference, budget and object-cache tests (STATE_REF_V1.md). */
final class StateRefTest {

    private static final byte[] KEY = new byte[32];
    private static final AuthContext ALICE = new AuthContext("conformance", true, "alice", Map.of());
    private static final AuthContext BOB = new AuthContext("conformance", true, "bob", Map.of());
    private static final String PROTOCOL = "Svc";

    /** In-memory object store with counters and fault injection. */
    static final class MemStore implements ObjectStore {
        final Map<String, byte[]> objects = new ConcurrentHashMap<>();
        final AtomicInteger puts = new AtomicInteger();
        final AtomicInteger gets = new AtomicInteger();
        volatile boolean failGets;
        volatile byte[] substitute;

        @Override public String putObject(byte[] data) {
            puts.incrementAndGet();
            String id = "obj-" + puts.get();
            objects.put(id, data.clone());
            return id;
        }

        @Override public byte[] get(String objectId, long maxBytes) throws Exception {
            gets.incrementAndGet();
            if (failGets) throw new java.io.IOException("503 from storage");
            byte[] v = objects.get(objectId);
            if (v == null) throw new ObjectMissingException("gone");
            return substitute != null ? substitute : v;
        }
    }

    private static StateRefRuntime runtime(MemStore store, Long maxRequestBytes, AtomicLong clock, ObjectCache cache) {
        return StateRefRuntime.builder()
                .tokenKey(KEY)
                .tokenTtlSeconds(60)
                .storage(() -> store)
                .maxRequestBytes(maxRequestBytes)
                .cache(cache)
                .clockSeconds(clock::get)
                .build();
    }

    private static byte[] random(int n, long seed) {
        byte[] out = new byte[n];
        new Random(seed).nextBytes(out);
        return out;
    }

    private static byte[] encode(StateRefRuntime rt, StateRef ref, AuthContext auth, byte[] callId) {
        return StateRefs.encodingForStream(rt, auth, PROTOCOL, "m", callId, 0, ref::toBytes);
    }

    private static byte[] callId(int b) {
        byte[] id = new byte[16];
        id[0] = (byte) b;
        return id;
    }

    @Test
    void codecTagsAndBounds() {
        assertEquals(TokenCodec.CODEC_RAW, TokenCodec.pack(new byte[] {1, 2, 3})[0], "a tiny payload stays raw");
        byte[] big = "abc".repeat(5000).getBytes(StandardCharsets.UTF_8);
        byte[] packed = TokenCodec.pack(big);
        assertEquals(TokenCodec.CODEC_ZSTD, packed[0]);
        assertArrayEquals(big, TokenCodec.unpack(packed, 1 << 20));
        assertThrows(TokenCodec.TokenCodecException.class, () -> TokenCodec.unpack(packed, 100),
                "the declared frame size is checked against the bound");
        assertThrows(TokenCodec.TokenCodecException.class, () -> TokenCodec.unpack(new byte[] {0x7f, 1}, 100));
        assertThrows(TokenCodec.TokenCodecException.class, () -> TokenCodec.unpack(new byte[] {0x02, 1}, 100),
                "this SDK never writes DEFLATE, so it refuses it");
    }

    @Test
    void inlineRoundTripAndSerializationOutsideAMintDoesNotCache() {
        StateRefScope init = new StateRefScope(null, ALICE, PROTOCOL, "m", null, true);
        byte[] value = "hello".getBytes(StandardCharsets.UTF_8);
        StateRef ref = init.stateRef(value);
        assertEquals("memory", ref.form());
        byte[] rendered = ref.toBytes();
        assertEquals(0x01, rendered[0]);
        assertNull(ref.encoded(), "outside a mint the form is not decided");
        StateRef back = StateRef.fromBytes(rendered);
        assertEquals("inline", back.form());
        assertArrayEquals(value, init.resolve(back));
    }

    @Test
    void memoAndMisuse() {
        StateRefScope init = new StateRefScope(null, ALICE, PROTOCOL, "m", null, true);
        byte[] a = {1, 2, 3};
        assertSame(init.stateRef(a), init.stateRef(new byte[] {1, 2, 3}), "byte-equal values share a ref");
        init.endInit();
        StateRefMisuseError e = assertThrows(StateRefMisuseError.class, () -> init.stateRef(a));
        assertEquals("state_ref_misuse", e.errorKind());
        assertEquals(farm.query.vgirpc.errors.Code.FAILED_PRECONDITION, e.errorCode());
        assertTrue(e.getMessage().contains("(Svc.m)"));
    }

    @Test
    void storedRefIsPutOnceAndReEmittedVerbatim() {
        MemStore store = new MemStore();
        AtomicLong clock = new AtomicLong(1_700_000_000L);
        ObjectCache cache = new ObjectCache(1 << 20);
        StateRefRuntime rt = runtime(store, null, clock, cache);
        byte[] value = random(65536, 1);
        StateRef ref = new StateRefScope(rt, ALICE, PROTOCOL, "m", null, true).stateRef(value);
        byte[] first = encode(rt, ref, ALICE, callId(1));
        byte[] second = encode(rt, ref, ALICE, callId(1));
        assertEquals("stored", ref.form());
        assertArrayEquals(first, second, "a stored ref re-serializes to identical bytes");
        assertEquals(1, store.puts.get(), "and is put exactly once");

        // Another replica (fresh cache) resolves it with one get, then from its cache.
        ObjectCache other = new ObjectCache(1 << 20);
        StateRefRuntime replica = runtime(store, null, clock, other);
        StateRefScope turn = new StateRefScope(replica, ALICE, PROTOCOL, "m", callId(1), false);
        assertArrayEquals(value, turn.resolve(StateRef.fromBytes(first)));
        assertArrayEquals(value, turn.resolve(StateRef.fromBytes(first)));
        assertEquals(1, store.gets.get());
    }

    @Test
    void storedTagsAreScopedAndExpire() {
        MemStore store = new MemStore();
        AtomicLong clock = new AtomicLong(1_700_000_000L);
        StateRefRuntime rt = runtime(store, null, clock, new ObjectCache(0));
        StateRef ref = new StateRefScope(rt, ALICE, PROTOCOL, "m", null, true).stateRef(random(40000, 2));
        byte[] encoded = encode(rt, ref, ALICE, callId(1));
        StateRef presented = StateRef.fromBytes(encoded);

        assertThrows(InvalidStateRefError.class,
                () -> new StateRefScope(rt, ALICE, PROTOCOL, "m", callId(2), false).resolve(presented),
                "another stream's tag");
        assertThrows(InvalidStateRefError.class,
                () -> new StateRefScope(rt, BOB, PROTOCOL, "m", callId(1), false).resolve(presented),
                "another principal");
        assertThrows(InvalidStateRefError.class,
                () -> new StateRefScope(rt, ALICE, "Other", "m", callId(1), false).resolve(presented),
                "another protocol");
        AuthContext bound = new AuthContext("conformance", true, "alice", Map.of("peer_evidence_binding", "b1"));
        assertThrows(InvalidStateRefError.class,
                () -> new StateRefScope(rt, bound, PROTOCOL, "m", callId(1), false).resolve(presented),
                "another peer-evidence binding");
        assertEquals(0, store.gets.get(), "no scope failure fetches anything");

        clock.addAndGet(61);
        InvalidStateRefError expired = assertThrows(InvalidStateRefError.class,
                () -> new StateRefScope(rt, ALICE, PROTOCOL, "m", callId(1), false).resolve(presented));
        assertEquals("StateRef expired", expired.getMessage());
        assertEquals(0, store.gets.get(), "an expired tag is refused before any get");
    }

    @Test
    void storedObjectIntegrityAndStorageErrors() {
        MemStore store = new MemStore();
        AtomicLong clock = new AtomicLong(1_700_000_000L);
        StateRefRuntime rt = runtime(store, null, clock, new ObjectCache(0));
        StateRef ref = new StateRefScope(rt, ALICE, PROTOCOL, "m", null, true).stateRef(random(40000, 3));
        StateRef presented = StateRef.fromBytes(encode(rt, ref, ALICE, callId(1)));
        StateRefScope turn = new StateRefScope(rt, ALICE, PROTOCOL, "m", callId(1), false);

        store.substitute = random(10, 4);
        assertThrows(InvalidStateRefError.class, () -> turn.resolve(presented), "size mismatch");
        store.substitute = null;
        store.failGets = true;
        assertEquals("storage_unavailable",
                assertThrows(StorageUnavailableError.class, () -> turn.resolve(presented)).errorKind());
        store.failGets = false;
        store.objects.clear();
        assertEquals("state_ref_missing",
                assertThrows(StateRefMissingError.class, () -> turn.resolve(presented)).errorKind());
        assertThrows(InvalidStateRefError.class,
                () -> turn.resolve(StateRef.fromBytes(new byte[] {0x07, 1, 2})), "unknown form");
    }

    @Test
    void storeOverTheFetchCapIsRefused() {
        MemStore store = new MemStore();
        StateRefRuntime rt = StateRefRuntime.builder().tokenKey(KEY).storage(() -> store)
                .maxFetchBytes(1000).cache(new ObjectCache(0)).build();
        StateRef ref = new StateRefScope(rt, ALICE, PROTOCOL, "m", null, true).stateRef(random(40000, 5));
        assertThrows(StateRefTooLargeError.class, () -> encode(rt, ref, ALICE, callId(1)));
        assertEquals(0, store.puts.get());
    }

    @Test
    void noStorageKeepsEverythingInline() {
        StateRefRuntime rt = StateRefRuntime.builder().tokenKey(KEY).build();
        StateRef ref = new StateRefScope(rt, ALICE, PROTOCOL, "m", null, true).stateRef(random(65536, 6));
        encode(rt, ref, ALICE, callId(1));
        assertEquals("inline", ref.form());
    }

    @Test
    void turnBudgetInlineLimitAndCursorRule() {
        assertNull(StateRefRuntime.turnBudget(null, 0));
        assertEquals((32768L - 4096) * 100 / 134 - 500, StateRefRuntime.turnBudget(32768L, 500));
        StateRefRuntime unbounded = StateRefRuntime.builder().tokenKey(KEY).build();
        assertEquals(16384, unbounded.inlineBytes(0));
        assertNull(unbounded.checkCursor(10_000_000, 0, PROTOCOL, "m"));

        StateRefRuntime warn = StateRefRuntime.builder().tokenKey(KEY).maxRequestBytes(32768L).build();
        assertEquals(((32768L - 4096) * 100 / 134) / 4, warn.inlineBytes(0));
        assertNull(warn.checkCursor(1000, 0, PROTOCOL, "m"));
        String warning = warn.checkCursor(40000, 0, PROTOCOL, "m");
        assertNotNull(warning);
        assertTrue(warning.startsWith("cursor_large:"));

        StateRefRuntime error = StateRefRuntime.builder().tokenKey(KEY).maxRequestBytes(32768L)
                .cursorRule(StateRefRuntime.CursorRule.ERROR).build();
        CursorTooLargeError e = assertThrows(CursorTooLargeError.class,
                () -> error.checkCursor(40000, 0, PROTOCOL, "m"));
        assertTrue(e.getMessage().contains("ctx.state_ref()"));

        StateRefRuntime floor = StateRefRuntime.builder().tokenKey(KEY).maxRequestBytes(9000L)
                .cursorRule(StateRefRuntime.CursorRule.ERROR).build();
        assertTrue(!floor.cursorRuleEnabled());
        assertNull(floor.checkCursor(1_000_000, 0, PROTOCOL, "m"), "below the floor the rule is off");
        StateRefRuntime tiny = StateRefRuntime.builder().tokenKey(KEY).maxRequestBytes(4096L).build();
        assertEquals(0, tiny.inlineBytes(0), "the inline limit floors at 0");

        assertThrows(IllegalArgumentException.class,
                () -> StateRefRuntime.inlineBytesFromEnv(Map.of(StateRefRuntime.STATE_REF_INLINE_BYTES_ENV, "0")));
        assertEquals(16384, StateRefRuntime.inlineBytesFromEnv(Map.of()));
    }

    @Test
    void argumentReferences() {
        MemStore store = new MemStore();
        byte[] payload = "an argument".getBytes(StandardCharsets.UTF_8);
        String id = store.putObject(payload);
        String sha = HexFormat.of().formatHex(StateRefs.sha256(payload));
        byte[] ref = ArgumentRefs.mint(KEY, ALICE, payload.length, sha, id);
        ObjectCache cache = new ObjectCache(1 << 20);

        assertEquals(new ArgumentRefs.ArgumentRef(payload.length, sha, id), ArgumentRefs.open(KEY, ALICE, ref));
        assertArrayEquals(payload, ArgumentRefs.resolve(ref, payload.length, sha, KEY, ALICE, store, 1 << 20, cache));
        assertThrows(InvalidArgumentsRefError.class,
                () -> ArgumentRefs.resolve(ref, payload.length, sha, KEY, BOB, store, 1 << 20, cache));
        AuthContext bound = new AuthContext("conformance", true, "alice", Map.of("peer_evidence_binding", "b1"));
        assertThrows(InvalidArgumentsRefError.class, () -> ArgumentRefs.open(KEY, bound, ref),
                "refs do not cross peer-evidence bindings");
        assertThrows(InvalidArgumentsRefError.class,
                () -> ArgumentRefs.resolve(ref, payload.length + 1, sha, KEY, ALICE, store, 1 << 20, cache));
        assertThrows(InvalidArgumentsRefError.class,
                () -> ArgumentRefs.resolve(ref, payload.length, sha, KEY, ALICE, null, 1 << 20, cache));
        assertThrows(InvalidStateRefError.class,
                () -> new StateRefScope(StateRefRuntime.builder().tokenKey(KEY).storage(() -> store).build(),
                        ALICE, StateRefs.SERVER_SCOPE, "m", callId(1), false)
                        .resolve(StateRef.fromBytes(concat(new byte[] {0x02}, ref))),
                "an argument ref is not a StateRef tag");

        byte[] other = "something else".getBytes(StandardCharsets.UTF_8);
        String otherId = store.putObject(other);
        store.objects.put(otherId, "tampered bytes".getBytes(StandardCharsets.UTF_8));
        String otherSha = HexFormat.of().formatHex(StateRefs.sha256(other));
        byte[] otherRef = ArgumentRefs.mint(KEY, ALICE, other.length, otherSha, otherId);
        assertThrows(ArgumentsRefIntegrityError.class,
                () -> ArgumentRefs.resolve(otherRef, other.length, otherSha, KEY, ALICE, store, 1 << 20, cache));
        store.objects.remove(otherId);
        assertThrows(ArgumentsRefMissingError.class,
                () -> ArgumentRefs.resolve(otherRef, other.length, otherSha, KEY, ALICE, store, 1 << 20, cache));
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    @Test
    void objectCacheSingleFlightBoundAndAge() throws Exception {
        AtomicLong now = new AtomicLong(1_000_000L);
        ObjectCache cache = new ObjectCache(100, 86_400, now::get);
        AtomicInteger fetches = new AtomicInteger();
        CountDownLatch release = new CountDownLatch(1);
        Thread[] threads = new Thread[8];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> cache.getOrFetch("a", () -> {
                fetches.incrementAndGet();
                release.await();
                return new byte[60];
            }));
            threads[i].start();
        }
        Thread.sleep(200);
        release.countDown();
        for (Thread t : threads) t.join(10_000);
        assertEquals(1, fetches.get(), "concurrent resolves share one fetch");
        assertTrue(cache.getOrFetch("a", () -> new byte[60]).hit());

        cache.getOrFetch("b", () -> new byte[60]);
        ObjectCache.Stats stats = cache.stats();
        assertTrue(stats.residentBytes() <= 100);
        assertEquals(1, stats.evictions());
        assertTrue(!cache.contains("a"), "the bound evicted the least recently used");

        assertTrue(cache.getOrFetch("b", () -> new byte[60]).hit());
        cache.ageEntries(86_401);
        assertTrue(!cache.getOrFetch("b", () -> new byte[60]).hit(), "an entry older than a day is a miss");

        cache.getOrFetch("huge", () -> new byte[500]);
        assertTrue(!cache.contains("huge"), "a value over the bound is returned but not retained");
        assertThrows(IllegalArgumentException.class,
                () -> ObjectCache.defaultBytes(Map.of(ObjectCache.OBJECT_CACHE_BYTES_ENV, "-1")));
        assertEquals(0, ObjectCache.defaultBytes(Map.of(ObjectCache.OBJECT_CACHE_BYTES_ENV, "0")));
    }
}
