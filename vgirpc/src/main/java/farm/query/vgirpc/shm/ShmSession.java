// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.shm;

import farm.query.vgirpc.wire.Metadata;

import java.util.Map;

/**
 * Per-connection shared-memory state. The C++ client creates and owns the
 * segment and advertises {@code vgi_rpc.shm_segment_name} +
 * {@code vgi_rpc.shm_segment_size} on its {@code init} requests; the worker is
 * an attacher. One {@code ShmSession} lives for the duration of an
 * {@link farm.query.vgirpc.RpcServer#serve} call and is closed when the
 * connection ends (munmap + close fd, never unlink).
 *
 * <p>Attach is lazy and best-effort: if the segment can't be mapped, the
 * session stays disabled and the connection silently uses inline transfer.</p>
 */
public final class ShmSession implements AutoCloseable {

    private static final boolean DEBUG = System.getenv("VGI_RPC_SHM_DEBUG") != null;
    // Per-connection utilization summary (one log line at close — negligible
    // overhead, unlike DEBUG's per-batch prints). Implied by DEBUG.
    private static final boolean STATS = DEBUG || System.getenv("VGI_RPC_SHM_STATS") != null;

    private Shm segment;
    private String segmentName;     // name of the currently-attached segment, if any
    private boolean unavailable;    // shm is impossible on this runtime — permanent

    /** The attached segment, or {@code null} if shm is not in use on this connection. */
    public Shm segment() { return segment; }

    /**
     * Attach to the advertised segment when a request carries the segment
     * name/size. A pooled worker process serves successive client connections
     * over one {@code serve} loop, and the engine creates a fresh segment (new
     * name) per connection — so when the advertised name <em>changes</em> we must
     * detach the old mapping and re-attach the new one. Reusing the previous
     * attachment would read a stale (already-unlinked) segment. Mirrors the Go
     * worker's {@code shmConnState.ensure}.
     */
    public void attachIfAdvertised(Map<String, String> meta) {
        if (unavailable || meta == null) return;
        // Honor the kill-switch even if a peer advertises a segment anyway: a
        // disabled worker never attaches, so inbound resolve / outbound writes
        // stay dormant and the connection uses inline transfer.
        if (Shm.disabledByEnv()) { unavailable = true; detach(); return; }
        String name = meta.get(Metadata.SHM_SEGMENT_NAME);
        String size = meta.get(Metadata.SHM_SEGMENT_SIZE);
        if (name == null || size == null) return;          // not advertised on this request
        if (segment != null && name.equals(segmentName)) return;  // already attached to this one
        // A new or changed segment was advertised — drop the old mapping and (re)attach.
        detach();
        try {
            // null on Java 21 / non-POSIX / native-access-denied: shm unavailable,
            // stay disabled and use inline transfer. A real attach failure on a
            // capable JVM throws and is caught below.
            Shm attached = ShmFactory.attach(name, Long.parseLong(size));
            if (attached == null) {
                unavailable = true;   // runtime can't map shm at all; stop trying
                return;
            }
            segment = attached;
            segmentName = name;
        } catch (Exception e) {
            // Per-segment failure: fall back to inline for now, but a later
            // (different) advertised segment may still attach — don't latch.
            if (DEBUG) {
                System.err.println("[vgi-shm] attach failed for " + name + ": " + e
                        + " — using inline transfer");
            }
        }
    }

    /** Detach the current segment (munmap + close fd, never unlink), if any. */
    private void detach() {
        if (segment != null) {
            segment.close();
            segment = null;
            segmentName = null;
        }
    }

    /**
     * Detach from the segment (munmap + close fd, never unlink — the client owns
     * the segment). Optionally emits a utilization summary when shm stats are
     * enabled. Idempotent.
     */
    @Override
    public void close() {
        if (segment != null) {
            if (STATS) {
                long out = segment.outShmBatches, outFb = segment.outInlineEligibleBatches;
                long in = segment.inShmBatches, inInline = segment.inInlineDataBatches;
                System.err.printf(
                    "[vgi-shm] conn closed %s: outbound shm=%d/%d batches (%.1f%%, %.1f MiB), inline-eligible-fallback=%d; "
                        + "inbound shm=%d/%d batches (%.1f%%, %.1f MiB), inline-data=%d%n",
                    segment.name(),
                    out, out + outFb, pct(out, out + outFb), segment.outShmBytes / 1048576.0, outFb,
                    in, in + inInline, pct(in, in + inInline), segment.inShmBytes / 1048576.0, inInline);
                double busy = segment.busyNs / 1e6, idle = segment.idleNs / 1e6;
                System.err.printf(
                    "[vgi-shm] timeline %s: worker-busy=%.1f ms (resolve=%.1f process=%.1f emit=%.1f), "
                        + "worker-idle/client+handoff=%.1f ms  ->  worker %.0f%% / client+handoff %.0f%%%n",
                    segment.name(), busy,
                    segment.resolveNs / 1e6, segment.processNs / 1e6, segment.emitNs / 1e6, idle,
                    pct((long) (busy * 1000), (long) ((busy + idle) * 1000)),
                    pct((long) (idle * 1000), (long) ((busy + idle) * 1000)));
            }
            detach();
        }
    }

    private static double pct(long n, long d) { return d == 0 ? 100.0 : 100.0 * n / d; }
}
