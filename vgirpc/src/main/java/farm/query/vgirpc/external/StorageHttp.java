// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.external;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Storage HTTP with a stall (idle) timeout and no whole-request deadline.
 *
 * <p>A storage transfer (a pre-signed PUT or GET to R2/S3/GCS, an inbound
 * pointer fetch, a client upload) may legitimately take minutes on a slow link;
 * what must fail is a transfer that has stopped moving. {@link #exchange} runs a
 * request under a watchdog that aborts it only when no bytes have moved for the
 * stall timeout (default 30 s, {@value #ENV_STALL_TIMEOUT}), matching
 * vgi-rpc-python, whose httpx timeouts bound each connect, read and write
 * rather than the request.</p>
 *
 * <p>{@link HttpRequest.Builder#timeout} is not that: it bounds everything up
 * to the response headers, which includes sending the request body, so a
 * 100 MB PUT at 2 MB/s (50 s) fails a 30 s timeout although it is progressing;
 * and it does not bound reading the response body at all. Here:</p>
 * <ul>
 *   <li>the request body is published in 64 KiB chunks, on demand, and every
 *       chunk the client takes is progress (after the last one, the window adds
 *       the time to drain the socket buffers at the rate observed, so the end
 *       of a slow upload is not mistaken for a stall);</li>
 *   <li>the response headers and every response body chunk are progress;</li>
 *   <li>connecting is bounded by the client's connect timeout (use
 *       {@link #newClientBuilder}, 30 s) and by the first stall window.</li>
 * </ul>
 * <p>A stall surfaces as a {@link StallTimeoutException}, an
 * {@link IOException}, so callers that retry transport errors retry it.</p>
 */
public final class StorageHttp {

    /** Environment variable: seconds a storage transfer may move no bytes before
     *  it is abandoned (and retried). Same name in every vgi-rpc SDK. */
    public static final String ENV_STALL_TIMEOUT = "VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS";
    /** Default stall timeout (vgi-rpc-python: 30 s). */
    public static final Duration DEFAULT_STALL_TIMEOUT = Duration.ofSeconds(30);
    /** Default connect timeout (vgi-rpc-python: 30 s). */
    public static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(30);
    /** Request bodies are published in chunks of this size. */
    public static final int CHUNK_BYTES = 64 * 1024;
    /** Floor rate (bytes/s) for the drain window after the last request chunk. */
    public static final long MIN_DRAIN_RATE_BYTES_PER_SECOND = 64 * 1024;

    private static final long BURST_NANOS = TimeUnit.MILLISECONDS.toNanos(250);
    private static final ScheduledThreadPoolExecutor TIMER = newTimer();

    private StorageHttp() {}

    private static ScheduledThreadPoolExecutor newTimer() {
        ScheduledThreadPoolExecutor t = new ScheduledThreadPoolExecutor(1, r -> {
            Thread th = new Thread(r, "vgi-rpc-storage-stall-watchdog");
            th.setDaemon(true);
            return th;
        });
        t.setRemoveOnCancelPolicy(true);
        return t;
    }

    /**
     * The stall timeout from {@value #ENV_STALL_TIMEOUT} in {@code env}
     * (seconds, a positive number, fractions allowed); 30 s when unset.
     *
     * @param env environment mapping
     * @return the stall timeout
     * @throws IllegalArgumentException if the variable is set but is not a positive finite number
     */
    public static Duration stallTimeoutFromEnv(Map<String, String> env) {
        String raw = env.get(ENV_STALL_TIMEOUT);
        if (raw == null || raw.isBlank()) return DEFAULT_STALL_TIMEOUT;
        String text = raw.trim();
        double seconds;
        try {
            if (!text.matches("\\+?(\\d+\\.?\\d*|\\.\\d+)")) throw new NumberFormatException(text);
            seconds = Double.parseDouble(text);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(ENV_STALL_TIMEOUT + " must be a positive number of seconds, got '"
                    + raw + "'");
        }
        if (!(seconds > 0) || Double.isInfinite(seconds)) {
            throw new IllegalArgumentException(ENV_STALL_TIMEOUT + " must be a positive number of seconds, got '"
                    + raw + "'");
        }
        return Duration.ofMillis(Math.max(1, Math.round(seconds * 1000)));
    }

    /**
     * The stall timeout from the process environment.
     *
     * @return the stall timeout
     * @throws IllegalArgumentException if {@value #ENV_STALL_TIMEOUT} is malformed
     */
    public static Duration stallTimeoutFromEnv() {
        return stallTimeoutFromEnv(System.getenv());
    }

    /**
     * A bare client builder for storage hosts: the default connect timeout, no
     * redirects, HTTP/1.1, no authenticator or cookie handler.
     *
     * @return the builder
     */
    public static HttpClient.Builder newClientBuilder() {
        return HttpClient.newBuilder()
                .connectTimeout(DEFAULT_CONNECT_TIMEOUT)
                .followRedirects(HttpClient.Redirect.NEVER)
                .version(HttpClient.Version.HTTP_1_1);
    }

    /** A storage transfer moved no bytes for the stall timeout. */
    public static final class StallTimeoutException extends HttpTimeoutException {
        private static final long serialVersionUID = 1L;

        /**
         * A stall of {@code stall}.
         *
         * @param stall the stall timeout that elapsed
         */
        public StallTimeoutException(Duration stall) {
            super("storage transfer stalled: no bytes moved for " + stall.toMillis() + " ms");
        }
    }

    /** A 2xx response body is larger than the caller's cap. */
    public static final class BodyTooLargeException extends IOException {
        private static final long serialVersionUID = 1L;
        private final boolean declared;

        BodyTooLargeException(String message, boolean declared) {
            super(message);
            this.declared = declared;
        }

        /**
         * Whether the declared size was the problem.
         *
         * @return true when the declared Content-Length was over the cap
         */
        public boolean declared() { return declared; }
    }

    /**
     * A completed exchange.
     *
     * @param status the HTTP status
     * @param headers the response headers
     * @param body the response body (a non-2xx body is truncated at 64 KiB)
     */
    public record Result(int status, HttpHeaders headers, byte[] body) {}

    /**
     * Send one request under the stall watchdog and read its whole response.
     *
     * @param client the client (its connect timeout bounds connecting)
     * @param request the request builder: URI and headers set, no method, body or timeout
     * @param method the HTTP method
     * @param body the request body, or {@code null} for none
     * @param maxBodyBytes the largest 2xx body accepted
     * @param stall the stall timeout
     * @return the result
     * @throws StallTimeoutException when no bytes moved for {@code stall}
     * @throws BodyTooLargeException when a 2xx body is over {@code maxBodyBytes}
     * @throws IOException on any other transport failure (never carrying the URL's query)
     * @throws InterruptedException when interrupted
     */
    public static Result exchange(HttpClient client, HttpRequest.Builder request, String method, byte[] body,
                                  long maxBodyBytes, Duration stall) throws IOException, InterruptedException {
        Watch watch = new Watch(stall.toNanos());
        ChunkPublisher publisher = body == null || body.length == 0 ? null : new ChunkPublisher(body, watch);
        request.method(method, publisher == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.fromPublisher(publisher, body.length));
        java.util.concurrent.atomic.AtomicReference<CappedSubscriber> subscriber =
                new java.util.concurrent.atomic.AtomicReference<>();
        CompletableFuture<HttpResponse<byte[]>> future = client.sendAsync(request.build(), info -> {
            watch.progress();
            boolean ok = info.statusCode() / 100 == 2;
            long declared = info.headers().firstValueAsLong("Content-Length").orElse(-1);
            CappedSubscriber s = new CappedSubscriber(ok ? maxBodyBytes : 64 * 1024, !ok, declared, watch);
            subscriber.set(s);
            return s;
        });
        watch.start(() -> {
            if (publisher != null) publisher.fail(new StallTimeoutException(stall));
            CappedSubscriber s = subscriber.get();
            if (s != null) s.abort(new StallTimeoutException(stall));
            future.cancel(true);
        });
        try {
            HttpResponse<byte[]> resp = future.get();
            return new Result(resp.statusCode(), resp.headers(), resp.body());
        } catch (CancellationException e) {
            if (watch.stalled.get()) throw new StallTimeoutException(stall);
            throw new IOException("storage request cancelled");
        } catch (ExecutionException e) {
            if (watch.stalled.get()) throw new StallTimeoutException(stall);
            Throwable cause = e.getCause();
            while (cause instanceof java.util.concurrent.CompletionException && cause.getCause() != null) {
                cause = cause.getCause();
            }
            if (cause instanceof BodyTooLargeException tooLarge) throw tooLarge;
            if (cause instanceof IOException io) throw io;
            throw new IOException("storage request failed (" + (cause == null ? "unknown" : cause.getClass().getSimpleName())
                    + ")");
        } catch (InterruptedException e) {
            future.cancel(true);
            throw e;
        } finally {
            watch.stop();
        }
    }

    /** The idle timer: one scheduled check at a time, re-armed for the remaining window. */
    static final class Watch {
        private final long stallNanos;
        private final AtomicLong last = new AtomicLong(System.nanoTime());
        private volatile long window;
        private volatile boolean done;
        private volatile ScheduledFuture<?> pending;
        private Runnable onStall;
        final AtomicBoolean stalled = new AtomicBoolean(false);

        Watch(long stallNanos) {
            this.stallNanos = stallNanos;
            this.window = stallNanos;
        }

        long stallNanos() { return stallNanos; }

        void start(Runnable onStall) {
            this.onStall = onStall;
            last.set(System.nanoTime());
            arm(window);
        }

        private void arm(long nanos) {
            if (done) return;
            pending = TIMER.schedule(this::check, Math.max(1, nanos), TimeUnit.NANOSECONDS);
        }

        private void check() {
            if (done) return;
            long idle = System.nanoTime() - last.get();
            long w = window;
            if (idle >= w) {
                if (stalled.compareAndSet(false, true)) {
                    done = true;
                    onStall.run();
                }
                return;
            }
            arm(w - idle);
        }

        void progress() {
            last.set(System.nanoTime());
            window = stallNanos;
        }

        void progressWithWindow(long nanos) {
            last.set(System.nanoTime());
            window = Math.max(stallNanos, nanos);
        }

        void stop() {
            done = true;
            ScheduledFuture<?> p = pending;
            if (p != null) p.cancel(false);
        }
    }

    /** Publishes {@code data} in {@link #CHUNK_BYTES} slices on demand; each slice is progress. */
    static final class ChunkPublisher implements Flow.Publisher<ByteBuffer> {
        private final byte[] data;
        private final Watch watch;
        private volatile Flow.Subscriber<? super ByteBuffer> subscriber;
        private final AtomicLong demand = new AtomicLong();
        private final AtomicBoolean draining = new AtomicBoolean();
        private volatile boolean terminated;
        private int offset;
        private long firstNanos;
        private long burstBytes;
        private long burstEndNanos;

        ChunkPublisher(byte[] data, Watch watch) {
            this.data = data;
            this.watch = watch;
        }

        @Override
        public void subscribe(Flow.Subscriber<? super ByteBuffer> s) {
            // A retry by the client (e.g. a stale pooled connection) subscribes
            // again: start over from the first byte.
            offset = 0;
            firstNanos = 0;
            terminated = false;
            demand.set(0);
            subscriber = s;
            s.onSubscribe(new Flow.Subscription() {
                @Override
                public void request(long n) {
                    if (n <= 0) return;
                    demand.getAndAccumulate(n, (a, b) -> a + b < 0 ? Long.MAX_VALUE : a + b);
                    drain();
                }

                @Override
                public void cancel() {
                    terminated = true;
                }
            });
        }

        private void drain() {
            if (!draining.compareAndSet(false, true)) return;
            try {
                while (!terminated && demand.get() > 0) {
                    long now = System.nanoTime();
                    if (firstNanos == 0) firstNanos = now;
                    if (now - firstNanos <= BURST_NANOS) {
                        burstBytes = offset;
                        burstEndNanos = now;
                    }
                    if (offset >= data.length) {
                        terminated = true;
                        long steadyBytes = offset - burstBytes;
                        long steadyNanos = now - burstEndNanos;
                        double rate = steadyBytes >= 4L * CHUNK_BYTES && steadyNanos > 0
                                ? Math.max(MIN_DRAIN_RATE_BYTES_PER_SECOND, steadyBytes * 1e9 / steadyNanos)
                                : MIN_DRAIN_RATE_BYTES_PER_SECOND;
                        double drainSeconds = Math.min(burstBytes / rate,
                                (double) data.length / MIN_DRAIN_RATE_BYTES_PER_SECOND);
                        watch.progressWithWindow(watch.stallNanos() + (long) (drainSeconds * 1e9));
                        subscriber.onComplete();
                        return;
                    }
                    int n = Math.min(CHUNK_BYTES, data.length - offset);
                    ByteBuffer chunk = ByteBuffer.wrap(data, offset, n).slice().asReadOnlyBuffer();
                    offset += n;
                    demand.decrementAndGet();
                    watch.progress();
                    subscriber.onNext(chunk);
                }
            } finally {
                draining.set(false);
            }
            if (!terminated && demand.get() > 0) drain();
        }

        void fail(Throwable error) {
            if (terminated) return;
            terminated = true;
            Flow.Subscriber<? super ByteBuffer> s = subscriber;
            if (s != null) s.onError(error);
        }
    }

    /** Collects a response body up to a cap; each chunk is progress. */
    static final class CappedSubscriber implements HttpResponse.BodySubscriber<byte[]> {
        private final long cap;
        private final boolean truncate;
        private final long declared;
        private final Watch watch;
        private final CompletableFuture<byte[]> result = new CompletableFuture<>();
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();
        private volatile Flow.Subscription subscription;
        private long total;

        CappedSubscriber(long cap, boolean truncate, long declared, Watch watch) {
            this.cap = cap;
            this.truncate = truncate;
            this.declared = declared;
            this.watch = watch;
        }

        @Override
        public CompletionStage<byte[]> getBody() { return result; }

        @Override
        public void onSubscribe(Flow.Subscription s) {
            subscription = s;
            if (!truncate && declared > cap) {
                s.cancel();
                result.completeExceptionally(new BodyTooLargeException(
                        "body is " + declared + " bytes, over the " + cap + "-byte cap", true));
                return;
            }
            s.request(1);
        }

        @Override
        public void onNext(List<ByteBuffer> items) {
            watch.progress();
            for (ByteBuffer b : items) {
                int n = b.remaining();
                if (total + n > cap) {
                    if (truncate) {
                        int keep = (int) Math.max(0, cap - total);
                        byte[] part = new byte[keep];
                        b.get(part);
                        out.write(part, 0, keep);
                        total = cap;
                        subscription.cancel();
                        result.complete(out.toByteArray());
                        return;
                    }
                    subscription.cancel();
                    result.completeExceptionally(new BodyTooLargeException(
                            "body exceeded the " + cap + "-byte cap", false));
                    return;
                }
                byte[] chunk = new byte[n];
                b.get(chunk);
                out.write(chunk, 0, n);
                total += n;
            }
            subscription.request(1);
        }

        @Override
        public void onError(Throwable throwable) {
            result.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            result.complete(out.toByteArray());
        }

        void abort(Throwable error) {
            Flow.Subscription s = subscription;
            if (s != null) s.cancel();
            result.completeExceptionally(error);
        }
    }
}
