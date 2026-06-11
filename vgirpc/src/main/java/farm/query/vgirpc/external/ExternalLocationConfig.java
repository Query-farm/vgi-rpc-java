// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.external;

import java.net.URI;
import java.time.Duration;
import java.util.function.Consumer;

/**
 * Configuration for resolving external-location pointer batches (and, in Phase
 * 14, externalising outgoing batches above a threshold).
 *
 * <p>URLs flowing through the fetcher originate from the server or storage
 * layer — but an attacker who compromises a server could still redirect the
 * client to an attacker-controlled host. {@code urlValidator} (default:
 * HTTPS only) is invoked before each fetch; throw
 * {@link IllegalArgumentException} to reject.</p>
 */
public final class ExternalLocationConfig {

    /** Storage backend for the upload side. Null disables externalisation. */
    private final ExternalStorage storage;
    /** Size (in bytes) above which outgoing batches are externalised. */
    private final long thresholdBytes;
    /** Max total bytes the fetcher will download per pointer (safety cap). */
    private final long maxFetchBytes;
    /** Retry attempts on transient fetch errors (caller count, not including first). */
    private final int maxRetries;
    /** Delay between retries. */
    private final Duration retryDelay;
    /** Per-fetch HTTP timeout. */
    private final Duration httpTimeout;
    /** Run before each fetch; throw to reject the URL. */
    private final Consumer<URI> urlValidator;
    /** Max concurrent range requests for a single fetch (1 = no ranges). */
    private final int maxRangeParallelism;
    /** Upload-side compression. {@code null} disables. */
    private final Compression compression;

    private ExternalLocationConfig(Builder b) {
        this.storage = b.storage;
        this.thresholdBytes = b.thresholdBytes;
        this.maxFetchBytes = b.maxFetchBytes;
        this.maxRetries = b.maxRetries;
        this.retryDelay = b.retryDelay;
        this.httpTimeout = b.httpTimeout;
        this.urlValidator = b.urlValidator;
        this.maxRangeParallelism = b.maxRangeParallelism;
        this.compression = b.compression;
    }

    /** Storage backend used to upload outbound batches that reach the size threshold.
     * @return the upload backend, or {@code null} if externalisation is fetch-only. */
    public ExternalStorage storage() { return storage; }
    /** Externalisation cut-over point: outbound batches whose serialised IPC size
     * reaches this value are uploaded and replaced with a pointer batch.
     * @return batch size (bytes) at or above which a batch is externalised. */
    public long thresholdBytes() { return thresholdBytes; }
    /** Safety cap protecting the fetcher against runaway downloads.
     * @return hard cap (bytes) on a fetched body; oversized fetches are rejected. */
    public long maxFetchBytes() { return maxFetchBytes; }
    /** Retry budget for transient fetch errors, not counting the initial attempt.
     * @return number of fetch retries before giving up. */
    public int maxRetries() { return maxRetries; }
    /** Pause inserted between successive fetch attempts.
     * @return delay between fetch retries. */
    public Duration retryDelay() { return retryDelay; }
    /** Timeout applied to each individual HTTP request issued by the fetcher.
     * @return per-request HTTP timeout. */
    public Duration httpTimeout() { return httpTimeout; }
    /** Security hook run before every fetch; it throws {@link IllegalArgumentException}
     * to reject a URL.
     * @return validator applied to each fetch URL before the request is made. */
    public Consumer<URI> urlValidator() { return urlValidator; }
    /** Degree of HTTP range-request parallelism when downloading a single body.
     * @return maximum parallel range requests for a single body ({@code 1} = single request). */
    public int maxRangeParallelism() { return maxRangeParallelism; }
    /** Compression applied to externalised bodies before upload.
     * @return upload compression spec, or {@code null} to upload raw. */
    public Compression compression() { return compression; }

    /** Upload-side compression spec. {@code algorithm} is currently always
     * {@code "zstd"}; {@code level} maps to libzstd compression levels (1–22).
     *
     * @param algorithm compression algorithm identifier; only {@code "zstd"} is recognised
     * @param level libzstd compression level (1–22)
     */
    public record Compression(String algorithm, int level) {
        /** Convenience factory for zstd at libzstd's default level.
         * @return zstd compression at the default level (3). */
        public static Compression zstd() { return new Compression("zstd", 3); }
        /**
         * Convenience factory for zstd at an explicit level.
         *
         * @param level libzstd level 1–22
         * @return zstd compression at the given level
         */
        public static Compression zstd(int level) { return new Compression("zstd", level); }
    }

    /** HTTPS-only URL validator; throws {@link IllegalArgumentException} otherwise.
     * This is the default {@code urlValidator}.
     * @return a validator that rejects every URI whose scheme is not {@code https}. */
    public static Consumer<URI> httpsOnlyValidator() {
        return uri -> {
            String scheme = uri.getScheme();
            if (scheme == null || !scheme.equalsIgnoreCase("https")) {
                throw new IllegalArgumentException("URL must use https scheme: " + uri);
            }
        };
    }

    /** Accepts anything — suitable for tests and trusted-network use.
     * @return a validator that never rejects a URI. */
    public static Consumer<URI> permissiveValidator() {
        return uri -> { /* no-op */ };
    }

    /** Entry point for assembling a config.
     * @return a new {@link Builder} with default thresholds and HTTPS-only validation. */
    public static Builder builder() { return new Builder(); }

    /**
     * Fluent builder for {@link ExternalLocationConfig}. Defaults externalize
     * batches over 1&nbsp;MiB and cap fetches at 512&nbsp;MiB; set a
     * {@link ExternalStorage} backend and override the thresholds as needed.
     */
    public static final class Builder {
        private ExternalStorage storage;
        private long thresholdBytes = 1L << 20;      // 1 MiB
        private long maxFetchBytes = 512L * 1024 * 1024; // 512 MiB safety cap
        private int maxRetries = 2;
        private Duration retryDelay = Duration.ofMillis(500);
        private Duration httpTimeout = Duration.ofSeconds(30);
        private Consumer<URI> urlValidator = httpsOnlyValidator();
        private int maxRangeParallelism = 1; // simple path by default; set >1 to enable ranges
        private Compression compression; // null = upload raw

        /** Upload backend used to externalise large outbound batches.
         * @param s the storage backend; {@code null} (the default) disables externalisation
         * @return this builder */
        public Builder storage(ExternalStorage s) { this.storage = s; return this; }
        /** Size (bytes) at or above which a batch is externalised (default 1 MiB).
         * @param v the externalisation threshold in bytes
         * @return this builder */
        public Builder thresholdBytes(long v) { this.thresholdBytes = v; return this; }
        /** Hard cap (bytes) on a fetched body (default 512 MiB).
         * @param v the maximum fetch size in bytes
         * @return this builder */
        public Builder maxFetchBytes(long v) { this.maxFetchBytes = v; return this; }
        /** Number of fetch retries before failing (default 2).
         * @param n retry count, not including the initial attempt
         * @return this builder */
        public Builder maxRetries(int n) { this.maxRetries = n; return this; }
        /** Delay between fetch retries (default 500&nbsp;ms).
         * @param d the inter-retry delay
         * @return this builder */
        public Builder retryDelay(Duration d) { this.retryDelay = d; return this; }
        /** Per-request HTTP timeout (default 30&nbsp;s).
         * @param d the timeout applied to each HTTP request
         * @return this builder */
        public Builder httpTimeout(Duration d) { this.httpTimeout = d; return this; }
        /** Validator run against each fetch URL (default {@link #httpsOnlyValidator()}).
         * @param v the validator; throw {@link IllegalArgumentException} to reject a URL
         * @return this builder */
        public Builder urlValidator(Consumer<URI> v) { this.urlValidator = v; return this; }
        /** Maximum parallel range requests per body; {@code >1} enables ranged fetch (default 1).
         * @param n the parallelism degree
         * @return this builder */
        public Builder maxRangeParallelism(int n) { this.maxRangeParallelism = n; return this; }
        /** Upload compression spec; {@code null} uploads raw (default).
         * @param c the compression spec, e.g. {@link Compression#zstd()}
         * @return this builder */
        public Builder compression(Compression c) { this.compression = c; return this; }

        /** Freezes the current settings.
         * @return the immutable {@link ExternalLocationConfig}. */
        public ExternalLocationConfig build() { return new ExternalLocationConfig(this); }
    }
}
