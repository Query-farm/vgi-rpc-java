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

    public ExternalStorage storage() { return storage; }
    public long thresholdBytes() { return thresholdBytes; }
    public long maxFetchBytes() { return maxFetchBytes; }
    public int maxRetries() { return maxRetries; }
    public Duration retryDelay() { return retryDelay; }
    public Duration httpTimeout() { return httpTimeout; }
    public Consumer<URI> urlValidator() { return urlValidator; }
    public int maxRangeParallelism() { return maxRangeParallelism; }
    public Compression compression() { return compression; }

    /** Upload-side compression spec. {@code algorithm} is currently always
     * {@code "zstd"}; {@code level} maps to libzstd compression levels (1–22). */
    public record Compression(String algorithm, int level) {
        public static Compression zstd() { return new Compression("zstd", 3); }
        public static Compression zstd(int level) { return new Compression("zstd", level); }
    }

    /** HTTPS-only URL validator; throws {@link IllegalArgumentException} otherwise. */
    public static Consumer<URI> httpsOnlyValidator() {
        return uri -> {
            String scheme = uri.getScheme();
            if (scheme == null || !scheme.equalsIgnoreCase("https")) {
                throw new IllegalArgumentException("URL must use https scheme: " + uri);
            }
        };
    }

    /** Accepts anything — suitable for tests and trusted-network use. */
    public static Consumer<URI> permissiveValidator() {
        return uri -> { /* no-op */ };
    }

    public static Builder builder() { return new Builder(); }

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

        public Builder storage(ExternalStorage s) { this.storage = s; return this; }
        public Builder thresholdBytes(long v) { this.thresholdBytes = v; return this; }
        public Builder maxFetchBytes(long v) { this.maxFetchBytes = v; return this; }
        public Builder maxRetries(int n) { this.maxRetries = n; return this; }
        public Builder retryDelay(Duration d) { this.retryDelay = d; return this; }
        public Builder httpTimeout(Duration d) { this.httpTimeout = d; return this; }
        public Builder urlValidator(Consumer<URI> v) { this.urlValidator = v; return this; }
        public Builder maxRangeParallelism(int n) { this.maxRangeParallelism = n; return this; }
        public Builder compression(Compression c) { this.compression = c; return this; }

        public ExternalLocationConfig build() { return new ExternalLocationConfig(this); }
    }
}
