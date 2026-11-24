// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.external.gcs;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.HttpMethod;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.http.HttpTransportOptions;
import farm.query.vgirpc.external.StorageHttp;
import farm.query.vgirpc.external.ExternalStorage;
import farm.query.vgirpc.external.ObjectMissingException;
import farm.query.vgirpc.external.ObjectStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link ExternalStorage} backed by Google Cloud Storage. Uploads via the
 * synchronous {@code Storage} client using Application Default Credentials
 * (or a caller-supplied {@link Storage}) and returns a V4 signed GET URL.
 *
 * <p>Also the protocol-3.4 object API ({@link ObjectStore}): {@link #putObject} creates
 * {@code <keyPrefix><uuid>.bin} (only if absent) and the key is the {@code object_id}. GCS
 * cannot verify a SHA-256 at storage; readers verify size and digest themselves. Objects carry
 * no TTL: set a bucket lifecycle rule of at least one day on the prefix.</p>
 *
 * <p>{@link #fromEnv(java.util.Map)} reads the direct-storage environment (contract v1 §5):
 * {@value #ENV_BUCKET}, {@value #ENV_PREFIX}, {@value #ENV_ENDPOINT} (an emulator or private
 * endpoint; signed URLs then use its host) and {@value #ENV_CREDENTIALS_FILE} (else
 * Application Default Credentials: {@code GOOGLE_APPLICATION_CREDENTIALS}, workload identity,
 * the metadata server).</p>
 */
public final class GcsStorage implements ExternalStorage, ObjectStore, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(GcsStorage.class);

    /** Bucket name (required for the {@code gcs} backend). */
    public static final String ENV_BUCKET = "VGI_STORAGE_GCS_BUCKET";
    /** Object name prefix (default empty). */
    public static final String ENV_PREFIX = "VGI_STORAGE_GCS_PREFIX";
    /** Custom endpoint (emulator / private), e.g. {@code http://localhost:4443}; unset = Google. */
    public static final String ENV_ENDPOINT = "VGI_STORAGE_GCS_ENDPOINT";
    /** A service-account key file; unset = Application Default Credentials. */
    public static final String ENV_CREDENTIALS_FILE = "VGI_STORAGE_GCS_CREDENTIALS_FILE";
    /** Signed URL lifetime in seconds (default 3600). */
    public static final String ENV_URL_TTL = "VGI_STORAGE_URL_TTL_SECONDS";
    /** {@code 1} = check the bucket at startup instead of on first use. */
    public static final String ENV_VERIFY = "VGI_STORAGE_GCS_VERIFY";
    /** {@code 1} = allow an {@code http://} endpoint (refused otherwise). */
    public static final String ENV_ALLOW_PLAIN_HTTP = "VGI_STORAGE_ALLOW_PLAIN_HTTP";

    /**
     * Build from the direct-storage environment.
     *
     * @param env environment mapping
     * @return the storage
     * @throws IllegalArgumentException if the bucket is missing, a variable is malformed, or the
     *     credentials file cannot be read
     */
    public static GcsStorage fromEnv(java.util.Map<String, String> env) {
        return fromEnv(env, "rpc");
    }

    /**
     * As {@link #fromEnv(java.util.Map)}, for one key area: objects go under
     * {@code <VGI_STORAGE_GCS_PREFIX><area>/} ({@code rpc} for results and StateRef / argument
     * objects, {@code fs} for function storage).
     *
     * @param env  environment mapping
     * @param area the key area
     * @return the storage
     * @throws IllegalArgumentException if the bucket is missing or a variable is malformed
     */
    public static GcsStorage fromEnv(java.util.Map<String, String> env, String area) {
        String bucket = trimmed(env, ENV_BUCKET);
        if (bucket == null) throw new IllegalArgumentException(ENV_BUCKET + " is required for the gcs storage backend");
        if (area == null || !area.matches("[a-z0-9-]+")) throw new IllegalArgumentException("bad key area: " + area);
        String prefix = java.util.Objects.requireNonNullElse(trimmed(env, ENV_PREFIX), "");
        if (!prefix.isEmpty() && !prefix.endsWith("/")) prefix += "/";
        Builder b = builder(bucket).keyPrefix(prefix + area + "/");
        String endpoint = trimmed(env, ENV_ENDPOINT);
        if (endpoint != null) {
            URI uri;
            try {
                uri = new URI(endpoint);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(ENV_ENDPOINT + " is not a URL: '" + endpoint + "'");
            }
            if (uri.getScheme() == null || uri.getHost() == null
                    || !(uri.getScheme().equals("http") || uri.getScheme().equals("https"))) {
                throw new IllegalArgumentException(ENV_ENDPOINT + " must be an http:// or https:// URL, got '"
                        + endpoint + "'");
            }
            if (uri.getScheme().equals("http") && !java.util.Set.of("1", "true", "yes", "on")
                    .contains(java.util.Objects.requireNonNullElse(trimmed(env, ENV_ALLOW_PLAIN_HTTP), "0")
                            .toLowerCase(java.util.Locale.ROOT))) {
                throw new IllegalArgumentException(ENV_ENDPOINT + " is plain http: set " + ENV_ALLOW_PLAIN_HTTP
                        + "=1 to allow it (emulators and private-network stores only)");
            }
            b.endpoint(endpoint);
        }
        String file = trimmed(env, ENV_CREDENTIALS_FILE);
        if (file != null) b.credentialsFile(java.nio.file.Path.of(file));
        String ttl = trimmed(env, ENV_URL_TTL);
        if (ttl != null) {
            long seconds;
            try {
                seconds = Long.parseLong(ttl);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(ENV_URL_TTL + " must be a positive integer, got '" + ttl + "'");
            }
            if (seconds < 1 || seconds > 7 * 24 * 3600) {
                throw new IllegalArgumentException(ENV_URL_TTL + " must be between 1 and 604800, got " + seconds);
            }
            b.signDuration(Duration.ofSeconds(seconds));
        }
        b.stallTimeout(StorageHttp.stallTimeoutFromEnv(env));
        return b.build();
    }

    private static String trimmed(java.util.Map<String, String> env, String name) {
        String v = env.get(name);
        return v == null || v.isBlank() ? null : v.strip();
    }

    private final Storage storage;
    private final boolean ownsStorage;
    private final String bucket;
    private final String keyPrefix;
    private final Duration signDuration;
    private final String signedUrlHost;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Start building a {@code GcsStorage} that uploads into the given bucket.
     *
     * @param bucket destination GCS bucket name
     * @return a fresh {@link Builder}
     */
    public static Builder builder(String bucket) { return new Builder(bucket); }

    private GcsStorage(Builder b) {
        if (b.storage != null) {
            this.storage = b.storage;
            this.ownsStorage = false;
        } else {
            // Per-read (idle) timeouts as vgi-rpc-python's storage HTTP: 30 s connect,
            // the stall timeout per read; the client sets no whole-transfer deadline.
            Duration stall = b.stallTimeout != null ? b.stallTimeout : StorageHttp.stallTimeoutFromEnv();
            StorageOptions.Builder options = StorageOptions.newBuilder()
                    .setTransportOptions(HttpTransportOptions.newBuilder()
                            .setConnectTimeout((int) StorageHttp.DEFAULT_CONNECT_TIMEOUT.toMillis())
                            .setReadTimeout((int) Math.min(Integer.MAX_VALUE, stall.toMillis()))
                            .build());
            // A custom endpoint is the API host and, unless signedUrlHost says otherwise,
            // the host of the signed URLs.
            if (b.endpoint != null) options.setHost(b.endpoint);
            if (b.credentialsFile != null) {
                try (java.io.InputStream in = java.nio.file.Files.newInputStream(b.credentialsFile)) {
                    options.setCredentials(com.google.auth.oauth2.GoogleCredentials.fromStream(in));
                } catch (java.io.IOException e) {
                    throw new IllegalArgumentException("cannot read GCS credentials file " + b.credentialsFile
                            + " (" + e.getClass().getSimpleName() + ")");
                }
            }
            this.storage = options.build().getService();
            this.ownsStorage = true;
        }
        this.bucket = b.bucket;
        this.keyPrefix = b.keyPrefix.isEmpty() || b.keyPrefix.endsWith("/") ? b.keyPrefix : b.keyPrefix + "/";
        this.signDuration = b.signDuration;
        // Signed URLs for a custom endpoint carry its scheme too (an http:// emulator), which the
        // client's own host default drops in favour of https://.
        this.signedUrlHost = b.signedUrlHost != null ? b.signedUrlHost
                : b.storage == null && b.endpoint != null ? b.endpoint.replaceAll("/+$", "") : null;
    }

    /**
     * Create a blob at {@code <keyPrefix><uuid>.arrow} with content type
     * {@code application/vnd.apache.arrow.stream}, then return a V4 signed GET
     * URL valid for the configured {@link Builder#signDuration duration}.
     *
     * @param body full Arrow IPC stream bytes
     * @param contentEncoding stored as the blob's {@code Content-Encoding}
     *        (e.g. {@code "zstd"}); ignored when {@code null}
     * @return a short-lived signed GET URL the peer can fetch
     * @throws IllegalStateException if the signer returns a URL that cannot be parsed as a {@link URI}
     */
    @Override
    public URI upload(byte[] body, String contentEncoding) {
        String key = keyPrefix + UUID.randomUUID() + ".arrow";
        BlobInfo.Builder blob = BlobInfo.newBuilder(bucket, key)
                .setContentType("application/vnd.apache.arrow.stream");
        if (contentEncoding != null) blob.setContentEncoding(contentEncoding);
        storage.create(blob.build(), body);
        return signGet(key, signDuration);
    }

    /**
     * A {@code urlValidator} for inbound pointers: accepts only URLs on the host, port, scheme
     * and object prefix this store signs for, built on first use from a locally signed sample.
     *
     * @return the validator; it throws {@link IllegalArgumentException} for any other URL
     */
    public java.util.function.Consumer<URI> urlValidator() {
        return new java.util.function.Consumer<>() {
            private volatile java.util.function.Consumer<URI> pinned;

            @Override
            public void accept(URI uri) {
                java.util.function.Consumer<URI> v = pinned;
                if (v == null) {
                    URI sample = signGet(keyPrefix + "vgi-pin-sample", Duration.ofMinutes(1));
                    String path = sample.getRawPath();
                    String dir = path.substring(0, path.length() - "vgi-pin-sample".length());
                    String host = sample.getPort() < 0 ? sample.getHost() : sample.getHost() + ":" + sample.getPort();
                    v = farm.query.vgirpc.vendor.BlobVendor.pinnedHostsValidator(java.util.List.of(host),
                            "http".equalsIgnoreCase(sample.getScheme()),
                            java.util.List.of(sample.getScheme() + "://" + sample.getRawAuthority() + dir));
                    pinned = v;
                }
                v.accept(uri);
            }
        };
    }

    private URI signGet(String key, Duration ttl) {
        var signOptions = new ArrayList<Storage.SignUrlOption>();
        signOptions.add(Storage.SignUrlOption.httpMethod(HttpMethod.GET));
        signOptions.add(Storage.SignUrlOption.withV4Signature());
        if (signedUrlHost != null) {
            signOptions.add(Storage.SignUrlOption.withHostName(signedUrlHost));
        }
        URL url = storage.signUrl(
                BlobInfo.newBuilder(bucket, key).build(),
                ttl.toMillis(),
                TimeUnit.MILLISECONDS,
                signOptions.toArray(Storage.SignUrlOption[]::new));
        try {
            URI signed = url.toURI();
            // The client always writes https://; a V4 signature covers the host, not the
            // scheme, so an http:// emulator endpoint gets its own scheme back.
            if (signedUrlHost != null && signedUrlHost.startsWith("http://") && "https".equals(signed.getScheme())) {
                signed = URI.create("http" + signed.toString().substring("https".length()));
            }
            return signed;
        } catch (URISyntaxException e) {
            // The URL came from the GCS signer — it should not fail URI parsing.
            throw new IllegalStateException("GCS signer returned unparseable URL", e);
        }
    }

    /**
     * Store {@code data} under a fresh key and return the key.
     *
     * @param data the bytes to store
     * @return the object key (the {@code object_id})
     */
    @Override
    public String putObject(byte[] data) {
        String key = keyPrefix + UUID.randomUUID().toString().replace("-", "") + ".bin";
        storage.create(BlobInfo.newBuilder(bucket, key).setContentType("application/octet-stream").build(),
                data, Storage.BlobTargetOption.doesNotExist());
        return key;
    }

    /**
     * Fetch the blob a key names, refusing more than {@code maxBytes}.
     *
     * @param objectId the object key
     * @param maxBytes the largest object accepted
     * @return the object bytes
     * @throws ObjectMissingException when the blob does not exist
     * @throws java.io.IOException when the blob is over the cap
     */
    @Override
    public byte[] get(String objectId, long maxBytes) throws ObjectMissingException, java.io.IOException {
        com.google.cloud.storage.Blob blob = storage.get(com.google.cloud.storage.BlobId.of(bucket, objectId));
        if (blob == null) throw new ObjectMissingException("GCS object is gone");
        Long size = blob.getSize();
        if (size != null && size > maxBytes) {
            throw new java.io.IOException("GCS object is " + size + " bytes, over max_fetch_bytes (" + maxBytes + ")");
        }
        byte[] out = blob.getContent();
        if (out.length > maxBytes) throw new java.io.IOException("GCS object exceeds max_fetch_bytes (" + maxBytes + ")");
        return out;
    }

    /**
     * Check the bucket: fails with an actionable message when it does not exist, access is
     * denied, or the endpoint is unreachable.
     *
     * @throws IllegalStateException if the bucket is not usable
     */
    public void verify() {
        String where = "GCS bucket '" + bucket + "'";
        try {
            if (storage.get(bucket) == null) {
                throw new IllegalStateException(where + " does not exist: create it (and lifecycle rules: 1 day on "
                        + "rpc/, 2 days on fs/), or fix " + ENV_BUCKET + " / " + ENV_ENDPOINT);
            }
        } catch (com.google.cloud.storage.StorageException e) {
            if (e.getCode() == 403 || e.getCode() == 401) {
                throw new IllegalStateException(where + ": access denied: the credentials need storage.buckets.get, "
                        + "storage.objects.get and storage.objects.create (check " + ENV_CREDENTIALS_FILE
                        + " / Application Default Credentials)");
            }
            if (e.getCode() == 404) {
                throw new IllegalStateException(where + " does not exist: fix " + ENV_BUCKET);
            }
            throw new IllegalStateException(where + " is not usable (" + (e.getCode() > 0 ? "HTTP " + e.getCode()
                    : e.getClass().getSimpleName()) + "): check " + ENV_ENDPOINT + " and the network");
        }
    }

    /**
     * Delete an object, best effort: failures are ignored (a lifecycle rule removes leftovers).
     *
     * @param objectId the object name
     */
    public void deleteObject(String objectId) {
        try {
            storage.delete(com.google.cloud.storage.BlobId.of(bucket, objectId));
        } catch (RuntimeException e) {
            LOG.debug("GCS delete of an object failed ({})", e.getClass().getSimpleName());
        }
    }

    /**
     * Close the underlying {@link Storage} client, but only when it was created
     * internally — a caller-supplied client (via {@link Builder#storage}) is left
     * open. Idempotent.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true) && ownsStorage) {
            try { storage.close(); } catch (Exception e) { LOG.warn("GCS Storage close failed", e); }
        }
    }

    /** Fluent builder for {@link GcsStorage}. */
    public static final class Builder {
        private final String bucket;
        private String keyPrefix = "vgi-rpc/";
        private Duration signDuration = Duration.ofHours(1);
        private String signedUrlHost;
        private Storage storage;
        private String endpoint;
        private java.nio.file.Path credentialsFile;
        private Duration stallTimeout;

        Builder(String bucket) { this.bucket = bucket; }

        /** Key prefix for uploaded blobs; a trailing slash is added if missing (default {@code "vgi-rpc/"}). */
        public Builder keyPrefix(String keyPrefix) { this.keyPrefix = keyPrefix; return this; }
        /** Validity window of the signed GET URLs (default one hour). */
        public Builder signDuration(Duration signDuration) { this.signDuration = signDuration; return this; }
        /**
         * Override only the host used in generated signed URLs. This is useful
         * when uploads use an emulator or private API endpoint but recipients
         * must receive URLs for a public gateway. By default the supplied
         * {@link Storage} client's configured host is used.
         */
        public Builder signedUrlHost(String signedUrlHost) {
            this.signedUrlHost = signedUrlHost;
            return this;
        }
        /**
         * Supply a pre-configured GCS client (e.g. for custom project/credentials).
         * A client provided here is not closed by {@link GcsStorage#close()}; when
         * omitted, Application Default Credentials are used and the client is owned.
         */
        public Builder storage(Storage storage) { this.storage = storage; return this; }

        /** Custom API endpoint (an emulator such as fake-gcs-server, or a private endpoint),
         *  e.g. {@code http://localhost:4443}; also the signed URLs' host unless
         *  {@link #signedUrlHost} is set. Ignored with a supplied {@link #storage}. */
        public Builder endpoint(String endpoint) { this.endpoint = endpoint; return this; }
        /** A service-account key file (default Application Default Credentials). Ignored with a
         *  supplied {@link #storage}. */
        public Builder credentialsFile(java.nio.file.Path file) { this.credentialsFile = file; return this; }
        /** Per-read stall timeout (default {@value StorageHttp#ENV_STALL_TIMEOUT}, else 30&nbsp;s).
         *  Ignored with a supplied {@link #storage}. */
        public Builder stallTimeout(Duration stallTimeout) { this.stallTimeout = stallTimeout; return this; }

        /** Build the configured {@link GcsStorage}. */
        public GcsStorage build() { return new GcsStorage(this); }
    }
}
