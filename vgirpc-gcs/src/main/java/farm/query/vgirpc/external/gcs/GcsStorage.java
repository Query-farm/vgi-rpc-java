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
 */
public final class GcsStorage implements ExternalStorage, ObjectStore, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(GcsStorage.class);

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
            this.storage = StorageOptions.newBuilder()
                    .setTransportOptions(HttpTransportOptions.newBuilder()
                            .setConnectTimeout((int) StorageHttp.DEFAULT_CONNECT_TIMEOUT.toMillis())
                            .setReadTimeout((int) Math.min(Integer.MAX_VALUE, StorageHttp.stallTimeoutFromEnv().toMillis()))
                            .build())
                    .build().getService();
            this.ownsStorage = true;
        }
        this.bucket = b.bucket;
        this.keyPrefix = b.keyPrefix.endsWith("/") ? b.keyPrefix : b.keyPrefix + "/";
        this.signDuration = b.signDuration;
        this.signedUrlHost = b.signedUrlHost;
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
        var signOptions = new ArrayList<Storage.SignUrlOption>();
        signOptions.add(Storage.SignUrlOption.httpMethod(HttpMethod.GET));
        signOptions.add(Storage.SignUrlOption.withV4Signature());
        if (signedUrlHost != null) {
            signOptions.add(Storage.SignUrlOption.withHostName(signedUrlHost));
        }
        URL url = storage.signUrl(
                BlobInfo.newBuilder(bucket, key).build(),
                signDuration.toMillis(),
                TimeUnit.MILLISECONDS,
                signOptions.toArray(Storage.SignUrlOption[]::new));
        try {
            return url.toURI();
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

        /** Build the configured {@link GcsStorage}. */
        public GcsStorage build() { return new GcsStorage(this); }
    }
}
