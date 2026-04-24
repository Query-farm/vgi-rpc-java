// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.external.gcs;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.HttpMethod;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import farm.query.vgirpc.external.ExternalStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link ExternalStorage} backed by Google Cloud Storage. Uploads via the
 * synchronous {@code Storage} client using Application Default Credentials
 * (or a caller-supplied {@link Storage}) and returns a V4 signed GET URL.
 */
public final class GcsStorage implements ExternalStorage, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(GcsStorage.class);

    private final Storage storage;
    private final boolean ownsStorage;
    private final String bucket;
    private final String keyPrefix;
    private final Duration signDuration;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public static Builder builder(String bucket) { return new Builder(bucket); }

    private GcsStorage(Builder b) {
        if (b.storage != null) {
            this.storage = b.storage;
            this.ownsStorage = false;
        } else {
            this.storage = StorageOptions.getDefaultInstance().getService();
            this.ownsStorage = true;
        }
        this.bucket = b.bucket;
        this.keyPrefix = b.keyPrefix.endsWith("/") ? b.keyPrefix : b.keyPrefix + "/";
        this.signDuration = b.signDuration;
    }

    @Override
    public URI upload(byte[] body, String contentEncoding) {
        String key = keyPrefix + UUID.randomUUID() + ".arrow";
        BlobInfo.Builder blob = BlobInfo.newBuilder(bucket, key)
                .setContentType("application/vnd.apache.arrow.stream");
        if (contentEncoding != null) blob.setContentEncoding(contentEncoding);
        storage.create(blob.build(), body);
        URL url = storage.signUrl(BlobInfo.newBuilder(bucket, key).build(),
                signDuration.toMillis(), TimeUnit.MILLISECONDS,
                Storage.SignUrlOption.httpMethod(HttpMethod.GET),
                Storage.SignUrlOption.withV4Signature());
        try {
            return url.toURI();
        } catch (URISyntaxException e) {
            // The URL came from the GCS signer — it should not fail URI parsing.
            throw new IllegalStateException("GCS signer returned unparseable URL", e);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true) && ownsStorage) {
            try { storage.close(); } catch (Exception e) { LOG.warn("GCS Storage close failed", e); }
        }
    }

    public static final class Builder {
        private final String bucket;
        private String keyPrefix = "vgi-rpc/";
        private Duration signDuration = Duration.ofHours(1);
        private Storage storage;

        Builder(String bucket) { this.bucket = bucket; }

        public Builder keyPrefix(String keyPrefix) { this.keyPrefix = keyPrefix; return this; }
        public Builder signDuration(Duration signDuration) { this.signDuration = signDuration; return this; }
        /** Supply a pre-configured GCS client (e.g. for custom project/credentials). */
        public Builder storage(Storage storage) { this.storage = storage; return this; }

        public GcsStorage build() { return new GcsStorage(this); }
    }
}
