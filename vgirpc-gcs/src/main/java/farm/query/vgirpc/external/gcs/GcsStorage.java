// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.external.gcs;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.HttpMethod;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import farm.query.vgirpc.external.ExternalStorage;

import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.UUID;

/**
 * {@link ExternalStorage} backed by Google Cloud Storage. Uploads via the
 * synchronous {@code Storage} client using Application Default Credentials
 * (or a caller-supplied {@link Storage}) and returns a V4 signed GET URL.
 */
public final class GcsStorage implements ExternalStorage {

    private final Storage storage;
    private final String bucket;
    private final String keyPrefix;
    private final Duration signDuration;

    public static Builder builder(String bucket) { return new Builder(bucket); }

    private GcsStorage(Builder b) {
        this.storage = b.storage != null ? b.storage : StorageOptions.getDefaultInstance().getService();
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
                signDuration.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS,
                Storage.SignUrlOption.httpMethod(HttpMethod.GET),
                Storage.SignUrlOption.withV4Signature());
        try {
            return url.toURI();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static final class Builder {
        private final String bucket;
        private String keyPrefix = "vgi-rpc/";
        private Duration signDuration = Duration.ofHours(1);
        private Storage storage;

        Builder(String bucket) { this.bucket = bucket; }

        public Builder keyPrefix(String p) { this.keyPrefix = p; return this; }
        public Builder signDuration(Duration d) { this.signDuration = d; return this; }
        /** Supply a pre-configured GCS client (e.g. for custom project/credentials). */
        public Builder storage(Storage s) { this.storage = s; return this; }

        public GcsStorage build() { return new GcsStorage(this); }
    }
}
