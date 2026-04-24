// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.external.s3;

import farm.query.vgirpc.external.ExternalStorage;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link ExternalStorage} implementation backed by Amazon S3 (or any S3-compatible
 * object store; endpoint is configurable for MinIO / LocalStack).
 *
 * <p>Uploads opaque IPC bytes under {@code <keyPrefix>/<uuid>} and returns a
 * V4 pre-signed GET URL whose expiry defaults to one hour.</p>
 */
public final class S3Storage implements ExternalStorage, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(S3Storage.class);

    private final S3Client s3;
    private final S3Presigner presigner;
    private final String bucket;
    private final String keyPrefix;
    private final Duration presignDuration;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public static Builder builder(String bucket) { return new Builder(bucket); }

    private S3Storage(Builder b) {
        this.bucket = b.bucket;
        this.keyPrefix = b.keyPrefix.endsWith("/") ? b.keyPrefix : b.keyPrefix + "/";
        this.presignDuration = b.presignDuration;

        var sc = S3Client.builder()
                .region(b.region)
                .credentialsProvider(b.credentials);
        if (b.endpointOverride != null) sc.endpointOverride(b.endpointOverride);
        if (b.forcePathStyle) sc.forcePathStyle(true);
        this.s3 = sc.build();

        var pb = S3Presigner.builder()
                .region(b.region)
                .credentialsProvider(b.credentials);
        if (b.endpointOverride != null) pb.endpointOverride(b.endpointOverride);
        if (b.forcePathStyle) pb.serviceConfiguration(
                software.amazon.awssdk.services.s3.S3Configuration.builder()
                        .pathStyleAccessEnabled(true).build());
        this.presigner = pb.build();
    }

    @Override
    public URI upload(byte[] body, String contentEncoding) {
        String key = keyPrefix + UUID.randomUUID() + ".arrow";
        PutObjectRequest.Builder put = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentType("application/vnd.apache.arrow.stream")
                .contentLength((long) body.length);
        if (contentEncoding != null) put.contentEncoding(contentEncoding);
        s3.putObject(put.build(), RequestBody.fromBytes(body));

        GetObjectPresignRequest presign = GetObjectPresignRequest.builder()
                .signatureDuration(presignDuration)
                .getObjectRequest(r -> r.bucket(bucket).key(key))
                .build();
        try {
            return presigner.presignGetObject(presign).url().toURI();
        } catch (URISyntaxException e) {
            // The URL came from the AWS presigner — it should not fail URI parsing.
            throw new IllegalStateException("S3 presigner returned unparseable URL", e);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            try { s3.close(); } catch (Exception e) { LOG.warn("S3Client close failed", e); }
            try { presigner.close(); } catch (Exception e) { LOG.warn("S3Presigner close failed", e); }
        }
    }

    public static final class Builder {
        private final String bucket;
        private String keyPrefix = "vgi-rpc/";
        private Region region = Region.US_EAST_1;
        private AwsCredentialsProvider credentials = DefaultCredentialsProvider.create();
        private URI endpointOverride;
        private boolean forcePathStyle;
        private Duration presignDuration = Duration.ofHours(1);

        Builder(String bucket) { this.bucket = bucket; }

        public Builder keyPrefix(String keyPrefix) { this.keyPrefix = keyPrefix; return this; }
        public Builder region(Region region) { this.region = region; return this; }
        public Builder region(String region) { return region(Region.of(region)); }
        public Builder credentials(AwsCredentialsProvider credentials) { this.credentials = credentials; return this; }
        public Builder endpointOverride(URI endpointOverride) { this.endpointOverride = endpointOverride; return this; }
        public Builder forcePathStyle(boolean forcePathStyle) { this.forcePathStyle = forcePathStyle; return this; }
        public Builder presignDuration(Duration presignDuration) { this.presignDuration = presignDuration; return this; }

        public S3Storage build() { return new S3Storage(this); }
    }
}
