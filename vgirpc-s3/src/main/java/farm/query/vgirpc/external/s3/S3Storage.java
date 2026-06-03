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

    /**
     * Start building an {@code S3Storage} that uploads into the given bucket.
     *
     * @param bucket destination S3 bucket name
     * @return a fresh {@link Builder}
     */
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

    /**
     * Upload the IPC bytes under {@code <keyPrefix><uuid>.arrow} with content type
     * {@code application/vnd.apache.arrow.stream}, then return a V4 pre-signed GET
     * URL valid for the configured {@link Builder#presignDuration duration}.
     *
     * @param body full Arrow IPC stream bytes
     * @param contentEncoding stored as the object's {@code Content-Encoding}
     *        (e.g. {@code "zstd"}); ignored when {@code null}
     * @return a short-lived pre-signed GET URL the peer can fetch
     * @throws IllegalStateException if the presigner returns a URL that cannot be parsed as a {@link URI}
     */
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

    /** Close the underlying {@link S3Client} and {@link S3Presigner}. Idempotent. */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            try { s3.close(); } catch (Exception e) { LOG.warn("S3Client close failed", e); }
            try { presigner.close(); } catch (Exception e) { LOG.warn("S3Presigner close failed", e); }
        }
    }

    /** Fluent builder for {@link S3Storage}. */
    public static final class Builder {
        private final String bucket;
        private String keyPrefix = "vgi-rpc/";
        private Region region = Region.US_EAST_1;
        private AwsCredentialsProvider credentials = DefaultCredentialsProvider.create();
        private URI endpointOverride;
        private boolean forcePathStyle;
        private Duration presignDuration = Duration.ofHours(1);

        Builder(String bucket) { this.bucket = bucket; }

        /** Key prefix for uploaded objects; a trailing slash is added if missing (default {@code "vgi-rpc/"}). */
        public Builder keyPrefix(String keyPrefix) { this.keyPrefix = keyPrefix; return this; }
        /** Set the AWS region (default {@link Region#US_EAST_1}). */
        public Builder region(Region region) { this.region = region; return this; }
        /** Set the AWS region by id, e.g. {@code "us-west-2"}. */
        public Builder region(String region) { return region(Region.of(region)); }
        /** Override the credentials provider (default {@link DefaultCredentialsProvider}). */
        public Builder credentials(AwsCredentialsProvider credentials) { this.credentials = credentials; return this; }
        /** Override the S3 endpoint, e.g. for MinIO or LocalStack. */
        public Builder endpointOverride(URI endpointOverride) { this.endpointOverride = endpointOverride; return this; }
        /** Enable path-style addressing (required by most S3-compatible stores). */
        public Builder forcePathStyle(boolean forcePathStyle) { this.forcePathStyle = forcePathStyle; return this; }
        /** Validity window of the pre-signed GET URLs (default one hour). */
        public Builder presignDuration(Duration presignDuration) { this.presignDuration = presignDuration; return this; }

        /** Build the configured {@link S3Storage}. */
        public S3Storage build() { return new S3Storage(this); }
    }
}
