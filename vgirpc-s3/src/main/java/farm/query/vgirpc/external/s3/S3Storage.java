// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.external.s3;

import farm.query.vgirpc.external.ExternalStorage;
import farm.query.vgirpc.external.ObjectMissingException;
import farm.query.vgirpc.external.ObjectStore;
import farm.query.vgirpc.external.StorageHttp;
import farm.query.vgirpc.external.UploadUrlProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

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
 *
 * <p>Also the protocol-3.4 object API ({@link ObjectStore}): {@link #putObject} stores
 * under {@code <keyPrefix><uuid>.bin} with a signed {@code x-amz-checksum-sha256}, and the
 * key is the {@code object_id}. Objects carry no TTL: set a bucket lifecycle rule of at least
 * one day on the prefix.</p>
 *
 * <p>And an {@link UploadUrlProvider} (upload URLs v2): {@link #generateUploadUrl} pre-signs a
 * PUT bound to the declared size and SHA-256, so a server wired to S3 directly (no blob vendor)
 * accepts requests over its request cap and argument references.</p>
 *
 * <p>Timeouts match vgi-rpc-python's storage HTTP: a 30&nbsp;s connect timeout and a
 * socket idle (stall) timeout ({@value StorageHttp#ENV_STALL_TIMEOUT}, default 30&nbsp;s)
 * on every read, and no fixed whole-request deadline, so a large object on a slow link
 * completes. A blocked write is not covered by the socket timeout, so each PUT attempt
 * has a deadline that scales with the body: connect + stall + size at 64&nbsp;KiB/s
 * ({@link Builder#minUploadRateBytesPerSecond}). The SDK retries transient failures.</p>
 */
public final class S3Storage implements ExternalStorage, ObjectStore, UploadUrlProvider, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(S3Storage.class);

    private final S3Client s3;
    private final S3Presigner presigner;
    private final String bucket;
    private final String keyPrefix;
    private final Duration presignDuration;
    private final Duration connectTimeout;
    private final Duration stallTimeout;
    private final long minUploadRate;
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
        this.connectTimeout = b.connectTimeout;
        this.stallTimeout = b.stallTimeout != null ? b.stallTimeout : StorageHttp.stallTimeoutFromEnv();
        if (b.minUploadRateBytesPerSecond <= 0) {
            throw new IllegalArgumentException("minUploadRateBytesPerSecond must be positive");
        }
        this.minUploadRate = b.minUploadRateBytesPerSecond;

        var sc = S3Client.builder()
                .region(b.region)
                .credentialsProvider(b.credentials)
                // Idle timeouts, not deadlines: Apache's socket timeout bounds each
                // read; no apiCallTimeout / apiCallAttemptTimeout is set.
                .httpClientBuilder(ApacheHttpClient.builder()
                        .connectionTimeout(b.connectTimeout)
                        .socketTimeout(stallTimeout))
                // Every upload is a known-length byte array. Avoid the SDK's
                // aws-chunked Content-Encoding, which otherwise becomes a
                // duplicate header when the object itself is zstd encoded and
                // is rejected by strict S3-compatible implementations.
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(b.forcePathStyle)
                        .chunkedEncodingEnabled(false)
                        .build());
        if (b.endpointOverride != null) sc.endpointOverride(b.endpointOverride);
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
        put.overrideConfiguration(o -> o.apiCallAttemptTimeout(uploadAttemptDeadline(body.length)));
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

    /**
     * Pre-signed PUT and GET URLs for one sized client upload (upload URLs v2), as
     * vgi-rpc-python's {@code S3Storage.generate_upload_url}.
     *
     * <p>The PUT URL signs {@code Content-Length} and {@code x-amz-checksum-sha256} (SigV4), so
     * S3 refuses a body that is not exactly {@code size} bytes ({@code SignatureDoesNotMatch}) or
     * does not hash to {@code sha256Hex} ({@code BadDigest}). The client sends the returned
     * headers verbatim. The object is not deleted automatically: set a lifecycle rule on the
     * prefix.</p>
     *
     * @param size      exact byte length the client will PUT ({@code >= 1})
     * @param sha256Hex SHA-256 of those bytes, 64 lowercase hex characters
     * @return the PUT/GET pair, the headers the PUT must carry, and the object key
     * @throws IllegalArgumentException if {@code size} or {@code sha256Hex} is malformed
     */
    @Override
    public UploadUrl generateUploadUrl(long size, String sha256Hex) {
        if (size < 1) throw new IllegalArgumentException("size must be >= 1, got " + size);
        if (sha256Hex == null || !sha256Hex.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("sha256 must be 64 lowercase hex characters");
        }
        String checksum = java.util.Base64.getEncoder().encodeToString(java.util.HexFormat.of().parseHex(sha256Hex));
        String key = keyPrefix + UUID.randomUUID() + ".arrow";
        PresignedPutObjectRequest put = presigner.presignPutObject(PutObjectPresignRequest.builder()
                .signatureDuration(presignDuration)
                .putObjectRequest(r -> r.bucket(bucket).key(key).contentLength(size).checksumSHA256(checksum))
                .build());
        String get = presigner.presignGetObject(GetObjectPresignRequest.builder()
                .signatureDuration(presignDuration)
                .getObjectRequest(r -> r.bucket(bucket).key(key))
                .build()).url().toString();
        // Every signed header the client must send, except the ones its HTTP stack derives.
        java.util.Map<String, String> headers = new java.util.LinkedHashMap<>();
        put.signedHeaders().forEach((name, values) -> {
            if (!"host".equalsIgnoreCase(name) && !"content-length".equalsIgnoreCase(name)) {
                headers.put(name, String.join(",", values));
            }
        });
        headers.putIfAbsent("x-amz-checksum-sha256", checksum);
        return new UploadUrl(put.url().toString(), get, put.expiration(), headers, key);
    }

    /**
     * Store {@code data} under a fresh key and return the key: a SigV4 PutObject with the
     * length and {@code ChecksumSHA256}, so S3/R2 verify the body.
     *
     * @param data the bytes to store
     * @return the object key (the {@code object_id})
     */
    @Override
    public String putObject(byte[] data) {
        String key = keyPrefix + UUID.randomUUID().toString().replace("-", "") + ".bin";
        String sha256;
        try {
            sha256 = java.util.Base64.getEncoder().encodeToString(
                    java.security.MessageDigest.getInstance("SHA-256").digest(data));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
        s3.putObject(PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentType("application/octet-stream")
                .contentLength((long) data.length)
                .checksumSHA256(sha256)
                .overrideConfiguration(o -> o.apiCallAttemptTimeout(uploadAttemptDeadline(data.length)))
                .build(), RequestBody.fromBytes(data));
        return key;
    }

    /**
     * Fetch the object a key names, refusing more than {@code maxBytes}.
     *
     * @param objectId the object key
     * @param maxBytes the largest object accepted
     * @return the object bytes
     * @throws ObjectMissingException when the key does not exist
     * @throws java.io.IOException on a read failure or an object over the cap
     */
    @Override
    public byte[] get(String objectId, long maxBytes) throws ObjectMissingException, java.io.IOException {
        try (ResponseInputStream<GetObjectResponse> in =
                     s3.getObject(r -> r.bucket(bucket).key(objectId))) {
            Long length = in.response().contentLength();
            if (length != null && length > maxBytes) {
                throw new java.io.IOException("S3 object is " + length + " bytes, over max_fetch_bytes (" + maxBytes + ")");
            }
            // maxBytes + 1 (to detect an over-cap body) without overflowing at Long.MAX_VALUE.
            byte[] out = in.readNBytes((int) Math.min(maxBytes, Integer.MAX_VALUE - 9) + 1);
            if (out.length > maxBytes) {
                throw new java.io.IOException("S3 object exceeds max_fetch_bytes (" + maxBytes + ")");
            }
            return out;
        } catch (NoSuchKeyException e) {
            throw new ObjectMissingException("S3 object is gone");
        } catch (S3Exception e) {
            if (e.statusCode() == 404) throw new ObjectMissingException("S3 object is gone");
            throw e;
        }
    }

    /**
     * The per-attempt deadline of a PUT of {@code size} bytes: connect + stall +
     * {@code size} at the floor rate (default 64&nbsp;KiB/s). The Apache client's
     * socket timeout bounds every read but not a blocked write, so a server that
     * stops reading a body would otherwise hang the upload; this backstop scales
     * with the body so a slow upload that keeps moving above the floor rate is
     * never cut off. Reads (GETs) have no deadline beyond the stall timeout.
     */
    Duration uploadAttemptDeadline(long size) {
        long bodyMillis = (long) Math.ceil(size * 1000.0 / minUploadRate);
        return connectTimeout.plus(stallTimeout).plusMillis(bodyMillis);
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
        private Duration connectTimeout = StorageHttp.DEFAULT_CONNECT_TIMEOUT;
        private Duration stallTimeout;
        private long minUploadRateBytesPerSecond = 64 * 1024;

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
        /** TCP/TLS connect timeout (default 30&nbsp;s). */
        public Builder connectTimeout(Duration connectTimeout) { this.connectTimeout = connectTimeout; return this; }
        /** Socket idle (stall) timeout: a read that waits this long with no bytes fails and
         *  is retried (default {@value StorageHttp#ENV_STALL_TIMEOUT}, else 30&nbsp;s). */
        public Builder stallTimeout(Duration stallTimeout) { this.stallTimeout = stallTimeout; return this; }
        /** Floor rate for the per-attempt PUT deadline, connect + stall + size / rate
         *  (default 64&nbsp;KiB/s): the backstop for a server that stops reading a body. */
        public Builder minUploadRateBytesPerSecond(long rate) { this.minUploadRateBytesPerSecond = rate; return this; }

        /** Build the configured {@link S3Storage}. */
        public S3Storage build() { return new S3Storage(this); }
    }
}
