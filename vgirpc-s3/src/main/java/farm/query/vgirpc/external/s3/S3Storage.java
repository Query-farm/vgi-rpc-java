// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.external.s3;

import farm.query.vgirpc.external.ExternalStorage;
import farm.query.vgirpc.external.ObjectMissingException;
import farm.query.vgirpc.external.ObjectStore;
import farm.query.vgirpc.external.StorageHttp;
import farm.query.vgirpc.external.UploadUrlProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache5.Apache5HttpClient;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link ExternalStorage} implementation backed by Amazon S3 (or any S3-compatible
 * object store: MinIO, Ceph, Cloudflare R2's S3 API, Wasabi, Backblaze B2, LocalStack).
 *
 * <p>Uploads opaque IPC bytes under {@code <keyPrefix><uuid>.arrow} and returns a
 * V4 pre-signed GET URL whose lifetime defaults to one hour (see {@link #effectiveUrlTtl}).</p>
 *
 * <p>Also the protocol-3.4 object API ({@link ObjectStore}): {@link #putObject} stores
 * under {@code <keyPrefix><uuid>.bin} with a signed {@code x-amz-checksum-sha256}, and the
 * key is the {@code object_id}. Objects carry no TTL: set a bucket lifecycle rule expiring the
 * prefix after one to two days. The worker never deletes.</p>
 *
 * <p>And an {@link UploadUrlProvider} (upload URLs v2): {@link #generateUploadUrl} pre-signs a
 * PUT bound to the declared size and SHA-256, so a server wired to S3 directly (no blob vendor)
 * accepts requests over its request cap and argument references. With
 * {@link Checksum#NONE} (a service that does not enforce {@code x-amz-checksum-sha256}) it
 * issues no upload grants ({@link #supportsUploadGrants()} is false): results and objects are
 * still stored, and their readers verify the digest themselves.</p>
 *
 * <p>The environment contract (direct object storage v1, the same in every SDK) is read by
 * {@link #fromEnv(Map)}: {@code VGI_STORAGE_S3_*} and {@value #ENV_URL_TTL}.</p>
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

    /** Bucket name (required for the {@code s3} backend). */
    public static final String ENV_BUCKET = "VGI_STORAGE_S3_BUCKET";
    /** Key prefix, e.g. {@code vgi/} (default empty). */
    public static final String ENV_PREFIX = "VGI_STORAGE_S3_PREFIX";
    /** Custom endpoint URL ({@code https://} or {@code http://}); unset = AWS. */
    public static final String ENV_ENDPOINT = "VGI_STORAGE_S3_ENDPOINT";
    /** Endpoint used only for the presigned URLs handed to clients (default: the endpoint). */
    public static final String ENV_PUBLIC_ENDPOINT = "VGI_STORAGE_S3_PUBLIC_ENDPOINT";
    /** Signing region ({@code auto} for R2); default {@code AWS_REGION}, {@code AWS_DEFAULT_REGION}, us-east-1. */
    public static final String ENV_REGION = "VGI_STORAGE_S3_REGION";
    /** {@code true} = bucket in the path; default {@code true} with an endpoint, else {@code false}. */
    public static final String ENV_FORCE_PATH_STYLE = "VGI_STORAGE_S3_FORCE_PATH_STYLE";
    /** {@code sha256} (default) or {@code none}. */
    public static final String ENV_CHECKSUM = "VGI_STORAGE_S3_CHECKSUM";
    /**
     * The AWS SDK's own request checksums: {@code when_required} (only where an operation
     * requires one) or {@code when_supported} (a CRC32 on every upload, the SDK's own default,
     * which some S3-compatible stores reject). Default {@code when_required} with an endpoint,
     * {@code when_supported} for AWS.
     */
    public static final String ENV_SDK_CHECKSUM = "VGI_STORAGE_S3_SDK_CHECKSUM";
    /** Explicit access key id (with {@value #ENV_SECRET_ACCESS_KEY}). */
    public static final String ENV_ACCESS_KEY_ID = "VGI_STORAGE_S3_ACCESS_KEY_ID";
    /** Explicit secret access key. */
    public static final String ENV_SECRET_ACCESS_KEY = "VGI_STORAGE_S3_SECRET_ACCESS_KEY";
    /** Optional session token for the explicit keys. */
    public static final String ENV_SESSION_TOKEN = "VGI_STORAGE_S3_SESSION_TOKEN";
    /** Presigned URL lifetime in seconds (default 3600, capped by credential expiry). */
    public static final String ENV_URL_TTL = "VGI_STORAGE_URL_TTL_SECONDS";
    /** {@code 1} = check the bucket at startup instead of on first use. */
    public static final String ENV_VERIFY = "VGI_STORAGE_S3_VERIFY";
    /** Expiry of the explicit session token (RFC 3339 or epoch seconds); caps URL lifetimes. */
    public static final String ENV_SESSION_EXPIRES = "VGI_STORAGE_S3_SESSION_EXPIRES";
    /** {@code 1} = allow an {@code http://} endpoint or public endpoint (refused otherwise). */
    public static final String ENV_ALLOW_PLAIN_HTTP = "VGI_STORAGE_ALLOW_PLAIN_HTTP";
    /** Key area for results, upload grants and StateRef / argument objects. */
    public static final String AREA_RPC = "rpc";
    /** Key area for function storage values. */
    public static final String AREA_FS = "fs";

    /** The shortest lifetime an upload grant may be signed for (15 minutes). */
    public static final Duration MIN_UPLOAD_GRANT_TTL = Duration.ofMinutes(15);
    /** URLs stop this long before the signing credentials expire. */
    static final Duration CREDENTIAL_EXPIRY_MARGIN = Duration.ofSeconds(60);

    /** Integrity enforced by storage on worker and client PUTs. */
    public enum Checksum {
        /** {@code x-amz-checksum-sha256} is signed and sent; upload grants are issued. */
        SHA256,
        /** No checksum header; no upload grants. For services that ignore the header. */
        NONE
    }

    private final S3Client s3;
    private final S3Presigner presigner;
    private final AwsCredentialsProvider credentials;
    private final String bucket;
    private final String keyPrefix;
    private final Duration presignDuration;
    private final Duration connectTimeout;
    private final Duration stallTimeout;
    private final long minUploadRate;
    private final Checksum checksum;
    private final String endpointDescription;
    private final String regionId;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Start building an {@code S3Storage} that uploads into the given bucket.
     *
     * @param bucket destination S3 bucket name
     * @return a fresh {@link Builder}
     */
    public static Builder builder(String bucket) { return new Builder(bucket); }

    /**
     * Build from the direct-storage environment ({@value #ENV_BUCKET} and the other
     * {@code VGI_STORAGE_S3_*} variables, {@value #ENV_URL_TTL}).
     *
     * <p>Credentials: {@value #ENV_ACCESS_KEY_ID} / {@value #ENV_SECRET_ACCESS_KEY} (plus an
     * optional {@value #ENV_SESSION_TOKEN}) when set, else the AWS SDK's default chain
     * (environment, shared profile including {@code role_arn} and {@code credential_process},
     * web identity, container credentials, EC2 IMDSv2), which refreshes temporary credentials
     * before they expire.</p>
     *
     * @param env environment mapping
     * @return the storage
     * @throws IllegalArgumentException if the bucket is missing or a variable is malformed
     */
    public static S3Storage fromEnv(Map<String, String> env) {
        return builderFromEnv(env).build();
    }

    /**
     * As {@link #fromEnv(Map)}, for one key area: keys go under
     * {@code <VGI_STORAGE_S3_PREFIX><area>/} ({@link #AREA_RPC} for results, upload grants and
     * StateRef / argument objects, which {@link #fromEnv(Map)} uses; {@link #AREA_FS} for
     * function storage). Operators set lifecycle rules of 1 day on {@code rpc/} and 2 days on
     * {@code fs/}.
     *
     * @param env  environment mapping
     * @param area the key area
     * @return the storage
     * @throws IllegalArgumentException if the bucket is missing or a variable is malformed
     */
    public static S3Storage fromEnv(Map<String, String> env, String area) {
        return builderFromEnv(env, area).build();
    }

    /**
     * The {@link Builder} {@link #fromEnv(Map)} would build, for further settings.
     *
     * @param env environment mapping
     * @return the builder
     * @throws IllegalArgumentException if the bucket is missing or a variable is malformed
     */
    public static Builder builderFromEnv(Map<String, String> env) {
        return builderFromEnv(env, AREA_RPC);
    }

    /**
     * The {@link Builder} {@link #fromEnv(Map, String)} would build.
     *
     * @param env  environment mapping
     * @param area the key area ({@link #AREA_RPC} or {@link #AREA_FS})
     * @return the builder
     * @throws IllegalArgumentException if the bucket is missing or a variable is malformed
     */
    public static Builder builderFromEnv(Map<String, String> env, String area) {
        String bucket = trimmed(env, ENV_BUCKET);
        if (bucket == null) throw new IllegalArgumentException(ENV_BUCKET + " is required for the s3 storage backend");
        if (area == null || !area.matches("[a-z0-9-]+")) throw new IllegalArgumentException("bad key area: " + area);
        String prefix = Optional.ofNullable(trimmed(env, ENV_PREFIX)).orElse("");
        if (!prefix.isEmpty() && !prefix.endsWith("/")) prefix += "/";
        Builder b = builder(bucket).keyPrefix(prefix + area + "/");
        boolean allowHttp = parseBool(ENV_ALLOW_PLAIN_HTTP,
                Optional.ofNullable(trimmed(env, ENV_ALLOW_PLAIN_HTTP)).orElse("0"));
        String endpoint = trimmed(env, ENV_ENDPOINT);
        if (endpoint != null) b.endpointOverride(endpointUri(ENV_ENDPOINT, endpoint, allowHttp));
        String publicEndpoint = trimmed(env, ENV_PUBLIC_ENDPOINT);
        if (publicEndpoint != null) b.publicEndpoint(endpointUri(ENV_PUBLIC_ENDPOINT, publicEndpoint, allowHttp));
        String region = firstNonBlank(env, ENV_REGION, "AWS_REGION", "AWS_DEFAULT_REGION");
        b.region(region != null ? region : "us-east-1");
        String pathStyle = trimmed(env, ENV_FORCE_PATH_STYLE);
        if (pathStyle != null) b.forcePathStyle(parseBool(ENV_FORCE_PATH_STYLE, pathStyle));
        String checksum = trimmed(env, ENV_CHECKSUM);
        if (checksum != null) {
            b.checksum(switch (checksum.toLowerCase(Locale.ROOT)) {
                case "sha256" -> Checksum.SHA256;
                case "none" -> Checksum.NONE;
                default -> throw new IllegalArgumentException(ENV_CHECKSUM + " must be sha256 or none, got '"
                        + checksum + "'");
            });
        }
        String sdkChecksum = trimmed(env, ENV_SDK_CHECKSUM);
        if (sdkChecksum != null) {
            b.requestChecksumCalculation(switch (sdkChecksum.toLowerCase(Locale.ROOT)) {
                case "when_required" -> RequestChecksumCalculation.WHEN_REQUIRED;
                case "when_supported" -> RequestChecksumCalculation.WHEN_SUPPORTED;
                default -> throw new IllegalArgumentException(ENV_SDK_CHECKSUM
                        + " must be when_required or when_supported, got '" + sdkChecksum + "'");
            });
        }
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
            b.presignDuration(Duration.ofSeconds(seconds));
        }
        String keyId = trimmed(env, ENV_ACCESS_KEY_ID);
        String secret = trimmed(env, ENV_SECRET_ACCESS_KEY);
        String token = trimmed(env, ENV_SESSION_TOKEN);
        if ((keyId == null) != (secret == null)) {
            throw new IllegalArgumentException(ENV_ACCESS_KEY_ID + " and " + ENV_SECRET_ACCESS_KEY
                    + " must be set together");
        }
        if (token != null && keyId == null) {
            throw new IllegalArgumentException(ENV_SESSION_TOKEN + " needs " + ENV_ACCESS_KEY_ID + " and "
                    + ENV_SECRET_ACCESS_KEY);
        }
        String expires = trimmed(env, ENV_SESSION_EXPIRES);
        if (expires != null && token == null) {
            throw new IllegalArgumentException(ENV_SESSION_EXPIRES + " needs " + ENV_SESSION_TOKEN);
        }
        if (keyId != null) {
            if (token != null) {
                AwsSessionCredentials.Builder creds = AwsSessionCredentials.builder()
                        .accessKeyId(keyId).secretAccessKey(secret).sessionToken(token);
                if (expires != null) {
                    creds.expirationTime(parseInstant(ENV_SESSION_EXPIRES, expires));
                } else {
                    LOG.warn("{} is set without {}: presigned URL lifetimes cannot be capped at the session's "
                            + "expiry, so URLs may stop working early; prefer credential_process or the standard "
                            + "AWS credential chain, which report expiry", ENV_SESSION_TOKEN, ENV_SESSION_EXPIRES);
                }
                b.credentials(StaticCredentialsProvider.create(creds.build()));
            } else {
                b.credentials(StaticCredentialsProvider.create(AwsBasicCredentials.create(keyId, secret)));
            }
        }
        b.stallTimeout(StorageHttp.stallTimeoutFromEnv(env));
        return b;
    }

    private static String trimmed(Map<String, String> env, String name) {
        String v = env.get(name);
        return v == null || v.isBlank() ? null : v.strip();
    }

    private static String firstNonBlank(Map<String, String> env, String... names) {
        for (String n : names) {
            String v = trimmed(env, n);
            if (v != null) return v;
        }
        return null;
    }

    private static boolean parseBool(String name, String raw) {
        return switch (raw.toLowerCase(Locale.ROOT)) {
            case "1", "true", "yes", "on" -> true;
            case "0", "false", "no", "off" -> false;
            default -> throw new IllegalArgumentException(name + " must be true or false, got '" + raw + "'");
        };
    }

    private static Instant parseInstant(String name, String raw) {
        try {
            if (raw.matches("\\d+")) return Instant.ofEpochSecond(Long.parseLong(raw));
            return java.time.OffsetDateTime.parse(raw).toInstant();
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(name + " must be an RFC 3339 timestamp or epoch seconds, got '"
                    + raw + "'");
        }
    }

    private static URI endpointUri(String name, String raw, boolean allowHttp) {
        URI uri = endpointUri(name, raw);
        if ("http".equalsIgnoreCase(uri.getScheme()) && !allowHttp) {
            throw new IllegalArgumentException(name + " is plain http (" + uri.getScheme() + "://" + uri.getAuthority()
                    + "): set " + ENV_ALLOW_PLAIN_HTTP + "=1 to allow it (test and private-network stores only)");
        }
        return uri;
    }

    private static URI endpointUri(String name, String raw) {
        URI uri;
        try {
            uri = new URI(raw);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(name + " is not a URL: '" + raw + "'");
        }
        String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
        if (!(scheme.equals("https") || scheme.equals("http")) || uri.getHost() == null) {
            throw new IllegalArgumentException(name + " must be an http:// or https:// URL, got '" + raw + "'");
        }
        return uri;
    }

    private S3Storage(Builder b) {
        this.bucket = b.bucket;
        this.keyPrefix = b.keyPrefix.isEmpty() || b.keyPrefix.endsWith("/") ? b.keyPrefix : b.keyPrefix + "/";
        if (b.presignDuration == null || b.presignDuration.isZero() || b.presignDuration.isNegative()) {
            throw new IllegalArgumentException("presignDuration must be positive");
        }
        this.presignDuration = b.presignDuration;
        this.credentials = b.credentials != null ? b.credentials : DefaultCredentialsProvider.builder().build();
        this.checksum = b.checksum;
        this.regionId = b.region.id();
        this.endpointDescription = b.endpointOverride != null
                ? b.endpointOverride.getScheme() + "://" + b.endpointOverride.getAuthority()
                : "AWS (" + regionId + ")";
        // Path-style by default for a custom endpoint (MinIO, Ceph, R2, B2, LocalStack),
        // as boto3 does for vgi-rpc-python; virtual-hosted for AWS.
        boolean pathStyle = b.forcePathStyle != null ? b.forcePathStyle : b.endpointOverride != null;
        this.connectTimeout = b.connectTimeout;
        this.stallTimeout = b.stallTimeout != null ? b.stallTimeout : StorageHttp.stallTimeoutFromEnv();
        if (b.minUploadRateBytesPerSecond <= 0) {
            throw new IllegalArgumentException("minUploadRateBytesPerSecond must be positive");
        }
        this.minUploadRate = b.minUploadRateBytesPerSecond;
        // The SDK's own request checksums (a CRC32 on every upload since SDK 2.30) are
        // rejected by some S3-compatible stores: only where an operation requires one for a
        // custom endpoint, unless asked otherwise. The sha256 this class sends is explicit.
        RequestChecksumCalculation sdkChecksum = b.requestChecksumCalculation != null
                ? b.requestChecksumCalculation
                : b.endpointOverride != null ? RequestChecksumCalculation.WHEN_REQUIRED
                : RequestChecksumCalculation.WHEN_SUPPORTED;

        var sc = S3Client.builder()
                .region(b.region)
                .credentialsProvider(credentials)
                .requestChecksumCalculation(sdkChecksum)
                .responseChecksumValidation(b.endpointOverride != null
                        ? ResponseChecksumValidation.WHEN_REQUIRED : ResponseChecksumValidation.WHEN_SUPPORTED)
                // Idle timeouts, not deadlines: Apache's socket timeout bounds each
                // read; no apiCallTimeout / apiCallAttemptTimeout is set.
                .httpClientBuilder(Apache5HttpClient.builder()
                        .connectionTimeout(b.connectTimeout)
                        .socketTimeout(stallTimeout))
                // Every upload is a known-length byte array. Avoid the SDK's
                // aws-chunked Content-Encoding, which otherwise becomes a
                // duplicate header when the object itself is zstd encoded and
                // is rejected by strict S3-compatible implementations.
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(pathStyle)
                        .chunkedEncodingEnabled(false)
                        .build());
        if (b.endpointOverride != null) sc.endpointOverride(b.endpointOverride);
        this.s3 = sc.build();

        // Every URL handed out is signed for the public endpoint when one is set (the
        // worker may reach storage at an address clients cannot, e.g. Docker-internal).
        URI urlEndpoint = b.publicEndpoint != null ? b.publicEndpoint : b.endpointOverride;
        boolean urlPathStyle = b.forcePathStyle != null ? b.forcePathStyle : urlEndpoint != null;
        var pb = S3Presigner.builder()
                .region(b.region)
                .credentialsProvider(credentials);
        if (urlEndpoint != null) pb.endpointOverride(urlEndpoint);
        if (urlPathStyle) pb.serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build());
        this.presigner = pb.build();
        if (checksum == Checksum.NONE) {
            LOG.info("S3 storage {} bucket {}: checksum none (no x-amz-checksum-sha256): upload grants and "
                    + "argument references are off; results and objects are verified by their readers",
                    endpointDescription, bucket);
        }
    }

    /**
     * Whether this store issues upload grants ({@link #generateUploadUrl}): only with
     * {@link Checksum#SHA256}, since a grant relies on storage enforcing the digest.
     *
     * @return {@code true} unless the checksum is {@link Checksum#NONE}
     */
    public boolean supportsUploadGrants() {
        return checksum == Checksum.SHA256;
    }

    /**
     * The configured checksum mode.
     *
     * @return the checksum mode
     */
    public Checksum checksum() {
        return checksum;
    }

    /**
     * The lifetime a URL signed now may have: {@code min(presignDuration, credential expiry - now
     * - 60 s)}. A presigned URL stops working when the credentials that signed it expire,
     * whatever its {@code X-Amz-Expires}. When the remaining lifetime is under {@code minimum},
     * the credentials are resolved again first (the default chain refreshes temporary
     * credentials ahead of expiry); if they still expire too soon, the shorter lifetime is used
     * and a warning logged.
     *
     * @param minimum the shortest lifetime the caller needs
     * @return the lifetime to sign for
     * @throws IllegalStateException if the credentials have (all but) expired
     */
    public Duration effectiveUrlTtl(Duration minimum) {
        Duration ttl = presignDuration;
        Duration remaining = remainingCredentialLifetime();
        if (remaining == null) return ttl;
        Duration wanted = minimum.compareTo(ttl) < 0 ? minimum : ttl;
        if (remaining.compareTo(wanted) < 0) {
            Duration again = remainingCredentialLifetime();
            if (again != null) remaining = again;
            if (remaining != null && remaining.compareTo(wanted) < 0) {
                LOG.warn("S3 credentials expire in {} s, under the {} s a presigned URL needs; signing for {} s",
                        remaining.toSeconds(), wanted.toSeconds(), Math.max(0, remaining.toSeconds()));
            }
        }
        if (remaining == null) return ttl;
        if (remaining.isNegative() || remaining.isZero() || remaining.toSeconds() < 1) {
            throw new IllegalStateException("S3 credentials have expired; cannot sign a URL");
        }
        return remaining.compareTo(ttl) < 0 ? remaining : ttl;
    }

    /** Remaining credential lifetime minus the margin, or {@code null} for credentials that do not expire. */
    private Duration remainingCredentialLifetime() {
        AwsCredentialsIdentity identity = credentials.resolveIdentity().join();
        Optional<Instant> expires = identity.expirationTime();
        if (expires.isEmpty()) return null;
        return Duration.between(Instant.now(), expires.get()).minus(CREDENTIAL_EXPIRY_MARGIN);
    }

    /**
     * Check the bucket: a HEAD on it. Fails with an actionable message: the bucket does not
     * exist, access is denied (and to which identity is not logged), or the bucket is in another
     * region (naming the region the service reported).
     *
     * @throws IllegalStateException if the bucket is not usable
     */
    public void verify() {
        try {
            s3.headBucket(r -> r.bucket(bucket));
        } catch (S3Exception e) {
            int status = e.statusCode();
            String where = "S3 bucket '" + bucket + "' at " + endpointDescription;
            String reported = e.awsErrorDetails() == null || e.awsErrorDetails().sdkHttpResponse() == null ? null
                    : e.awsErrorDetails().sdkHttpResponse().firstMatchingHeader("x-amz-bucket-region").orElse(null);
            if (status == 301 || (status == 400 && reported != null && !reported.equals(regionId))) {
                throw new IllegalStateException(where + " is in region '" + (reported != null ? reported : "?")
                        + "', not '" + regionId + "': set " + ENV_REGION + "=" + (reported != null ? reported : "<region>"));
            }
            if (status == 404) {
                throw new IllegalStateException(where + " does not exist: create it (and a lifecycle rule expiring "
                        + "the prefix after 1-2 days), or fix " + ENV_BUCKET + " / " + ENV_ENDPOINT);
            }
            if (status == 403) {
                throw new IllegalStateException(where + ": access denied: the credentials need s3:ListBucket, "
                        + "s3:GetObject and s3:PutObject on it (check " + ENV_ACCESS_KEY_ID
                        + " / the AWS credential chain)");
            }
            throw new IllegalStateException(where + " is not usable: HTTP " + status
                    + (e.awsErrorDetails() != null && e.awsErrorDetails().errorCode() != null
                    ? " " + e.awsErrorDetails().errorCode() : ""));
        } catch (software.amazon.awssdk.core.exception.SdkClientException e) {
            throw new IllegalStateException("S3 bucket '" + bucket + "' at " + endpointDescription
                    + " is unreachable (" + e.getClass().getSimpleName() + "): check " + ENV_ENDPOINT
                    + " and the network");
        }
    }


    /**
     * A {@code urlValidator} for inbound pointers (client uploads to URLs this store signed):
     * accepts only URLs on the host, port, scheme and key prefix this store signs for (the
     * public endpoint when one is set). Built on first use from a locally signed sample URL, so
     * it matches path-style and virtual-hosted addressing alike; {@code http} is accepted only
     * when the signing endpoint is {@code http}.
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
                    v = pinnedFor(signedSample(PIN_SAMPLE_LEAF));
                    pinned = v;
                }
                v.accept(uri);
            }
        };
    }

    private static final String PIN_SAMPLE_LEAF = "vgi-pin-sample";

    static java.util.function.Consumer<URI> pinnedFor(URI sample) {
        String path = sample.getRawPath();
        String dir = path.substring(0, path.length() - PIN_SAMPLE_LEAF.length());
        String host = sample.getPort() < 0 ? sample.getHost() : sample.getHost() + ":" + sample.getPort();
        return farm.query.vgirpc.vendor.BlobVendor.pinnedHostsValidator(java.util.List.of(host),
                "http".equalsIgnoreCase(sample.getScheme()),
                java.util.List.of(sample.getScheme() + "://" + sample.getRawAuthority() + dir));
    }

    private URI signedSample(String leaf) {
        return presignGet(keyPrefix + leaf, Duration.ofMinutes(1));
    }

    private URI presignGet(String key, Duration ttl) {
        GetObjectPresignRequest presign = GetObjectPresignRequest.builder()
                .signatureDuration(ttl)
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
     * Upload the IPC bytes under {@code <keyPrefix><uuid>.arrow} with content type
     * {@code application/vnd.apache.arrow.stream}, then return a V4 pre-signed GET
     * URL valid for {@link #effectiveUrlTtl the effective lifetime}.
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
        return presignGet(key, effectiveUrlTtl(Duration.ofMinutes(5)));
    }

    /**
     * Pre-signed PUT and GET URLs for one sized client upload (upload URLs v2), as
     * vgi-rpc-python's {@code S3Storage.generate_upload_url}.
     *
     * <p>The PUT URL signs {@code Content-Length} and {@code x-amz-checksum-sha256} (SigV4), so
     * S3 refuses a body that is not exactly {@code size} bytes ({@code SignatureDoesNotMatch}) or
     * does not hash to {@code sha256Hex} ({@code BadDigest}). The client sends the returned
     * headers verbatim. The lifetime is at least {@link #MIN_UPLOAD_GRANT_TTL} when the
     * credentials allow it. The object is not deleted automatically: set a lifecycle rule on
     * the prefix.</p>
     *
     * @param size      exact byte length the client will PUT ({@code >= 1})
     * @param sha256Hex SHA-256 of those bytes, 64 lowercase hex characters
     * @return the PUT/GET pair, the headers the PUT must carry, and the object key
     * @throws IllegalArgumentException if {@code size} or {@code sha256Hex} is malformed
     * @throws UnsupportedOperationException with {@link Checksum#NONE}
     */
    @Override
    public UploadUrl generateUploadUrl(long size, String sha256Hex) {
        if (checksum != Checksum.SHA256) {
            throw new UnsupportedOperationException("S3 storage with checksum none issues no upload grants ("
                    + ENV_CHECKSUM + "=none)");
        }
        if (size < 1) throw new IllegalArgumentException("size must be >= 1, got " + size);
        if (sha256Hex == null || !sha256Hex.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("sha256 must be 64 lowercase hex characters");
        }
        String checksumB64 = java.util.Base64.getEncoder().encodeToString(java.util.HexFormat.of().parseHex(sha256Hex));
        String key = keyPrefix + UUID.randomUUID() + ".arrow";
        Duration ttl = effectiveUrlTtl(MIN_UPLOAD_GRANT_TTL);
        PresignedPutObjectRequest put = presigner.presignPutObject(PutObjectPresignRequest.builder()
                .signatureDuration(ttl)
                .putObjectRequest(r -> r.bucket(bucket).key(key).contentLength(size).checksumSHA256(checksumB64))
                .build());
        String get = presignGet(key, ttl).toString();
        // Every signed header the client must send, except the ones its HTTP stack derives.
        java.util.Map<String, String> headers = new java.util.LinkedHashMap<>();
        put.signedHeaders().forEach((name, values) -> {
            if (!"host".equalsIgnoreCase(name) && !"content-length".equalsIgnoreCase(name)) {
                headers.put(name, String.join(",", values));
            }
        });
        headers.putIfAbsent("x-amz-checksum-sha256", checksumB64);
        return new UploadUrl(put.url().toString(), get, put.expiration(), headers, key);
    }

    /**
     * Store {@code data} under a fresh key and return the key: a SigV4 PutObject with the
     * length and (with {@link Checksum#SHA256}) {@code ChecksumSHA256}, so S3/R2 verify the body.
     *
     * @param data the bytes to store
     * @return the object key (the {@code object_id})
     */
    @Override
    public String putObject(byte[] data) {
        String key = keyPrefix + UUID.randomUUID().toString().replace("-", "") + ".bin";
        PutObjectRequest.Builder put = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentType("application/octet-stream")
                .contentLength((long) data.length)
                .overrideConfiguration(o -> o.apiCallAttemptTimeout(uploadAttemptDeadline(data.length)));
        if (checksum == Checksum.SHA256) {
            try {
                put.checksumSHA256(java.util.Base64.getEncoder().encodeToString(
                        java.security.MessageDigest.getInstance("SHA-256").digest(data)));
            } catch (java.security.NoSuchAlgorithmException e) {
                throw new IllegalStateException("SHA-256 unavailable", e);
            }
        }
        s3.putObject(put.build(), RequestBody.fromBytes(data));
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
     * Delete an object, best effort: failures are logged at debug and ignored (a bucket
     * lifecycle rule removes whatever is left).
     *
     * @param objectId the object key
     */
    public void deleteObject(String objectId) {
        try {
            s3.deleteObject(r -> r.bucket(bucket).key(objectId));
        } catch (RuntimeException e) {
            LOG.debug("S3 delete of an object failed ({}); the lifecycle rule will remove it",
                    e.getClass().getSimpleName());
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
        private AwsCredentialsProvider credentials;
        private URI endpointOverride;
        private URI publicEndpoint;
        private Boolean forcePathStyle;
        private Duration presignDuration = Duration.ofHours(1);
        private Duration connectTimeout = StorageHttp.DEFAULT_CONNECT_TIMEOUT;
        private Duration stallTimeout;
        private long minUploadRateBytesPerSecond = 64 * 1024;
        private Checksum checksum = Checksum.SHA256;
        private RequestChecksumCalculation requestChecksumCalculation;

        Builder(String bucket) { this.bucket = bucket; }

        /** Key prefix for uploaded objects; a trailing slash is added if missing, empty for none
         *  (default {@code "vgi-rpc/"}; {@link #fromEnv} defaults to empty). */
        public Builder keyPrefix(String keyPrefix) { this.keyPrefix = keyPrefix; return this; }
        /** Set the AWS region (default {@link Region#US_EAST_1}). */
        public Builder region(Region region) { this.region = region; return this; }
        /** Set the AWS region by id, e.g. {@code "us-west-2"}, or {@code "auto"} for R2. */
        public Builder region(String region) { return region(Region.of(region)); }
        /** Override the credentials provider (default {@link DefaultCredentialsProvider}, which
         *  refreshes temporary credentials ahead of expiry). */
        public Builder credentials(AwsCredentialsProvider credentials) { this.credentials = credentials; return this; }
        /** Override the S3 endpoint, e.g. for MinIO or LocalStack. */
        public Builder endpointOverride(URI endpointOverride) { this.endpointOverride = endpointOverride; return this; }
        /** Endpoint for the presigned URLs handed to clients only (default: the endpoint). */
        public Builder publicEndpoint(URI publicEndpoint) { this.publicEndpoint = publicEndpoint; return this; }
        /** Path-style addressing (required by most S3-compatible stores). Default: on when
         *  {@link #endpointOverride} is set, else off (virtual-hosted, for AWS). */
        public Builder forcePathStyle(boolean forcePathStyle) { this.forcePathStyle = forcePathStyle; return this; }
        /** Lifetime of the pre-signed URLs (default one hour), capped by credential expiry. */
        public Builder presignDuration(Duration presignDuration) { this.presignDuration = presignDuration; return this; }
        /** TCP/TLS connect timeout (default 30&nbsp;s). */
        public Builder connectTimeout(Duration connectTimeout) { this.connectTimeout = connectTimeout; return this; }
        /** Socket idle (stall) timeout: a read that waits this long with no bytes fails and
         *  is retried (default {@value StorageHttp#ENV_STALL_TIMEOUT}, else 30&nbsp;s). */
        public Builder stallTimeout(Duration stallTimeout) { this.stallTimeout = stallTimeout; return this; }
        /** Floor rate for the per-attempt PUT deadline, connect + stall + size / rate
         *  (default 64&nbsp;KiB/s): the backstop for a server that stops reading a body. */
        public Builder minUploadRateBytesPerSecond(long rate) { this.minUploadRateBytesPerSecond = rate; return this; }
        /** The checksum storage enforces (default {@link Checksum#SHA256}). */
        public Builder checksum(Checksum checksum) { this.checksum = checksum; return this; }
        /** The AWS SDK's own request checksums (default {@code WHEN_REQUIRED} with an endpoint,
         *  {@code WHEN_SUPPORTED} for AWS). */
        public Builder requestChecksumCalculation(RequestChecksumCalculation calculation) {
            this.requestChecksumCalculation = calculation;
            return this;
        }

        /** Build the configured {@link S3Storage}. */
        public S3Storage build() { return new S3Storage(this); }
    }
}
