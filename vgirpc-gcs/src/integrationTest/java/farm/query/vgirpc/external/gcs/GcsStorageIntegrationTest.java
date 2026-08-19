// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.external.gcs;

import com.github.luben.zstd.Zstd;
import com.google.auth.Credentials;
import com.google.auth.ServiceAccountSigner;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import farm.query.vgirpc.external.ExternalFetcher;
import farm.query.vgirpc.external.ExternalLocationConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * GCS SDK integration against fake-gcs-server. The emulator exercises the
 * JSON upload and signed-URL download routes, but intentionally does not
 * validate signed-URL query parameters; the last test makes that limitation
 * executable rather than allowing this lane to imply stronger coverage.
 */
@Testcontainers
final class GcsStorageIntegrationTest {

    // Multi-architecture manifest for the 1.54.0 release.
    private static final String IMAGE = "fsouza/fake-gcs-server"
            + "@sha256:3730da0e31f7e5186a90ec4899dc2c336104e7599df400411392ef17e684c31f";
    private static final String PROJECT = "vgi-rpc-integration";
    private static final String BUCKET = "vgi-rpc-integration";
    private static final String PREFIX = "java-integration/";
    private static final int GCS_PORT = 4443;

    @Container
    private static final GenericContainer<?> FAKE_GCS = new GenericContainer<>(DockerImageName.parse(IMAGE))
            .withExposedPorts(GCS_PORT)
            .withCommand("-scheme", "http", "-port", Integer.toString(GCS_PORT),
                    "-public-host", "localhost", "-backend", "memory")
            .waitingFor(Wait.forHttp("/_internal/healthcheck").forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(1));

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();
    private static URI endpoint;
    private static Storage sdk;

    @BeforeAll
    static void createBucket() {
        endpoint = URI.create("http://" + FAKE_GCS.getHost() + ":" + FAKE_GCS.getMappedPort(GCS_PORT));
        sdk = StorageOptions.newBuilder()
                .setProjectId(PROJECT)
                .setHost(endpoint.toString())
                .setCredentials(new TestSignerCredentials())
                .build()
                .getService();
        sdk.create(BucketInfo.of(BUCKET));
    }

    @AfterAll
    static void closeSdk() throws Exception {
        if (sdk != null) sdk.close();
    }

    @Test
    void uploadPersistsBytesMetadataAndProducesFetchableRewrittenV4Url() throws Exception {
        byte[] decoded = "real fake-gcs-server payload: \u0000\u0001\u0002\n"
                .repeat(256).getBytes(StandardCharsets.UTF_8);
        byte[] encoded = Zstd.compress(decoded, 3);

        try (GcsStorage storage = GcsStorage.builder(BUCKET)
                .storage(sdk)
                .keyPrefix(PREFIX)
                .signDuration(Duration.ofMinutes(5))
                .signedUrlHost("storage.googleapis.com")
                .build()) {
            URI signedGoogleUrl = storage.upload(encoded, "zstd");

            assertEquals("storage.googleapis.com", signedGoogleUrl.getHost());
            assertTrue(signedGoogleUrl.getPath().startsWith("/" + BUCKET + "/" + PREFIX),
                    signedGoogleUrl::toString);
            assertTrue(signedGoogleUrl.getPath().endsWith(".arrow"), signedGoogleUrl::toString);
            String query = signedGoogleUrl.getRawQuery().toLowerCase(Locale.ROOT);
            assertTrue(query.contains("x-goog-algorithm=goog4-rsa-sha256"), query);
            assertTrue(query.contains("x-goog-expires=300"), query);
            assertTrue(query.contains("x-goog-signature="), query);

            String objectName = signedGoogleUrl.getPath()
                    .substring(("/" + BUCKET + "/").length());
            Blob blob = sdk.get(BlobId.of(BUCKET, objectName));
            assertNotNull(blob);
            assertArrayEquals(encoded, blob.getContent());
            assertEquals("application/vnd.apache.arrow.stream", blob.getContentType());
            assertEquals("zstd", blob.getContentEncoding());

            URI emulatorUrl = rewriteToEmulator(signedGoogleUrl);
            HttpResponse<byte[]> downloaded = get(emulatorUrl);
            assertEquals(200, downloaded.statusCode());
            assertArrayEquals(encoded, downloaded.body());
            assertEquals("application/vnd.apache.arrow.stream",
                    downloaded.headers().firstValue("Content-Type").orElseThrow());
            assertEquals("zstd", downloaded.headers().firstValue("Content-Encoding").orElseThrow());

            ExternalFetcher fetcher = new ExternalFetcher(ExternalLocationConfig.builder()
                    .urlValidator(ExternalLocationConfig.permissiveValidator())
                    .maxRetries(0)
                    .maxFetchBytes(1L << 20)
                    .maxDecompressedBytes(1L << 20)
                    .build());
            assertArrayEquals(decoded, fetcher.fetch(emulatorUrl, sha256Hex(decoded)));
        }
    }

    @Test
    void emulatorExplicitlyDoesNotValidateSignedUrlQueryParameters() throws Exception {
        byte[] expected = "signature limitation sentinel".getBytes(StandardCharsets.UTF_8);
        URI signedGoogleUrl;
        try (GcsStorage storage = GcsStorage.builder(BUCKET)
                .storage(sdk)
                .keyPrefix(PREFIX)
                .signedUrlHost("storage.googleapis.com")
                .build()) {
            signedGoogleUrl = storage.upload(expected, null);
        }

        URI valid = rewriteToEmulator(signedGoogleUrl);
        URI mutated = mutateSignature(valid);
        assertEquals(200, get(valid).statusCode());
        HttpResponse<byte[]> emulatorFalsePositive = get(mutated);
        assertEquals(200, emulatorFalsePositive.statusCode(),
                "fake-gcs-server intentionally does not validate signed-URL query parameters; "
                        + "real GCS is still required to prove signer correctness");
        assertArrayEquals(expected, emulatorFalsePositive.body());
    }

    private static HttpResponse<byte[]> get(URI uri) throws Exception {
        return HTTP.send(
                HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(10)).GET().build(),
                HttpResponse.BodyHandlers.ofByteArray());
    }

    private static URI rewriteToEmulator(URI signedGoogleUrl) {
        return URI.create(endpoint + signedGoogleUrl.getRawPath() + "?" + signedGoogleUrl.getRawQuery());
    }

    private static URI mutateSignature(URI uri) {
        String mutated = uri.toString().replaceFirst(
                "(?i)(X-Goog-Signature=)[0-9a-f]+", "$1" + "0".repeat(64));
        assertNotEquals(uri.toString(), mutated, "test URL did not contain a V4 signature");
        return URI.create(mutated);
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    /**
     * The emulator needs no authentication, while V4 URL generation needs a
     * {@link ServiceAccountSigner}. An ephemeral RSA test signer supplies both
     * contracts without checking a private key or cloud credential into the
     * repository. fake-gcs-server ignores the signature by design.
     */
    private static final class TestSignerCredentials extends Credentials implements ServiceAccountSigner {
        private final KeyPair keyPair;

        private TestSignerCredentials() {
            try {
                KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
                generator.initialize(2048);
                keyPair = generator.generateKeyPair();
            } catch (GeneralSecurityException impossible) {
                throw new IllegalStateException("RSA unavailable", impossible);
            }
        }

        @Override
        public String getAuthenticationType() {
            return NoCredentials.getInstance().getAuthenticationType();
        }

        @Override
        public Map<String, List<String>> getRequestMetadata(URI uri) {
            return Map.of();
        }

        @Override
        public boolean hasRequestMetadata() {
            return false;
        }

        @Override
        public boolean hasRequestMetadataOnly() {
            return true;
        }

        @Override
        public void refresh() {
            // No remote credential state.
        }

        @Override
        public String getAccount() {
            return "vgi-rpc-integration@local.invalid";
        }

        @Override
        public byte[] sign(byte[] toSign) {
            try {
                Signature signature = Signature.getInstance("SHA256withRSA");
                signature.initSign(keyPair.getPrivate());
                signature.update(toSign);
                return signature.sign();
            } catch (GeneralSecurityException impossible) {
                throw new IllegalStateException("RSA signing unavailable", impossible);
            }
        }
    }
}
