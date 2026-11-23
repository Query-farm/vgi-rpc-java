// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.conformance.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import farm.query.vgirpc.external.ExternalStorage;
import farm.query.vgirpc.external.ObjectMissingException;
import farm.query.vgirpc.external.ObjectStore;
import farm.query.vgirpc.external.UploadUrlProvider;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link ExternalStorage} adapter that talks to the Python
 * {@code vgi_rpc.conformance.fake_storage} HTTP service. Used by the
 * conformance HTTP worker when started with {@code --fake-storage URL}.
 *
 * <p>The 4-endpoint contract is documented in
 * {@code vgi_rpc/conformance/fake_storage.py}: this class implements
 * the upload side as {@code POST /alloc} followed by {@code PUT} to the
 * returned {@code object_url}.</p>
 *
 * <p>The protocol-3.4 object API: every {@code /alloc} response carries {@code object_id};
 * {@link #putObject} is a sized allocation with {@code "purpose": "object"} (counted as
 * {@code object_puts}) plus a verified PUT, and {@link #get(String, long)} is
 * {@code GET /objects/{id}} (counted as {@code object_gets}, 404 when absent).</p>
 */
final class FakeStorage implements ExternalStorage, UploadUrlProvider, ObjectStore {

    /** Minimal extractors for the fake storage allocation response. */
    private static final Pattern OBJECT_URL = Pattern.compile("\"object_url\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern UPLOAD_URL = Pattern.compile("\"upload_url\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern DOWNLOAD_URL = Pattern.compile("\"download_url\"\\s*:\\s*\"([^\"]+)\"");

    /**
     * Attempts per request against the fake-storage service.
     *
     * <p>The service is Python's {@code fake_storage.serve_in_thread}, a
     * single-threaded {@code wsgiref.simple_server} that closes the connection after
     * every response. This client pools connections, so it will periodically write a
     * request onto a socket the peer has already closed and see it fail as
     * {@code "HTTP/1.1 header parser received no bytes"} — a race between the pool
     * and the peer's close, not a real transport error.
     *
     * <p>Retrying is safe for both operations: a repeated {@code /alloc} simply
     * allocates a fresh object URL, and the {@code PUT} is idempotent (same URL,
     * same bytes). The library-side {@code ExternalFetcher} already retries for the
     * same reason; this is the harness catching up.
     */
    private static final int MAX_ATTEMPTS = 3;

    /** Pause between attempts, giving the peer time to accept from a backlog of 5. */
    private static final Duration RETRY_DELAY = Duration.ofMillis(50);

    private static final ObjectMapper JSON = new ObjectMapper();

    private final String baseUrl;
    private final HttpClient http;

    FakeStorage(String baseUrl) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    /**
     * Send a request, retrying a transport-level failure on a fresh connection.
     *
     * <p>Only {@link IOException} is retried. A non-2xx response is a real answer from
     * the service and is left to the caller — retrying it would mask a genuine fault.
     */
    private <T> HttpResponse<T> sendWithRetry(HttpRequest request, HttpResponse.BodyHandler<T> handler)
            throws IOException, InterruptedException {
        IOException last = null;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                return http.send(request, handler);
            } catch (IOException e) {
                last = e;
                if (attempt < MAX_ATTEMPTS) {
                    Thread.sleep(RETRY_DELAY.toMillis());
                }
            }
        }
        throw new IOException(
                "fake-storage request to " + request.uri() + " failed after " + MAX_ATTEMPTS + " attempts", last);
    }

    @Override
    public URI upload(byte[] body, String contentEncoding) throws IOException {
        try {
            String allocBody = contentEncoding == null ? "{}" :
                    "{\"content_encoding\":\"" + contentEncoding + "\"}";
            HttpRequest alloc = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/alloc"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(allocBody))
                    .build();
            HttpResponse<String> allocResp = sendWithRetry(alloc, HttpResponse.BodyHandlers.ofString());
            if (allocResp.statusCode() / 100 != 2) {
                throw new IOException("fake-storage /alloc failed: " + allocResp.statusCode());
            }
            String legacyUrl = extractUrl(OBJECT_URL, allocResp.body(), "object_url", null);
            URI uploadUrl = URI.create(extractUrl(UPLOAD_URL, allocResp.body(), "upload_url", legacyUrl));
            URI downloadUrl = URI.create(extractUrl(DOWNLOAD_URL, allocResp.body(), "download_url", legacyUrl));

            HttpRequest.Builder putBuilder = HttpRequest.newBuilder()
                    .uri(uploadUrl)
                    .header("Content-Type", "application/octet-stream")
                    .PUT(HttpRequest.BodyPublishers.ofByteArray(body));
            if (contentEncoding != null) {
                putBuilder.header("Content-Encoding", contentEncoding);
            }
            HttpResponse<Void> putResp = sendWithRetry(putBuilder.build(), HttpResponse.BodyHandlers.discarding());
            if (putResp.statusCode() / 100 != 2) {
                throw new IOException("fake-storage PUT failed: " + putResp.statusCode());
            }
            return downloadUrl;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted during fake-storage upload", ie);
        }
    }

    /**
     * Allocate a client upload slot (upload-URL protocol v2).
     *
     * <p>The announced size and SHA-256 ride in the {@code /alloc} body so a
     * fake storage that verifies uploads can refuse a mismatched PUT; any
     * {@code upload_headers} object it returns is handed to the client
     * verbatim.</p>
     */
    @Override
    public UploadUrl generateUploadUrl(long size, String sha256Hex) throws IOException {
        try {
            // A sized allocation: fake_storage then verifies the PUT like S3/R2.
            String sha256B64 = java.util.Base64.getEncoder()
                    .encodeToString(java.util.HexFormat.of().parseHex(sha256Hex));
            String allocBody = "{\"size\":" + size + ",\"sha256_b64\":\"" + sha256B64 + "\"}";
            HttpRequest alloc = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/alloc"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(allocBody))
                    .build();
            HttpResponse<String> allocResp = sendWithRetry(alloc, HttpResponse.BodyHandlers.ofString());
            if (allocResp.statusCode() / 100 != 2) {
                throw new IOException("fake-storage /alloc failed: " + allocResp.statusCode());
            }
            String legacyUrl = extractUrl(OBJECT_URL, allocResp.body(), "object_url", null);
            String uploadUrl = extractUrl(UPLOAD_URL, allocResp.body(), "upload_url", legacyUrl);
            String downloadUrl = extractUrl(DOWNLOAD_URL, allocResp.body(), "download_url", legacyUrl);
            return new UploadUrl(uploadUrl, downloadUrl, Instant.now().plus(Duration.ofHours(1)),
                    uploadHeaders(allocResp.body()), objectId(allocResp.body()));
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted during fake-storage alloc", ie);
        }
    }

    @Override
    public String putObject(byte[] data) throws IOException {
        try {
            String sha256B64 = java.util.Base64.getEncoder().encodeToString(
                    java.security.MessageDigest.getInstance("SHA-256").digest(data));
            String allocBody = "{\"size\":" + data.length + ",\"sha256_b64\":\"" + sha256B64
                    + "\",\"purpose\":\"object\"}";
            HttpRequest alloc = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/alloc"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(allocBody))
                    .build();
            HttpResponse<String> allocResp = sendWithRetry(alloc, HttpResponse.BodyHandlers.ofString());
            if (allocResp.statusCode() / 100 != 2) {
                throw new IOException("fake-storage /alloc failed: " + allocResp.statusCode());
            }
            String legacyUrl = extractUrl(OBJECT_URL, allocResp.body(), "object_url", null);
            URI uploadUrl = URI.create(extractUrl(UPLOAD_URL, allocResp.body(), "upload_url", legacyUrl));
            HttpRequest.Builder put = HttpRequest.newBuilder()
                    .uri(uploadUrl)
                    .header("Content-Type", "application/octet-stream")
                    .PUT(HttpRequest.BodyPublishers.ofByteArray(data));
            uploadHeaders(allocResp.body()).forEach(put::header);
            HttpResponse<Void> putResp = sendWithRetry(put.build(), HttpResponse.BodyHandlers.discarding());
            if (putResp.statusCode() / 100 != 2) {
                throw new IOException("fake-storage PUT failed: " + putResp.statusCode());
            }
            return objectId(allocResp.body());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted during fake-storage put_object", ie);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public byte[] get(String objectId, long maxBytes) throws IOException, ObjectMissingException {
        try {
            HttpRequest get = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/objects/" + objectId))
                    .GET()
                    .build();
            HttpResponse<byte[]> resp = sendWithRetry(get, HttpResponse.BodyHandlers.ofByteArray());
            if (resp.statusCode() == 404) throw new ObjectMissingException("fake storage object is gone");
            if (resp.statusCode() / 100 != 2) {
                throw new IOException("fake-storage GET /objects failed: " + resp.statusCode());
            }
            byte[] data = resp.body();
            if (data.length > maxBytes) {
                throw new IOException("object is " + data.length + " bytes, over max_fetch_bytes (" + maxBytes + ")");
            }
            return data;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted during fake-storage get", ie);
        }
    }

    /** The {@code object_id} of an {@code /alloc} response. */
    private static String objectId(String body) throws IOException {
        JsonNode node = JSON.readTree(body).path("object_id");
        if (!node.isTextual() || node.asText().isEmpty()) {
            throw new IOException("fake-storage /alloc response has no object_id");
        }
        return node.asText();
    }

    /** The {@code upload_headers} object of an {@code /alloc} response, or none. */
    private static Map<String, String> uploadHeaders(String body) throws IOException {
        JsonNode node = JSON.readTree(body).path("upload_headers");
        Map<String, String> out = new LinkedHashMap<>();
        if (node.isObject()) node.properties().forEach(e -> out.put(e.getKey(), e.getValue().asText()));
        return out;
    }

    /** Extract a JSON URL field, falling back to the legacy dual-method URL. */
    private static String extractUrl(Pattern pattern, String body, String field, String fallback)
            throws IOException {
        Matcher matcher = pattern.matcher(body);
        if (matcher.find()) return matcher.group(1);
        if (fallback != null) return fallback;
        throw new IOException("fake-storage /alloc missing " + field + " in: " + body);
    }
}
