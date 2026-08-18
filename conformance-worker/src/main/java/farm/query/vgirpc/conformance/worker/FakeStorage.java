// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.conformance.worker;

import farm.query.vgirpc.external.ExternalStorage;
import farm.query.vgirpc.external.UploadUrlProvider;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
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
 */
final class FakeStorage implements ExternalStorage, UploadUrlProvider {

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

    @Override
    public UploadUrl generateUploadUrl() throws IOException {
        try {
            HttpRequest alloc = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/alloc"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString("{}"))
                    .build();
            HttpResponse<String> allocResp = sendWithRetry(alloc, HttpResponse.BodyHandlers.ofString());
            if (allocResp.statusCode() / 100 != 2) {
                throw new IOException("fake-storage /alloc failed: " + allocResp.statusCode());
            }
            String legacyUrl = extractUrl(OBJECT_URL, allocResp.body(), "object_url", null);
            String uploadUrl = extractUrl(UPLOAD_URL, allocResp.body(), "upload_url", legacyUrl);
            String downloadUrl = extractUrl(DOWNLOAD_URL, allocResp.body(), "download_url", legacyUrl);
            return new UploadUrl(uploadUrl, downloadUrl, Instant.now().plus(Duration.ofHours(1)));
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted during fake-storage alloc", ie);
        }
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
