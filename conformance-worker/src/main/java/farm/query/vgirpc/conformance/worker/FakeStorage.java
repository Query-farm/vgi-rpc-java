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

    /** Minimal extractor for the {@code "object_url": "..."} field. */
    private static final Pattern OBJECT_URL = Pattern.compile("\"object_url\"\\s*:\\s*\"([^\"]+)\"");

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
            Matcher m = OBJECT_URL.matcher(allocResp.body());
            if (!m.find()) {
                throw new IOException("fake-storage /alloc missing object_url in: " + allocResp.body());
            }
            URI objectUrl = URI.create(m.group(1));

            HttpRequest.Builder putBuilder = HttpRequest.newBuilder()
                    .uri(objectUrl)
                    .header("Content-Type", "application/octet-stream")
                    .PUT(HttpRequest.BodyPublishers.ofByteArray(body));
            if (contentEncoding != null) {
                putBuilder.header("Content-Encoding", contentEncoding);
            }
            HttpResponse<Void> putResp = sendWithRetry(putBuilder.build(), HttpResponse.BodyHandlers.discarding());
            if (putResp.statusCode() / 100 != 2) {
                throw new IOException("fake-storage PUT failed: " + putResp.statusCode());
            }
            return objectUrl;
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
            Matcher m = OBJECT_URL.matcher(allocResp.body());
            if (!m.find()) {
                throw new IOException("fake-storage /alloc missing object_url in: " + allocResp.body());
            }
            // Fake storage uses the same path for PUT and GET (HTTP method
            // disambiguation), mirroring the Python adapter.
            String url = m.group(1);
            return new UploadUrl(url, url, Instant.now().plus(Duration.ofHours(1)));
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted during fake-storage alloc", ie);
        }
    }
}
