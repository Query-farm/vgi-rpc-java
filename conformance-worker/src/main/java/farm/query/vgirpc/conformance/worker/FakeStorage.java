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

    private final String baseUrl;
    private final HttpClient http;

    FakeStorage(String baseUrl) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
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
            HttpResponse<String> allocResp = http.send(alloc, HttpResponse.BodyHandlers.ofString());
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
            HttpResponse<Void> putResp = http.send(putBuilder.build(), HttpResponse.BodyHandlers.discarding());
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
            HttpResponse<String> allocResp = http.send(alloc, HttpResponse.BodyHandlers.ofString());
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
