// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.external;

import com.github.luben.zstd.Zstd;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * HTTP fetcher for external-location pointer batches. Uses the JDK
 * {@link HttpClient}; when {@code Content-Encoding: zstd} is on the response
 * the body is transparently decompressed. When the configured maximum range
 * parallelism is > 1 and the server advertises {@code Accept-Ranges: bytes}
 * with a known {@code Content-Length}, the body is fetched in parallel chunks
 * and reassembled.
 */
public final class ExternalFetcher {

    private static final int MAX_REDIRECTS = 5;

    private final ExternalLocationConfig config;
    private final HttpClient client;

    /**
     * @param config configuration controlling timeouts, retries, size caps,
     *        URL validation, and range parallelism
     */
    public ExternalFetcher(ExternalLocationConfig config) {
        this.config = config;
        this.client = HttpClient.newBuilder()
                .connectTimeout(config.httpTimeout())
                // Redirects are followed manually so every target passes the
                // same URL policy before any request reaches it.
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
    }

    /**
     * Fetch {@code url}, validating against {@link ExternalLocationConfig#urlValidator()}.
     * Returns the decompressed IPC bytes. If {@code expectedSha256Hex} is non-null,
     * verifies the SHA-256 of the decompressed bytes matches.
     */
    public byte[] fetch(URI url, String expectedSha256Hex) throws IOException {
        Throwable lastError = null;
        int attempts = config.maxRetries() + 1;
        for (int attempt = 0; attempt < attempts; attempt++) {
            try {
                byte[] body = fetchOnce(url);
                if (expectedSha256Hex != null) {
                    verifySha256(body, expectedSha256Hex, url);
                }
                return body;
            } catch (NonRetryableFetchException e) {
                throw e;
            } catch (IOException e) {
                lastError = e;
                if (attempt < attempts - 1) {
                    try { Thread.sleep(config.retryDelay().toMillis()); }
                    catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("interrupted during retry", ie);
                    }
                }
            }
        }
        String detail = lastError != null && lastError.getMessage() != null
                ? ": " + lastError.getMessage()
                : "";
        // Do not retain the original exception as a cause: HTTP-client
        // exceptions may embed the full signed URL in their message, and the
        // RPC traceback serializes the entire cause chain.
        throw new IOException("External fetch failed after " + attempts + " attempts for "
                + safeUri(url) + detail);
    }

    private byte[] fetchOnce(URI url) throws IOException {
        // Fast path: no ranges.
        if (config.maxRangeParallelism() <= 1) {
            return fetchSingle(url);
        }
        return fetchRanged(url);
    }

    private byte[] fetchSingle(URI url) throws IOException {
        FetchResponse resp = sendBytesFollowingRedirects(url, null);
        return maybeDecompress(resp.body(),
                resp.headers().firstValue("Content-Encoding").orElse(null));
    }

    private byte[] fetchRanged(URI url) throws IOException {
        try {
            // HEAD probe.
            HeadResponse head = sendHeadFollowingRedirects(url);
            long length = head.headers().firstValueAsLong("Content-Length").orElse(-1);
            String accept = head.headers().firstValue("Accept-Ranges").orElse("");
            if (length <= 0 || !accept.equalsIgnoreCase("bytes")) {
                return fetchSingle(head.finalUri());
            }
            if (length > config.maxFetchBytes()) {
                throw new NonRetryableFetchException("External fetch exceeds max_fetch_bytes ("
                        + length + " > " + config.maxFetchBytes() + ") for " + safeUri(head.finalUri()));
            }

            int parallelism = Math.max(1, Math.min(config.maxRangeParallelism(), (int) Math.min(length / 65_536 + 1, 32)));
            long chunkSize = (length + parallelism - 1) / parallelism;

            List<CompletableFuture<byte[]>> futures = new ArrayList<>(parallelism);
            for (int i = 0; i < parallelism; i++) {
                long start = i * chunkSize;
                long end = Math.min(start + chunkSize - 1, length - 1);
                String range = "bytes=" + start + "-" + end;
                futures.add(CompletableFuture.supplyAsync(() -> {
                    try {
                        return sendBytesFollowingRedirects(head.finalUri(), range).body();
                    } catch (IOException e) {
                        throw new CompletionException(e);
                    }
                }));
            }

            byte[] out = new byte[(int) length];
            int pos = 0;
            for (CompletableFuture<byte[]> f : futures) {
                byte[] chunk = f.get();
                if (pos + chunk.length > out.length) {
                    throw new IOException("range responses exceeded announced Content-Length");
                }
                System.arraycopy(chunk, 0, out, pos, chunk.length);
                pos += chunk.length;
            }
            if (pos != out.length) {
                throw new IOException("range responses contained " + pos
                        + " bytes, expected " + out.length);
            }
            // Range responses don't carry Content-Encoding reliably per-range; trust HEAD.
            String encoding = head.headers().firstValue("Content-Encoding").orElse(null);
            return maybeDecompress(out, encoding);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof CompletionException ce && ce.getCause() != null) cause = ce.getCause();
            if (cause instanceof NonRetryableFetchException nonRetryable) throw nonRetryable;
            if (cause instanceof IOException io) throw io;
            throw new IOException("range fetch failed");
        }
    }

    private byte[] maybeDecompress(byte[] body, String contentEncoding) throws IOException {
        if (contentEncoding == null || contentEncoding.equalsIgnoreCase("identity")) {
            enforceDecodedCap(body.length);
            return body;
        }
        if (contentEncoding.equalsIgnoreCase("zstd")) {
            long size = Zstd.getFrameContentSize(body);
            if (size <= 0 || size > Integer.MAX_VALUE) {
                throw new IOException("zstd frame has unknown or unreasonable size");
            }
            enforceDecodedCap(size);
            byte[] out = new byte[(int) size];
            long r = Zstd.decompress(out, body);
            if (Zstd.isError(r)) throw new IOException("zstd decompression failed: " + Zstd.getErrorName(r));
            return out;
        }
        throw new IOException("unsupported Content-Encoding on external fetch: " + contentEncoding);
    }

    private void enforceDecodedCap(long size) throws NonRetryableFetchException {
        if (size > config.maxDecompressedBytes()) {
            throw new NonRetryableFetchException("External fetch exceeds max_decompressed_bytes ("
                    + size + " > " + config.maxDecompressedBytes() + ")");
        }
    }

    private static void verifySha256(byte[] body, String expectedHex, URI url) throws IOException {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            String actual = HexFormat.of().formatHex(md.digest(body));
            if (!actual.equalsIgnoreCase(expectedHex)) {
                throw new IOException("external checksum mismatch for " + safeUri(url)
                        + ": expected " + expectedHex + ", got " + actual);
            }
        } catch (Exception e) {
            if (e instanceof IOException io) throw io;
            throw new IOException("SHA-256 verification failed: " + e.getMessage(), e);
        }
    }

    private FetchResponse sendBytesFollowingRedirects(URI initial, String range) throws IOException {
        URI current = initial;
        for (int redirects = 0; ; redirects++) {
            validateUrl(current);
            HttpRequest.Builder request = HttpRequest.newBuilder(current)
                    .timeout(config.httpTimeout())
                    .header("Accept-Encoding", "zstd, identity");
            if (range != null) request.header("Range", range);
            HttpResponse<InputStream> response;
            try {
                response = client.send(request.GET().build(), HttpResponse.BodyHandlers.ofInputStream());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("external fetch interrupted");
            } catch (IOException e) {
                throw new IOException("HTTP request failed for " + safeUri(current));
            }
            int status = response.statusCode();
            if (isRedirect(status)) {
                response.body().close();
                if (redirects >= MAX_REDIRECTS) {
                    throw new NonRetryableFetchException(
                            "external redirect limit exceeded for " + safeUri(initial));
                }
                current = redirectTarget(current, response.headers());
                continue;
            }
            if (status / 100 != 2) {
                response.body().close();
                throw new IOException("HTTP " + status + " fetching " + safeUri(current));
            }
            long announced = response.headers().firstValueAsLong("Content-Length").orElse(-1);
            if (announced > config.maxFetchBytes()) {
                response.body().close();
                throw new NonRetryableFetchException("External fetch exceeds max_fetch_bytes ("
                        + announced + " > " + config.maxFetchBytes() + ") for " + safeUri(current));
            }
            try (InputStream body = response.body()) {
                return new FetchResponse(readEncodedBody(body), response.headers(), current);
            }
        }
    }

    private HeadResponse sendHeadFollowingRedirects(URI initial) throws IOException {
        URI current = initial;
        for (int redirects = 0; ; redirects++) {
            validateUrl(current);
            HttpResponse<Void> response;
            try {
                response = client.send(
                        HttpRequest.newBuilder(current).timeout(config.httpTimeout())
                                .method("HEAD", HttpRequest.BodyPublishers.noBody()).build(),
                        HttpResponse.BodyHandlers.discarding());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("external fetch interrupted");
            } catch (IOException e) {
                throw new IOException("HTTP request failed for " + safeUri(current));
            }
            int status = response.statusCode();
            if (isRedirect(status)) {
                if (redirects >= MAX_REDIRECTS) {
                    throw new NonRetryableFetchException(
                            "external redirect limit exceeded for " + safeUri(initial));
                }
                current = redirectTarget(current, response.headers());
                continue;
            }
            if (status / 100 != 2) {
                throw new IOException("HTTP " + status + " fetching " + safeUri(current));
            }
            return new HeadResponse(response.headers(), current);
        }
    }

    private byte[] readEncodedBody(InputStream input) throws IOException {
        long cap = config.maxFetchBytes();
        if (cap < 0 || cap >= Integer.MAX_VALUE) cap = Integer.MAX_VALUE - 1L;
        ByteArrayOutputStream out = new ByteArrayOutputStream((int) Math.min(cap, 64 * 1024));
        byte[] chunk = new byte[8192];
        long total = 0;
        while (true) {
            int n = input.read(chunk);
            if (n < 0) break;
            total += n;
            if (total > cap) {
                throw new NonRetryableFetchException("External fetch exceeds max_fetch_bytes (more than "
                        + config.maxFetchBytes() + ")");
            }
            out.write(chunk, 0, n);
        }
        return out.toByteArray();
    }

    private void validateUrl(URI uri) {
        try {
            if (config.urlValidator() != null) config.urlValidator().accept(uri);
        } catch (RuntimeException rejected) {
            // Preserve the public validator contract (rejection is an
            // IllegalArgumentException), but never retain the validator's
            // message or cause because either may contain a signed URL.
            throw new IllegalArgumentException("URL rejected by policy: " + safeUri(uri));
        }
    }

    private static URI redirectTarget(URI current, HttpHeaders headers) throws IOException {
        String location = headers.firstValue("Location").orElse(null);
        if (location == null) {
            throw new NonRetryableFetchException("redirect response omitted Location for " + safeUri(current));
        }
        try {
            return current.resolve(location);
        } catch (IllegalArgumentException invalid) {
            throw new NonRetryableFetchException("invalid redirect target from " + safeUri(current));
        }
    }

    private static boolean isRedirect(int status) {
        return status == 301 || status == 302 || status == 303 || status == 307 || status == 308;
    }

    static String safeUri(URI uri) {
        try {
            return new URI(uri.getScheme(), null, uri.getHost(), uri.getPort(),
                    uri.getRawPath(), null, null).toASCIIString();
        } catch (Exception ignored) {
            return "<redacted-url>";
        }
    }

    private record FetchResponse(byte[] body, HttpHeaders headers, URI finalUri) {}
    private record HeadResponse(HttpHeaders headers, URI finalUri) {}

    private static final class NonRetryableFetchException extends IOException {
        NonRetryableFetchException(String message) { super(message); }
    }
}
