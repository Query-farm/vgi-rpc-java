// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.external;

import com.github.luben.zstd.Zstd;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * HTTP fetcher for external-location pointer batches. Uses the JDK
 * {@link HttpClient}; when {@code Content-Encoding: zstd} is on the response
 * the body is transparently decompressed. When the configured maximum range
 * parallelism is > 1 and the server advertises {@code Accept-Ranges: bytes}
 * with a known {@code Content-Length}, the body is fetched in parallel chunks
 * and reassembled.
 */
public final class ExternalFetcher {

    private final ExternalLocationConfig config;
    private final HttpClient client;

    public ExternalFetcher(ExternalLocationConfig config) {
        this.config = config;
        this.client = HttpClient.newBuilder()
                .connectTimeout(config.httpTimeout())
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    /**
     * Fetch {@code url}, validating against {@link ExternalLocationConfig#urlValidator()}.
     * Returns the decompressed IPC bytes. If {@code expectedSha256Hex} is non-null,
     * verifies the SHA-256 of the decompressed bytes matches.
     */
    public byte[] fetch(URI url, String expectedSha256Hex) throws IOException {
        if (config.urlValidator() != null) config.urlValidator().accept(url);

        Throwable lastError = null;
        int attempts = config.maxRetries() + 1;
        for (int attempt = 0; attempt < attempts; attempt++) {
            try {
                byte[] body = fetchOnce(url);
                if (body.length > config.maxFetchBytes()) {
                    throw new IOException("External fetch exceeded maxFetchBytes (" +
                            body.length + " > " + config.maxFetchBytes() + ")");
                }
                if (expectedSha256Hex != null) {
                    verifySha256(body, expectedSha256Hex, url);
                }
                return body;
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
        throw new IOException("External fetch failed after " + attempts + " attempts: " + url, lastError);
    }

    private byte[] fetchOnce(URI url) throws IOException {
        // Fast path: no ranges.
        if (config.maxRangeParallelism() <= 1) {
            return fetchSingle(url);
        }
        return fetchRanged(url);
    }

    private byte[] fetchSingle(URI url) throws IOException {
        try {
            HttpRequest req = HttpRequest.newBuilder(url)
                    .timeout(config.httpTimeout())
                    .header("Accept-Encoding", "zstd, identity")
                    .GET().build();
            HttpResponse<byte[]> resp = client.send(req, HttpResponse.BodyHandlers.ofByteArray());
            if (resp.statusCode() / 100 != 2) {
                throw new IOException("HTTP " + resp.statusCode() + " fetching " + url);
            }
            byte[] body = resp.body();
            return maybeDecompress(body, resp.headers().firstValue("Content-Encoding").orElse(null));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted", e);
        }
    }

    private byte[] fetchRanged(URI url) throws IOException {
        try {
            // HEAD probe.
            HttpResponse<Void> head = client.send(
                    HttpRequest.newBuilder(url).timeout(config.httpTimeout()).method("HEAD",
                            HttpRequest.BodyPublishers.noBody()).build(),
                    HttpResponse.BodyHandlers.discarding());
            long length = head.headers().firstValueAsLong("Content-Length").orElse(-1);
            String accept = head.headers().firstValue("Accept-Ranges").orElse("");
            if (length <= 0 || !accept.equalsIgnoreCase("bytes")) {
                return fetchSingle(url);
            }
            if (length > config.maxFetchBytes()) {
                throw new IOException("External fetch would exceed maxFetchBytes (" +
                        length + " > " + config.maxFetchBytes() + ")");
            }

            int parallelism = Math.max(1, Math.min(config.maxRangeParallelism(), (int) Math.min(length / 65_536 + 1, 32)));
            long chunkSize = (length + parallelism - 1) / parallelism;

            CompletableFuture<byte[]>[] futures = new CompletableFuture[parallelism];
            for (int i = 0; i < parallelism; i++) {
                long start = i * chunkSize;
                long end = Math.min(start + chunkSize - 1, length - 1);
                String range = "bytes=" + start + "-" + end;
                HttpRequest req = HttpRequest.newBuilder(url)
                        .timeout(config.httpTimeout())
                        .header("Range", range)
                        .GET().build();
                futures[i] = client.sendAsync(req, HttpResponse.BodyHandlers.ofByteArray())
                        .thenApply(r -> {
                            if (r.statusCode() / 100 != 2) {
                                throw new RuntimeException("HTTP " + r.statusCode() + " on range " + range);
                            }
                            return r.body();
                        });
            }

            byte[] out = new byte[(int) length];
            int pos = 0;
            for (CompletableFuture<byte[]> f : futures) {
                byte[] chunk = f.get();
                System.arraycopy(chunk, 0, out, pos, chunk.length);
                pos += chunk.length;
            }
            // Range responses don't carry Content-Encoding reliably per-range; trust HEAD.
            String encoding = head.headers().firstValue("Content-Encoding").orElse(null);
            return maybeDecompress(out, encoding);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted", e);
        } catch (ExecutionException e) {
            throw new IOException("range fetch failed: " + e.getCause(), e.getCause());
        }
    }

    private static byte[] maybeDecompress(byte[] body, String contentEncoding) throws IOException {
        if (contentEncoding == null) return body;
        if (contentEncoding.equalsIgnoreCase("zstd")) {
            long size = Zstd.getFrameContentSize(body);
            if (size <= 0 || size > Integer.MAX_VALUE) {
                throw new IOException("zstd frame has unknown or unreasonable size");
            }
            byte[] out = new byte[(int) size];
            long r = Zstd.decompress(out, body);
            if (Zstd.isError(r)) throw new IOException("zstd decompression failed: " + Zstd.getErrorName(r));
            return out;
        }
        if (contentEncoding.equalsIgnoreCase("identity")) return body;
        throw new IOException("unsupported Content-Encoding on external fetch: " + contentEncoding);
    }

    private static void verifySha256(byte[] body, String expectedHex, URI url) throws IOException {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            String actual = HexFormat.of().formatHex(md.digest(body));
            if (!actual.equalsIgnoreCase(expectedHex)) {
                throw new IOException("SHA-256 mismatch for " + url + ": expected " + expectedHex + ", got " + actual);
            }
        } catch (Exception e) {
            if (e instanceof IOException io) throw io;
            throw new IOException("SHA-256 verification failed: " + e.getMessage(), e);
        }
    }
}
