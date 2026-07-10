// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import com.github.luben.zstd.Zstd;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Locale;
import java.util.zip.GZIPInputStream;

/**
 * Decoding of HTTP bodies by their {@code Content-Encoding}.
 *
 * <p>Intended for an intermediary (proxy, gateway, test harness) that must read
 * a compressed request or response body to inspect or rewrite it, without
 * standing up an {@link HttpServer}. Handles the codings vgirpc speaks
 * ({@code zstd}, {@code gzip}); mirrors vgi-rpc's {@code http.decode_content_encoding}.
 */
public final class ContentCodec {

    private ContentCodec() {}

    /**
     * Decode a body per its {@code Content-Encoding}, or return it unchanged.
     *
     * <p>The header may list several codings applied in order, which are decoded
     * in reverse. Unknown and {@code identity} codings are left as-is.
     *
     * @param data the raw (possibly compressed) body bytes
     * @param contentEncoding the {@code Content-Encoding} header value, or {@code null}
     * @param maxOutputSize per-coding decompression output cap; {@code <= 0} for no cap
     * @return the decoded body bytes (the input unchanged when nothing applies)
     * @throws IOException if a body fails to decompress, or exceeds {@code maxOutputSize}
     */
    public static byte[] decode(byte[] data, String contentEncoding, long maxOutputSize)
            throws IOException {
        if (contentEncoding == null || contentEncoding.isBlank()) return data;
        String[] codings = contentEncoding.split(",");
        byte[] result = data;
        for (int i = codings.length - 1; i >= 0; i--) {
            String name = codings[i].trim().toLowerCase(Locale.ROOT);
            if (name.isEmpty()) continue;
            if (MediaTypes.ZSTD.equals(name)) {
                result = zstdDecompress(result);
            } else if (MediaTypes.GZIP.equals(name)) {
                result = gzipDecompress(result, maxOutputSize);
            }
            // identity / unknown coding — leave as-is
        }
        return result;
    }

    private static byte[] zstdDecompress(byte[] data) throws IOException {
        long size = Zstd.getFrameContentSize(data);
        if (size <= 0) throw new IOException("zstd frame has unknown size");
        byte[] out = new byte[(int) size];
        long ret = Zstd.decompress(out, data);
        if (Zstd.isError(ret)) throw new IOException("zstd decompress failed: " + Zstd.getErrorName(ret));
        return out;
    }

    private static byte[] gzipDecompress(byte[] data, long maxOutput) throws IOException {
        try (GZIPInputStream gz = new GZIPInputStream(new ByteArrayInputStream(data));
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] chunk = new byte[8192];
            long total = 0;
            int n;
            while ((n = gz.read(chunk)) > 0) {
                total += n;
                if (maxOutput > 0 && total > maxOutput) {
                    throw new IOException("gzip body exceeds " + maxOutput + " bytes");
                }
                out.write(chunk, 0, n);
            }
            return out.toByteArray();
        }
    }
}
