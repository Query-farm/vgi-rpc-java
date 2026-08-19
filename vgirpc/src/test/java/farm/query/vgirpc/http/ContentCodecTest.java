// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import com.github.luben.zstd.Zstd;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ContentCodecTest {

    @Test
    void zstdDeclaredOutputIsCappedBeforeAllocation() {
        byte[] raw = new byte[256 * 1024];
        byte[] encoded = Zstd.compress(raw, 1);

        assertThrows(
                ContentCodec.OutputTooLargeException.class,
                () -> ContentCodec.decode(encoded, "zstd", 4096));
    }

    @Test
    void gzipExpansionUsesTheSameDecodedOutputCap() throws Exception {
        byte[] raw = new byte[256 * 1024];
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(bytes)) {
            gzip.write(raw);
        }

        assertThrows(
                ContentCodec.OutputTooLargeException.class,
                () -> ContentCodec.decode(bytes.toByteArray(), "gzip", 4096));
    }

    @Test
    void boundedZstdStillDecodesValidBodies() throws Exception {
        byte[] raw = "bounded zstd".repeat(128).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertArrayEquals(raw, ContentCodec.decode(Zstd.compress(raw, 1), "zstd", raw.length));
    }
}
