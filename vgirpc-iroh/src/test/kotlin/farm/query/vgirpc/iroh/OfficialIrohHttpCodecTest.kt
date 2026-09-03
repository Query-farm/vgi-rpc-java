// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.iroh

import farm.query.vgirpc.transport.IrohHttpRequest
import farm.query.vgirpc.transport.IrohTransportException
import java.nio.charset.StandardCharsets
import java.time.Duration
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class OfficialIrohHttpCodecTest {
    @Test
    fun `request uses ordinary HTTP 1 framing and endpoint host`() {
        val encoded = encodeHttpRequest(
            IrohHttpRequest(
                "POST",
                "/vgi/echo",
                mapOf("Authorization" to listOf("Bearer token")),
                "abc".toByteArray(),
                Duration.ofSeconds(1),
                1024,
            ),
            "0123456789abcdef",
        ).toString(StandardCharsets.ISO_8859_1)
        assertEquals(
            "POST /vgi/echo HTTP/1.1\r\n" +
                "Authorization: Bearer token\r\n" +
                "Host: 0123456789abcdef\r\n" +
                "Content-Length: 3\r\n" +
                "\r\nabc",
            encoded,
        )
    }

    @Test
    fun `response parser preserves duplicate headers and decodes chunks`() {
        val wire = (
            "HTTP/1.1 200 OK\r\n" +
                "Transfer-Encoding: chunked\r\n" +
                "X-Test: one\r\n" +
                "X-Test: two\r\n\r\n" +
                "3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n"
            ).toByteArray(StandardCharsets.ISO_8859_1)
        val response = parseHttpResponse(wire, 5)
        assertEquals(200, response.status())
        assertEquals(listOf("one", "two"), response.headerValues("X-Test"))
        assertArrayEquals("abcde".toByteArray(), response.body())
    }

    @Test
    fun `response parser enforces decoded body limit`() {
        val wire = "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ntest"
            .toByteArray(StandardCharsets.ISO_8859_1)
        assertThrows(IrohTransportException::class.java) { parseHttpResponse(wire, 3) }
    }
}
