// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.iroh

import farm.query.vgirpc.http.HttpRpcConnection
import farm.query.vgirpc.transport.IrohTransportOptions
import java.nio.charset.StandardCharsets
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test

class OfficialIrohHttpIntegrationTest {
    @Test
    fun `official provider interoperates with iroh-http-core when configured`() {
        val remote = System.getenv("VGI_IROH_HTTP_TEST_ENDPOINT")
        assumeTrue(!remote.isNullOrBlank())
        HttpRpcConnection.irohBuilder(
            remote,
            IrohTransportOptions.defaults(),
        ).buildIroh().use { connection ->
            val post = HttpRpcConnection::class.java.getDeclaredMethod(
                "post",
                String::class.java,
                ByteArray::class.java,
                String::class.java,
            )
            post.isAccessible = true
            val result = post.invoke(
                connection,
                "http://iroh.invalid/echo",
                "request".toByteArray(StandardCharsets.UTF_8),
                "echo",
            ) as ByteArray
            assertArrayEquals("response".toByteArray(StandardCharsets.UTF_8), result)
        }
    }
}
