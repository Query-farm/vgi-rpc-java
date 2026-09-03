// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.iroh

import farm.query.vgirpc.http.HttpRpcConnection
import farm.query.vgirpc.transport.IrohTransportOptions
import java.time.Duration
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test

class OfficialIrohHttpIntegrationTest {
    @Test
    fun `official provider carries typed calls bearer auth and Iroh identity`() {
        val remote = System.getenv("VGI_IROH_HTTP_TEST_ENDPOINT")
        assumeTrue(!remote.isNullOrBlank())
        val target = if (remote.startsWith("httpi://")) remote else "httpi://$remote"
        val directAddresses = System.getenv("VGI_IROH_HTTP_TEST_DIRECT_ADDRESSES")
            ?.split(',')
            ?.map(String::trim)
            ?.filter(String::isNotEmpty)
            .orEmpty()
        val options = if (directAddresses.isEmpty()) {
            IrohTransportOptions.defaults()
        } else {
            IrohTransportOptions(
                null,
                emptyList(),
                true,
                Duration.ofSeconds(30),
                Duration.ofMinutes(2),
                null,
                directAddresses,
            )
        }
        HttpRpcConnection.irohBuilder(
            target,
            options,
        ).bearerToken("java-iroh-ci-token").buildIroh().use { connection ->
            val service = connection.proxy(IrohHttpIntegrationService::class.java)
            assertEquals("typed:request", service.echo("request"))

            val identity = service.identity().split(':')
            assertEquals(7, identity.size)
            assertEquals(listOf("bearer", "java-ci", "iroh", "java-hosted-ci"), identity.take(4))
            assertEquals("cryptographic_peer", identity[5])
            assertEquals("true", identity[6], "bearer principal must be bound to the Iroh evidence")
            assertTrue(identity[4].matches(Regex("[0-9a-f]{64}")))
            val remoteId = target.removePrefix("httpi://").substringBefore('/')
            assertNotEquals(remoteId, identity[4], "worker must report the authenticated client EndpointId")
        }
    }
}
