// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.transport;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Locale;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

final class IrohEndpointTest {
    private static final String ID = "0123456789abcdef".repeat(4);

    @Test void parsesCanonicalSchemes() {
        IrohEndpoint raw = IrohEndpoint.parse("iroh://" + ID);
        assertEquals(IrohEndpoint.Scheme.IROH, raw.scheme());
        assertEquals(32, raw.endpointIdBytes().length);
        assertEquals(IrohEndpoint.ARROW_MUX_ALPN, raw.alpn());

        IrohEndpoint http = IrohEndpoint.parse("httpi://" + ID + "/api/v1");
        assertEquals("/api/v1", http.basePath());
        assertEquals(IrohEndpoint.HTTP_ALPN, http.alpn());
    }

    @Test void passesCanonicalFixture() throws IOException {
        try (var input = IrohEndpointTest.class.getResourceAsStream("/iroh_transport_vectors.json")) {
            assertNotNull(input);
            JsonNode fixture = new ObjectMapper().readTree(input);
            assertEquals(IrohEndpoint.ARROW_MUX_ALPN, fixture.at("/alpns/iroh").asText());
            assertEquals(IrohEndpoint.HTTP_ALPN, fixture.at("/alpns/httpi").asText());
            for (JsonNode vector : fixture.get("uri_cases")) {
                String uri = vector.get("uri").asText();
                if (!vector.get("valid").asBoolean()) {
                    assertThrows(IrohUriException.class, () -> IrohEndpoint.parse(uri), uri);
                    continue;
                }
                IrohEndpoint endpoint = IrohEndpoint.parse(uri);
                assertEquals(vector.get("scheme").asText(), endpoint.scheme().name().toLowerCase(Locale.ROOT), uri);
                assertEquals(vector.get("base_path").asText(), endpoint.basePath(), uri);
            }
            for (JsonNode vector : fixture.get("error_cases")) {
                assertNotNull(IrohErrorStage.valueOf(vector.get("stage").asText().toUpperCase(Locale.ROOT)));
                assertNotNull(IrohErrorCategory.valueOf(vector.get("category").asText().toUpperCase(Locale.ROOT)));
                assertNotNull(IrohDispatchCertainty.valueOf(
                        vector.get("dispatch_certainty").asText().toUpperCase(Locale.ROOT)));
            }
        }
    }

    @Test void rejectsNonCanonicalUris() {
        for (String value : new String[]{
                "iroh://" + ID.toUpperCase(), "iroh://" + ID + "/",
                "iroh://user@" + ID, "iroh://" + ID + ":443",
                "httpi://" + ID + "/a//b", "httpi://" + ID + "/a/../b",
                "httpi://" + ID + "/bad%2", "httpi://" + ID + "?x=1"}) {
            assertThrows(IllegalArgumentException.class, () -> IrohEndpoint.parse(value), value);
        }
    }

    @Test void dispatchesOnlyRawThroughProvider() throws IOException {
        RpcTransport expected = new RpcTransport() {
            public java.io.InputStream reader() { return new ByteArrayInputStream(new byte[0]); }
            public java.io.OutputStream writer() { return new ByteArrayOutputStream(); }
            public void close() {}
        };
        IrohTransportProvider provider = (endpoint, options) -> {
            assertEquals(ID, endpoint.endpointId());
            assertEquals(IrohEndpoint.ARROW_MUX_ALPN, endpoint.alpn());
            return expected;
        };
        assertSame(expected, IrohTransports.connect("iroh://" + ID, null, provider));
        IrohTransportException unsupported = assertThrows(IrohTransportException.class,
                () -> IrohTransports.connect("httpi://" + ID, null, provider));
        assertEquals(IrohErrorCategory.UNSUPPORTED, unsupported.category());
        assertEquals(IrohDispatchCertainty.NOT_SENT, unsupported.dispatchCertainty());
    }
}
