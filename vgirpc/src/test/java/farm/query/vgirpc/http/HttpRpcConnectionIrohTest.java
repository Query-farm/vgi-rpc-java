// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.transport.IrohDispatchCertainty;
import farm.query.vgirpc.transport.IrohEndpoint;
import farm.query.vgirpc.transport.IrohErrorCategory;
import farm.query.vgirpc.transport.IrohErrorStage;
import farm.query.vgirpc.transport.IrohHttpRequest;
import farm.query.vgirpc.transport.IrohHttpResponse;
import farm.query.vgirpc.transport.IrohHttpTransport;
import farm.query.vgirpc.transport.IrohTransportException;
import farm.query.vgirpc.transport.IrohTransportOptions;
import farm.query.vgirpc.transport.IrohTransportProvider;
import farm.query.vgirpc.transport.RpcTransport;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

final class HttpRpcConnectionIrohTest {
    private static final String ID =
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    @Test
    void httpiReusesHttpStateMachineAndOwnsProvider() throws Exception {
        FakeProvider provider = new FakeProvider(false);
        HttpRpcConnection connection = HttpRpcConnection.irohBuilder(
                        "httpi://" + ID + "/vgi", IrohTransportOptions.defaults(), provider)
                .bearerToken("secret")
                .buildIroh();

        byte[] result = connection.post("http://iroh.invalid/vgi/echo",
                "request".getBytes(StandardCharsets.UTF_8), "echo");
        assertArrayEquals("response".getBytes(StandardCharsets.UTF_8), result);
        assertEquals(IrohEndpoint.HTTP_ALPN, provider.endpoint.alpn());
        assertEquals(List.of("/vgi/health", "/vgi/echo"),
                provider.transport.requests.stream().map(IrohHttpRequest::path).toList());
        assertEquals(List.of("Bearer secret"), provider.transport.requests.get(1)
                .headers().get(HttpHeaders.AUTHORIZATION));

        connection.close();
        assertTrue(provider.transport.closed);
    }

    @Test
    void structuredIrohFailureRemainsTheRpcErrorCause() throws Exception {
        FakeProvider provider = new FakeProvider(true);
        HttpRpcConnection connection = HttpRpcConnection.irohBuilder(
                        "httpi://" + ID, IrohTransportOptions.defaults(), provider)
                .buildIroh();
        RpcError error = assertThrows(RpcError.class, () -> connection.post(
                "http://iroh.invalid/echo", new byte[0], "echo"));
        assertInstanceOf(IrohTransportException.class, error.getCause());
        IrohTransportException cause = (IrohTransportException) error.getCause();
        assertEquals(IrohErrorStage.READ, cause.stage());
        assertEquals(IrohDispatchCertainty.SENT, cause.dispatchCertainty());
        connection.close();
    }

    private static final class FakeProvider implements IrohTransportProvider {
        private IrohEndpoint endpoint;
        private final FakeTransport transport;

        private FakeProvider(boolean fail) { transport = new FakeTransport(fail); }

        @Override public RpcTransport openArrowMux(IrohEndpoint endpoint,
                                                   IrohTransportOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override public IrohHttpTransport openHttp(IrohEndpoint endpoint,
                                                    IrohTransportOptions options) {
            this.endpoint = endpoint;
            return transport;
        }
    }

    private static final class FakeTransport implements IrohHttpTransport {
        private final List<IrohHttpRequest> requests = new ArrayList<>();
        private final boolean fail;
        private boolean closed;

        private FakeTransport(boolean fail) { this.fail = fail; }

        @Override public IrohHttpResponse execute(IrohHttpRequest request) throws IOException {
            requests.add(request);
            if (fail) {
                throw new IrohTransportException("read failed", IrohErrorStage.READ,
                        IrohErrorCategory.CONNECTION_RESET, IrohDispatchCertainty.SENT);
            }
            Map<String, List<String>> headers = request.method().equals("OPTIONS")
                    ? Map.of(HttpServer.ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER, List.of("true"))
                    : Map.of(
                            HttpServer.ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER, List.of("true"),
                            HttpHeaders.CONTENT_TYPE, List.of(HttpServer.ARROW_CONTENT_TYPE));
            byte[] body = request.method().equals("OPTIONS") ? new byte[0]
                    : "response".getBytes(StandardCharsets.UTF_8);
            return new IrohHttpResponse(request.method().equals("OPTIONS") ? 204 : 200,
                    headers, body);
        }

        @Override public void close() { closed = true; }
    }
}
