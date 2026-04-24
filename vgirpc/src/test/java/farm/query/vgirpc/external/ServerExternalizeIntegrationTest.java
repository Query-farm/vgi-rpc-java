// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.external;

import com.sun.net.httpserver.HttpServer;
import farm.query.vgirpc.RpcConnection;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.transport.RpcTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * End-to-end: a {@link RpcServer} with externalisation configured returns a
 * large unary result as a pointer batch, and a client with the matching
 * {@link LocationResolver} transparently resolves it — no code path in the
 * service impl is aware of externalisation.
 */
final class ServerExternalizeIntegrationTest {

    public interface BigEchoService {
        String echo(String value);
        String produceLarge();
    }

    public static final class BigEchoImpl implements BigEchoService {
        @Override public String echo(String value) { return value; }
        @Override public String produceLarge() {
            // Large enough to force externalisation at threshold=16 bytes.
            return "X".repeat(4096);
        }
    }

    private HttpServer storageHttp;
    private int storagePort;
    private final Map<String, byte[]> objects = new ConcurrentHashMap<>();

    @BeforeEach
    void start() throws Exception {
        storageHttp = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        storageHttp.createContext("/obj/", exchange -> {
            String key = exchange.getRequestURI().getPath().substring("/obj/".length());
            byte[] body = objects.get(key);
            if (body == null) { exchange.sendResponseHeaders(404, -1); exchange.close(); return; }
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.getResponseBody().close();
        });
        storageHttp.start();
        storagePort = storageHttp.getAddress().getPort();
    }

    @AfterEach
    void stop() { if (storageHttp != null) storageHttp.stop(0); }

    /** In-memory storage exposed via the test HTTP server. */
    private final class MapStorage implements ExternalStorage {
        @Override
        public URI upload(byte[] body, String contentEncoding) {
            String key = UUID.randomUUID().toString();
            objects.put(key, body);
            return URI.create("http://127.0.0.1:" + storagePort + "/obj/" + key);
        }
    }

    @Test
    void large_unary_result_is_externalised_and_resolved() throws Exception {
        ExternalLocationConfig cfg = ExternalLocationConfig.builder()
                .storage(new MapStorage())
                .thresholdBytes(16) // force externalisation for anything non-trivial
                .urlValidator(ExternalLocationConfig.permissiveValidator())
                .build();

        RpcServer server = new RpcServer(BigEchoService.class, new BigEchoImpl());
        server.setExternalConfig(cfg);

        // In-process pipe transports: client writes to clientOut / reads serverOut, server
        // writes to serverOut / reads clientOut.
        PipedOutputStream clientOut = new PipedOutputStream();
        PipedInputStream serverIn = new PipedInputStream(clientOut, 1 << 16);
        PipedOutputStream serverOut = new PipedOutputStream();
        PipedInputStream clientIn = new PipedInputStream(serverOut, 1 << 16);

        RpcTransport serverTransport = new InProcessTransport(serverIn, serverOut);
        RpcTransport clientTransport = new InProcessTransport(clientIn, clientOut);

        Thread serverThread = new Thread(() -> server.serve(serverTransport), "rpc-server");
        serverThread.setDaemon(true);
        serverThread.start();

        try (RpcConnection conn = new RpcConnection(clientTransport, m -> {}, cfg)) {
            BigEchoService proxy = conn.proxy(BigEchoService.class);
            String out = proxy.produceLarge();
            assertEquals(4096, out.length());
            // The service uploaded exactly one pointer, the client fetched it back.
            assertEquals(1, objects.size());
        } finally {
            clientTransport.close();
            serverThread.join(2000);
        }
    }

    private static final class InProcessTransport implements RpcTransport {
        private final InputStream in;
        private final OutputStream out;
        InProcessTransport(InputStream in, OutputStream out) { this.in = in; this.out = out; }
        @Override public InputStream reader() { return in; }
        @Override public OutputStream writer() { return out; }
        @Override public void close() {
            try { out.flush(); } catch (Exception ignore) {}
            try { out.close(); } catch (Exception ignore) {}
            try { in.close(); } catch (Exception ignore) {}
        }
    }
}
