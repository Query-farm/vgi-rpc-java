// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.transport.RpcTransport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Regression test for the {@code RpcConnection} client over a <em>reused</em>
 * (persistent) transport — subprocess / pipe / Unix socket, where
 * {@code transport.reader()} returns the same stream for every call.
 *
 * <p>Each unary response is framed as an Arrow IPC stream
 * {@code [schema][batch][EOS]}. {@code doUnary} must consume the trailing EOS
 * marker after decoding the result; otherwise the next call's
 * {@link farm.query.vgirpc.wire.IpcStreamReader} reads the stale EOS first and
 * fails with "Unexpected end of input. Missing schema". Before the
 * EOS-drain fix, call #2 here failed every time.</p>
 */
final class RpcConnectionReuseTest {

    /** Minimal service: a few unary methods, no streaming. */
    public interface EchoService {
        String echo(String value);
        long add(long a, long b);
        Optional<String> echoOptional(Optional<String> value);
    }

    public static final class EchoImpl implements EchoService {
        @Override public String echo(String value) { return value; }
        @Override public long add(long a, long b) { return a + b; }
        @Override public Optional<String> echoOptional(Optional<String> value) { return value; }
    }

    @Test
    @Timeout(20)
    void multipleUnaryCallsOverOneReusedTransport() throws Exception {
        RpcServer server = new RpcServer(EchoService.class, new EchoImpl());

        // In-process pipe transport: client writes clientOut / reads serverOut,
        // server writes serverOut / reads clientOut. The streams persist across
        // calls — exactly the condition that triggers the stale-EOS bug.
        PipedOutputStream clientOut = new PipedOutputStream();
        PipedInputStream serverIn = new PipedInputStream(clientOut, 1 << 16);
        PipedOutputStream serverOut = new PipedOutputStream();
        PipedInputStream clientIn = new PipedInputStream(serverOut, 1 << 16);

        RpcTransport serverTransport = new InProcessTransport(serverIn, serverOut);
        RpcTransport clientTransport = new InProcessTransport(clientIn, clientOut);

        Thread serverThread = new Thread(() -> server.serve(serverTransport), "rpc-server");
        serverThread.setDaemon(true);
        serverThread.start();

        try (RpcConnection conn = new RpcConnection(clientTransport)) {
            EchoService proxy = conn.proxy(EchoService.class);
            // Several sequential calls on ONE connection. Call #2 onward would
            // fail with "Missing schema" before the doUnary EOS-drain fix.
            assertEquals("one", proxy.echo("one"));
            assertEquals("two", proxy.echo("two"));
            assertEquals(7L, proxy.add(3L, 4L));
            assertEquals("three", proxy.echo("three"));
            assertEquals(100L, proxy.add(60L, 40L));
            // An Optional<T>-returning method must round-trip an absent value as
            // Optional.empty(), not a raw null.
            assertEquals(Optional.of("present"), proxy.echoOptional(Optional.of("present")));
            assertEquals(Optional.empty(), proxy.echoOptional(Optional.empty()));
        } finally {
            clientTransport.close();
            serverThread.join(2000);
        }
    }

    private static final class InProcessTransport implements RpcTransport {
        private final InputStream in;
        private final OutputStream out;

        InProcessTransport(InputStream in, OutputStream out) {
            this.in = in;
            this.out = out;
        }

        @Override public InputStream reader() { return in; }
        @Override public OutputStream writer() { return out; }

        @Override public void close() {
            try { out.flush(); } catch (Exception ignore) { /* best-effort */ }
            try { out.close(); } catch (Exception ignore) { /* best-effort */ }
            try { in.close(); } catch (Exception ignore) { /* best-effort */ }
        }
    }
}
