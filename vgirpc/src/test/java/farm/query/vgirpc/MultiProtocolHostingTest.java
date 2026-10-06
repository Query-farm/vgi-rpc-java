// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.errors.Code;
import farm.query.vgirpc.schema.ProtocolName;
import farm.query.vgirpc.schema.ProtocolVersion;
import farm.query.vgirpc.transport.RpcTransport;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Hosting several application protocols on one server (WIRE_PROTOCOL.md §3.1).
 *
 * <p>The secondary repeats the primary's method name on purpose: dispatch is by the pair
 * (protocol, method), and a server that keyed on the bare name would answer one with the other.
 * Its reply is prefixed, so a mis-route is a wrong value rather than a coincidentally right one.
 */
@Timeout(30)
final class MultiProtocolHostingTest {

    @ProtocolName("demo.Primary.v1")
    @ProtocolVersion("2.0.0")
    public interface Primary {
        String echo_string(String value);
    }

    /** No version on purpose: a server gating every call on the primary's refuses it. */
    @ProtocolName("demo.Second.v1")
    public interface Second {
        String echo_string(String value);
        long only_here();
    }

    @ProtocolName("vgi_rpc.Shadow.v1")
    public interface Shadow {
        String echo_string(String value);
    }

    /** A second interface claiming the primary's routing key. */
    @ProtocolName("demo.Primary.v1")
    public interface Impostor {
        String echo_string(String value);
    }

    static final class PrimaryImpl implements Primary, Impostor, Shadow {
        @Override public String echo_string(String value) { return value; }
    }

    static final class SecondImpl implements Second {
        @Override public String echo_string(String value) { return "second:" + value; }
        @Override public long only_here() { return 7; }
    }

    private static RpcServer server() {
        RpcServer s = new RpcServer(Primary.class, new PrimaryImpl());
        s.setProtocolVersion("2.0.0");
        return s.addProtocol(Second.class, new SecondImpl());
    }

    // --- registration -----------------------------------------------------------------

    @Test
    void protocolsAreListedPrimaryFirstInRegistrationOrder() {
        List<String> names = server().applicationProtocols().stream()
                .map(RpcServer.ApplicationProtocol::name).toList();
        assertEquals(List.of("demo.Primary.v1", "demo.Second.v1"), names);
    }

    @Test
    void theReservedPrefixIsRefusedForAnExtraProtocol() {
        RpcServer s = server();
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> s.addProtocol(Shadow.class, new PrimaryImpl()));
        assertTrue(e.getMessage().contains("vgi_rpc."), e.getMessage());
        // Nothing was registered on the way to the refusal.
        assertEquals(2, s.applicationProtocols().size());
    }

    @Test
    void aSecondRegistrationUnderOneNameIsRefused() {
        RpcServer s = server();
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> s.addProtocol(Impostor.class, new PrimaryImpl()));
        assertTrue(e.getMessage().contains("already hosted"), e.getMessage());
        assertThrows(IllegalArgumentException.class, () -> s.addProtocol(Second.class, new SecondImpl()));
    }

    @Test
    void anImplementationMustImplementItsInterface() {
        assertThrows(IllegalArgumentException.class,
                () -> server().addProtocol(Primary.class, new SecondImpl()));
    }

    @Test
    void theSetIsFixedOnceServingBegins() throws Exception {
        RpcServer s = new RpcServer(Primary.class, new PrimaryImpl());
        try (Peer peer = Peer.start(s)) {
            assertEquals("x", peer.connection.proxy(Primary.class).echo_string("x"));
        }
        assertThrows(IllegalStateException.class, () -> s.addProtocol(Second.class, new SecondImpl()));
    }

    // --- dispatch ---------------------------------------------------------------------

    @Test
    void oneMethodNameResolvesToTwoBindings() throws Exception {
        try (Peer peer = Peer.start(server())) {
            assertEquals("hi", peer.connection.proxy(Primary.class).echo_string("hi"));
            assertEquals("second:hi", peer.connection.proxy(Second.class).echo_string("hi"));
            assertEquals(7L, peer.connection.proxy(Second.class).only_here());
        }
    }

    @Test
    void reflectionListsEverySecondaryAndDescribesIt() throws Exception {
        try (Peer peer = Peer.start(server())) {
            Introspect.ProtocolList listing = Introspect.listProtocols(peer::call);
            List<String> app = listing.protocols().stream()
                    .map(Introspect.ProtocolSummary::protocol)
                    .filter(n -> !n.startsWith("vgi_rpc.")).toList();
            assertEquals(List.of("demo.Primary.v1", "demo.Second.v1"), app);
            for (Introspect.ProtocolSummary p : listing.protocols()) {
                assertEquals(List.of(), p.features(), "features is reserved and emitted empty");
                if (p.protocol().equals("demo.Second.v1")) {
                    assertEquals("", p.protocolVersion());
                    assertEquals(64, p.protocolHash().length());
                }
            }
            Introspect.ServiceDescription d = Introspect.describe(peer::call, "demo.Second.v1", listing);
            assertEquals("demo.Second.v1", d.protocolName());
            assertEquals(java.util.Set.of("echo_string", "only_here"), d.methods().keySet());
        }
    }

    @Test
    void anAbsentMethodOnASecondaryIsUnimplemented() throws Exception {
        try (Peer peer = Peer.start(server())) {
            RpcError e = assertThrows(RpcError.class, () -> peer.raw("demo.Second.v1", "", "nope"));
            assertEquals(MethodNotImplementedError.ERROR_KIND, e.errorKind());
            assertEquals("UNIMPLEMENTED", e.errorCode());
        }
    }

    @Test
    void anUnhostedProtocolIsUnimplemented() throws Exception {
        try (Peer peer = Peer.start(server())) {
            RpcError e = assertThrows(RpcError.class, () -> peer.raw("demo.Missing.v1", "", "only_here"));
            assertEquals("protocol_not_supported", e.errorKind());
            assertEquals(Code.UNIMPLEMENTED, e.code());
        }
    }

    @Test
    void theVersionGateBelongsToTheResolvedBinding() throws Exception {
        try (Peer peer = Peer.start(server())) {
            // The secondary declares no version, so a call carrying none is served...
            byte[] ok = peer.raw("demo.Second.v1", "", "only_here");
            assertNotNull(ok);
            // ...and the primary refuses a mismatched client, naming itself in the detail.
            RpcError e = assertThrows(RpcError.class,
                    () -> peer.raw("demo.Primary.v1", "1.0.0", "echo_string"));
            assertEquals(ProtocolVersionError.ERROR_KIND, e.errorKind());
            assertEquals("FAILED_PRECONDITION", e.errorCode());
            assertNotNull(e.preconditionFailure());
            assertEquals("protocol_version", e.preconditionFailure().violations().get(0).type());
            assertEquals("demo.Primary.v1", e.preconditionFailure().violations().get(0).subject());
        }
    }

    @Test
    void tracebacksAreIncludedByDefaultAndOneSwitchOmitsThem() throws Exception {
        RpcServer on = server();
        assertTrue(on.includeTracebacks(), "included by default, on every transport");
        try (Peer peer = Peer.start(on)) {
            RpcError e = assertThrows(RpcError.class, () -> peer.raw("demo.Second.v1", "", "nope"));
            assertFalse(e.remoteTraceback().isEmpty(), "the default carries the traceback");
        }
        RpcServer off = server();
        off.setIncludeTracebacks(false);
        try (Peer peer = Peer.start(off)) {
            RpcError e = assertThrows(RpcError.class, () -> peer.raw("demo.Second.v1", "", "nope"));
            assertEquals("", e.remoteTraceback(), "the switch omits it");
            assertEquals("UNIMPLEMENTED", e.errorCode(), "everything else is still sent");
        }
    }

    // --- harness ----------------------------------------------------------------------

    private static final class Peer implements AutoCloseable {
        final RpcConnection connection;
        private final RpcTransport clientTransport;
        private final Thread serverThread;

        private Peer(RpcConnection connection, RpcTransport clientTransport, Thread serverThread) {
            this.connection = connection;
            this.clientTransport = clientTransport;
            this.serverThread = serverThread;
        }

        static Peer start(RpcServer server) throws Exception {
            PipedOutputStream clientOut = new PipedOutputStream();
            PipedInputStream serverIn = new PipedInputStream(clientOut, 1 << 16);
            PipedOutputStream serverOut = new PipedOutputStream();
            PipedInputStream clientIn = new PipedInputStream(serverOut, 1 << 16);
            RpcTransport serverTransport = new Pipes(serverIn, serverOut);
            RpcTransport clientTransport = new Pipes(clientIn, clientOut);
            Thread t = new Thread(() -> server.serve(serverTransport), "multi-protocol-peer");
            t.setDaemon(true);
            t.start();
            return new Peer(new RpcConnection(clientTransport), clientTransport, t);
        }

        byte[] call(String protocol, String method, AnnotatedBatch request) {
            return connection.callUnaryRaw(protocol, "", method, request);
        }

        /** A zero-parameter call; the server answers schema errors after routing and gating. */
        byte[] raw(String protocol, String version, String method) {
            try (VectorSchemaRoot params = VectorSchemaRoot.create(new Schema(List.of()), Allocators.root())) {
                params.setRowCount(0);
                return connection.callUnaryRaw(protocol, version, method, new AnnotatedBatch(params, Map.of()));
            }
        }

        @Override public void close() throws Exception {
            connection.close();
            clientTransport.close();
            serverThread.join(2000);
        }
    }

    private record Pipes(InputStream in, OutputStream out) implements RpcTransport {
        @Override public InputStream reader() { return in; }
        @Override public OutputStream writer() { return out; }
        @Override public void close() {
            try { in.close(); } catch (Exception ignore) { }
            try { out.close(); } catch (Exception ignore) { }
        }
    }
}
