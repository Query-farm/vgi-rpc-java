// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.schema.ProtocolName;
import farm.query.vgirpc.transport.RpcTransport;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The wire name is declared, and a request that names something else is refused.
 *
 * <p>Two defects, one cause. The wire name was derived from the Java interface's simple name,
 * with no way to say otherwise — so six implementations of the VGI protocol derived four
 * different names, and {@code vgi.v2} was not expressible here at all, because no Java
 * identifier contains a dot. And raw dispatch accepted <em>any</em> non-empty routing key,
 * justified in a comment as "this port hosts exactly one application protocol, so there is
 * nothing to mis-route to".
 *
 * <p>The second defect hid the first. Raw lanes addressing a Java worker by any name at all
 * passed — not because the name matched, but because nothing compared it. {@code
 * WIRE_PROTOCOL.md} §3.1 gives three distinct answers, and a client probing for an optional
 * protocol depends on telling {@code protocol_not_supported} from {@code
 * method_not_implemented}. This port gave neither: it dispatched.
 */
@Timeout(30)
final class ProtocolNameRoutingTest {

    // --- the declaration -----------------------------------------------------

    /** Carries a dot-qualified major, which is the shape no Java simple name can express. */
    @ProtocolName("demo.v2")
    public interface Declared {
        String shout(String value);
    }

    /** No declaration: keeps the simple name, as every interface predating the annotation does. */
    public interface Derived {
        String shout(String value);
    }

    /**
     * Extends a declared protocol and stays silent.
     *
     * <p>The reference reads {@code vars(protocol)} rather than {@code getattr} for exactly this
     * case: a fixture that subclasses a protocol to vary its <em>version</em> silently inherited
     * its parent's routing key, so the caller was refused for the wrong reason before ever
     * reaching the version gate the fixture existed to test.
     */
    public interface Subinterface extends Declared {
    }

    public static final class ShoutImpl implements Declared, Derived, Subinterface {
        @Override public String shout(String value) {
            return value.toUpperCase(java.util.Locale.ROOT);
        }
    }

    @ProtocolName("not a name")
    interface Malformed {
        default void ping() {}
    }

    @ProtocolName("vgi_rpc.Reflection.v1")
    interface Reserved {
        default void ping() {}
    }

    @Test
    void aDeclaredNameIsTheWireName() {
        assertEquals("demo.v2", ServiceIntrospector.protocolName(Declared.class));
    }

    @Test
    void anUndeclaredInterfaceKeepsItsSimpleName() {
        // The fallback is what makes this annotation additive: every interface that predates it
        // keeps the name it already answered to, so adding the capability breaks no peer.
        assertEquals("Derived", ServiceIntrospector.protocolName(Derived.class));
    }

    @Test
    void aDeclarationIsNotInherited() {
        assertEquals("Subinterface", ServiceIntrospector.protocolName(Subinterface.class),
                "a subinterface that silently answered to its parent's routing key would "
                        + "impersonate it — and would be refused for the wrong reason before "
                        + "reaching whatever gate it was defined to vary");
        assertNotEquals(ServiceIntrospector.protocolName(Declared.class),
                ServiceIntrospector.protocolName(Subinterface.class));
    }

    @Test
    void aMalformedNameFailsAtIntrospectionNotOnTheFirstRequest() {
        // At worker construction: RpcServer introspects its interface in its constructor, so an
        // unroutable declaration refuses to start rather than failing every call at runtime.
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new RpcServer(Malformed.class, new Object()));
        assertTrue(e.getMessage().contains("@ProtocolName"), e.getMessage());
    }

    @Test
    void anApplicationMayNotClaimTheFrameworkPrefix() {
        // A protocol named vgi_rpc.Reflection.v1 would shadow reflection on the server that hosts
        // both — and reflection is the one endpoint a confused client reaches for to find out why.
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new RpcServer(Reserved.class, new Object()));
        assertTrue(e.getMessage().contains(ProtocolNames.RESERVED_PREFIX), e.getMessage());
    }

    @Test
    void theServerHostsTheDeclaredNameAndTheClientAddressesIt() {
        RpcServer srv = new RpcServer(Declared.class, new ShoutImpl(), "srv-declared");
        assertEquals("demo.v2", srv.protocolName());
        assertEquals("demo.v2", srv.protocolIdentityFor("demo.v2").name());
        // Server and client read the name from one place, so they cannot drift: a drifted
        // routing key fails as a 404, not as a type error.
        assertEquals(srv.protocolName(), ServiceIntrospector.protocolName(Declared.class));
    }

    // --- the routing gate ----------------------------------------------------

    @Test
    void theDeclaredNameRoutes() throws Exception {
        try (Peer peer = Peer.start()) {
            assertEquals("HELLO", peer.shout("demo.v2", "hello"));
        }
    }

    @Test
    void aNameThisServerDoesNotHostIsRefused() throws Exception {
        try (Peer peer = Peer.start()) {
            // The interface's simple name, which is exactly what this server answered to before
            // the name was declarable — and must not answer to now, or the declaration is a
            // suggestion rather than a contract.
            RpcError e = assertThrows(RpcError.class, () -> peer.shout("Declared", "hello"));
            assertEquals(ProtocolNotSupportedError.ERROR_KIND, e.errorKind(),
                    "a client probing for an optional protocol must be able to tell 'you do not "
                            + "speak this' from 'you speak it but lack this method'");
            assertTrue(e.getMessage().contains("does not host protocol 'Declared'"), e.getMessage());
            assertTrue(e.getMessage().contains("demo.v2"),
                    "the refusal must name what IS hosted: " + e.getMessage());
        }
    }

    @Test
    void aRoutingKeyThatCannotBeAProtocolNameIsNotEchoed() throws Exception {
        try (Peer peer = Peer.start()) {
            String hostile = "<script>alert(1)</script>";
            RpcError e = assertThrows(RpcError.class, () -> peer.shout(hostile, "hello"));
            assertEquals(ProtocolNotSupportedError.ERROR_KIND, e.errorKind());
            assertTrue(e.getMessage().contains("is not a protocol name"), e.getMessage());
            assertFalse(e.getMessage().contains(hostile),
                    "the grammar is checked before the lookup so a request-supplied string never "
                            + "reaches an error message, a log field or a metric label: "
                            + e.getMessage());
        }
    }

    @Test
    void aCoHostedFrameworkProtocolStillRoutes() throws Exception {
        // The justification for the old leniency was "this port hosts exactly one application
        // protocol". It never hosted one protocol: reflection is registered unconditionally, and
        // it has to keep routing under the tightened check.
        try (Peer peer = Peer.start()) {
            try (VectorSchemaRoot empty = VectorSchemaRoot.create(
                    new Schema(List.of()), Allocators.root())) {
                empty.setRowCount(1);
                byte[] reply = peer.connection.callUnaryRaw(
                        Reflection.PROTOCOL_NAME, "", "list_protocols",
                        new AnnotatedBatch(empty, Map.of()));
                assertTrue(reply.length > 0);
            }
        }
    }

    // --- harness -------------------------------------------------------------

    private static final class Peer implements AutoCloseable {
        private final RpcConnection connection;
        private final RpcTransport clientTransport;
        private final Thread serverThread;

        private Peer(RpcConnection connection, RpcTransport clientTransport, Thread serverThread) {
            this.connection = connection;
            this.clientTransport = clientTransport;
            this.serverThread = serverThread;
        }

        static Peer start() throws Exception {
            RpcServer server = new RpcServer(Declared.class, new ShoutImpl());
            PipedOutputStream clientOut = new PipedOutputStream();
            PipedInputStream serverIn = new PipedInputStream(clientOut, 1 << 16);
            PipedOutputStream serverOut = new PipedOutputStream();
            PipedInputStream clientIn = new PipedInputStream(serverOut, 1 << 16);

            RpcTransport serverTransport = new Pipes(serverIn, serverOut);
            RpcTransport clientTransport = new Pipes(clientIn, clientOut);

            Thread serverThread = new Thread(() -> server.serve(serverTransport), "name-peer");
            serverThread.setDaemon(true);
            serverThread.start();
            return new Peer(new RpcConnection(clientTransport), clientTransport, serverThread);
        }

        /** Untyped so the routing key is a test input rather than derived from a Java type. */
        String shout(String protocol, String value) throws Exception {
            Schema schema = new Schema(List.of(
                    new Field("value", FieldType.notNullable(new ArrowType.Utf8()), null)));
            try (VectorSchemaRoot params = VectorSchemaRoot.create(schema, Allocators.root())) {
                params.allocateNew();
                ((VarCharVector) params.getVector("value"))
                        .setSafe(0, value.getBytes(StandardCharsets.UTF_8));
                params.setRowCount(1);
                byte[] reply = connection.callUnaryRaw(
                        protocol, "", "shout", new AnnotatedBatch(params, Map.of()));
                return resultString(reply);
            }
        }

        @Override public void close() throws Exception {
            connection.close();
            clientTransport.close();
            serverThread.join(2000);
        }
    }

    private static String resultString(byte[] framed) throws Exception {
        try (IpcStreamReader r = new IpcStreamReader(
                new ByteArrayInputStream(framed), Allocators.root())) {
            assertNotNull(r.readNextBatch());
            return r.root().getVector("result").getObject(0).toString();
        }
    }

    private record Pipes(InputStream in, OutputStream out) implements RpcTransport {
        @Override public InputStream reader() { return in; }
        @Override public OutputStream writer() { return out; }
        @Override public void close() { }
    }
}
