// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.identity;

import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.AuthScope;
import farm.query.vgirpc.Reflection;
import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.RpcMethodInfo;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.ServiceIntrospector;
import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.transport.RpcTransport;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import farm.query.vgirpc.wire.IpcStreamWriter;
import farm.query.vgirpc.wire.Metadata;
import farm.query.vgirpc.wire.Wire;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Identity as the server actually routes it.
 *
 * <p>The guard tests call {@link IdentityImpl} directly; these drive a real request through
 * {@link RpcServer#serveOne}, because the wiring is where the port-specific mistakes live: the
 * routing key, the narrowed method table, and whether the protocol shows up in reflection's own
 * output rather than having to be known a priori.
 */
final class IdentityServerWiringTest {

    /** An application protocol with nothing to do with identity. */
    interface App {
        default String ping() { return "pong"; }
    }

    private static final GrantMintHook MINTER =
            (principal, purpose, scopes, ttl) ->
                    new IssuedGrant("grant-for-" + principal + "/" + purpose + String.join(",", scopes),
                            1000.0 + ttl, "g1");

    private static RpcServer serverWith(IdentityImpl impl) {
        RpcServer server = new RpcServer(App.class, new App() { }, "srv123", true);
        server.setIdentity(impl);
        return server;
    }

    private static IdentityImpl resolving() {
        return IdentityImpl.builder()
                .resolveToken(token -> "good".equals(token) ? new TokenIdentity("bob", "ci-key") : null)
                .introspectPrincipals("proxy")
                .build();
    }

    // --- a minimal client ---------------------------------------------------

    /** One request in, one response out; {@code serveOne} needs nothing more. */
    private record Exchange(Map<String, String> metadata, Map<String, Object> row) { }

    private static Exchange call(RpcServer server, AuthContext caller, String protocol,
                                 String method, Map<String, Object> args) throws Exception {
        RpcMethodInfo info = ServiceIntrospector.describe(Identity.class).get(method);
        ByteArrayOutputStream request = new ByteArrayOutputStream();
        Map<String, String> meta = Wire.requestMetadata(method);
        // The routing key. A Java client does not send one today, so the test
        // stamps it the way the reference client and the C++ extension do.
        meta.put(Metadata.PROTOCOL, protocol);
        // Identity's own methods have a declared parameter schema; reflection's
        // do not appear in any interface here, so their arguments are spelled as
        // the plain utf8 the reference client sends.
        Schema params = info != null ? info.paramsSchema() : utf8Schema(args.keySet());
        try (IpcStreamWriter w = new IpcStreamWriter(request)) {
            w.writeSchema(params);
            if (params.getFields().isEmpty()) {
                try (VectorSchemaRoot zero = VectorSchemaRoot.create(params, Allocators.root())) {
                    zero.setRowCount(1);
                    w.writeBatch(zero, meta);
                }
            } else {
                try (Marshalling.EncodedRow enc =
                             Marshalling.encodeRowForWire(params, args, Allocators.root())) {
                    w.writeBatch(enc.root(), meta, enc.provider());
                }
            }
        }

        ByteArrayOutputStream response = new ByteArrayOutputStream();
        RpcTransport transport = new RpcTransport() {
            private final InputStream in = new ByteArrayInputStream(request.toByteArray());
            @Override public InputStream reader() { return in; }
            @Override public OutputStream writer() { return response; }
            @Override public void close() { }
        };
        AutoCloseable scope = AuthScope.push(caller, Map.of());
        try {
            server.serveOne(transport);
        } finally {
            scope.close();
        }

        try (IpcStreamReader r = new IpcStreamReader(
                new ByteArrayInputStream(response.toByteArray()), Allocators.root())) {
            Map<String, String> md = r.readNextBatch();
            if (md == null) throw new IllegalStateException("server wrote no batch");
            if (Wire.classify(r.root().getRowCount(), md) == Wire.BatchKind.ERROR) {
                throw Wire.errorFromMetadata(md);
            }
            Map<String, Object> row = r.root().getRowCount() == 0
                    ? Map.of()
                    : Marshalling.decodeRow(r.root(), r.dictionaryProvider(), r.wireSchema());
            return new Exchange(md, row);
        }
    }

    private static Schema utf8Schema(java.util.Collection<String> names) {
        List<Field> fields = new ArrayList<>();
        for (String name : names) {
            fields.add(new Field(name, FieldType.notNullable(new ArrowType.Utf8()), null));
        }
        return new Schema(fields);
    }

    private static AuthContext caller(String principal) {
        return new AuthContext("test", true, principal, Map.of());
    }

    // --- routing ------------------------------------------------------------

    @Test
    void routesOnTheProtocolKey() throws Exception {
        Exchange got = call(serverWith(resolving()), caller("proxy"),
                Identity.PROTOCOL_NAME, "introspect_token",
                new LinkedHashMap<>(Map.of("token", "good")));
        TokenIdentity id = RecordCodec.deserializeFromBytes(
                (byte[]) got.row().get("result"), TokenIdentity.class);
        assertEquals("bob", id.principal());
        assertEquals("ci-key", id.token_name());
        assertEquals(TokenIdentity.DEFAULT_TTL_SECONDS, id.ttl_seconds());
    }

    /**
     * A guard's refusal reaches the caller with its {@code error_kind} intact.
     *
     * <p>That string is the whole definitive-versus-transient signal now: as protocol methods
     * every handler exception surfaces the same way, where the old HTTP route distinguished 403
     * from 503 by status code.
     */
    @Test
    void aRefusalCarriesItsErrorKind() throws Exception {
        RpcError err = assertThrows(RpcError.class, () ->
                call(serverWith(resolving()), caller("mallory"),
                        Identity.PROTOCOL_NAME, "introspect_token",
                        new LinkedHashMap<>(Map.of("token", "good"))));
        assertEquals(IntrospectionRefusedError.ERROR_KIND, err.errorKind());
        assertFalse(err.getMessage().contains("good"), err.getMessage());
    }

    /**
     * A method the deployment did not configure is <em>absent</em>, not refusing.
     *
     * <p>Absent beats routed-and-refusing: it is what keeps a dependency upgrade from growing a
     * credential-to-identity oracle on every existing worker, and it is what a client discovers
     * by reflecting rather than by calling.
     */
    @Test
    void anUnconfiguredMethodIsNotHosted() {
        RpcError err = assertThrows(RpcError.class, () ->
                call(serverWith(resolving()), caller("alice"),
                        Identity.PROTOCOL_NAME, "issue_grant",
                        new LinkedHashMap<>(Map.of(
                                "purpose", "p", "scopes", List.of(), "ttl_seconds", 60L))));
        assertEquals("method_not_implemented", err.errorKind());
    }

    /** With no identity configured at all the protocol is not routed. */
    @Test
    void anUnconfiguredProtocolIsNotRouted() {
        RpcServer bare = new RpcServer(App.class, new App() { }, "srv123", true);
        RpcError err = assertThrows(RpcError.class, () ->
                call(bare, caller("proxy"), Identity.PROTOCOL_NAME, "introspect_token",
                        new LinkedHashMap<>(Map.of("token", "good"))));
        // It falls through to the application method table, which has never
        // heard of it -- the protocol is absent in every sense.
        assertTrue(err.getMessage().contains("introspect_token"), err.getMessage());
    }

    @Test
    void mintsThroughTheServer() throws Exception {
        RpcServer server = serverWith(IdentityImpl.builder().mintGrant(MINTER).build());
        Map<String, Object> claims = Map.of("auth_time", System.currentTimeMillis() / 1000.0);
        AuthContext fresh = new AuthContext("test", true, "alice", claims);
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("purpose", "reports");
        args.put("scopes", List.of("read", "write"));
        args.put("ttl_seconds", 3600L);
        Exchange got = call(server, fresh, Identity.PROTOCOL_NAME, "issue_grant", args);
        IssuedGrant grant = RecordCodec.deserializeFromBytes(
                (byte[]) got.row().get("result"), IssuedGrant.class);
        assertEquals("grant-for-alice/reportsread,write", grant.token());
        assertEquals("g1", grant.grant_id());
    }

    // --- reflection ---------------------------------------------------------

    /** Registered after reflection, so it appears in reflection's own output. */
    @Test
    void appearsInListProtocols() throws Exception {
        RpcServer server = serverWith(resolving());
        Exchange got = call(server, caller("proxy"), Reflection.PROTOCOL_NAME, "list_protocols",
                new LinkedHashMap<>());
        List<String> names = protocolNames((byte[]) got.row().get("result"));
        assertEquals(List.of("App", Reflection.PROTOCOL_NAME, Identity.PROTOCOL_NAME), names);
    }

    /** And is absent from it when the deployment configured nothing. */
    @Test
    void isAbsentFromListProtocolsWhenUnconfigured() throws Exception {
        RpcServer bare = new RpcServer(App.class, new App() { }, "srv123", true);
        Exchange got = call(bare, caller("proxy"), Reflection.PROTOCOL_NAME, "list_protocols",
                new LinkedHashMap<>());
        assertEquals(List.of("App", Reflection.PROTOCOL_NAME),
                protocolNames((byte[]) got.row().get("result")));
    }

    /**
     * The advertised hash is the narrowed one.
     *
     * <p>A worker that resolves credentials but does not mint grants is not offering the same
     * surface as one that does, so it must not fingerprint the same. This is the property the
     * two single-method vectors in the spec exist to pin.
     */
    @Test
    void advertisesTheNarrowedHash() throws Exception {
        assertEquals("27b75bef22e4c70baab92a5188a473506b89055d2cb2b58cc187f6fe7a436385",
                advertisedHash(serverWith(resolving())));
        assertEquals("c71b12f453310139b6b6a445378064661c52711d03ae1e4fba29b8f7976ef4d8",
                advertisedHash(serverWith(IdentityImpl.builder().mintGrant(MINTER).build())));
        assertEquals("8317f2ad8e2476bb99e8b94800ab79b19a8cf0c6bdd6d66c2d82bd62ffbe69d5",
                advertisedHash(serverWith(IdentityImpl.builder()
                        .resolveToken(t -> null).mintGrant(MINTER)
                        .introspectPrincipals("proxy").build())));
    }

    /** Describing the protocol names only the methods it hosts. */
    @Test
    void describeListsOnlyTheHostedMethods() throws Exception {
        RpcServer server = serverWith(resolving());
        Exchange got = call(server, caller("proxy"), Reflection.PROTOCOL_NAME, "describe",
                new LinkedHashMap<>(Map.of("protocol", Identity.PROTOCOL_NAME)));
        Map<String, Object> description = decodeSingleRow((byte[]) got.row().get("result"));
        List<String> methodNames = new ArrayList<>();
        for (Object m : (List<?>) description.get("methods")) {
            methodNames.add((String) ((Map<?, ?>) m).get("name"));
        }
        assertEquals(List.of("introspect_token"), methodNames);
    }

    /**
     * The application protocol's version gate does not reach identity.
     *
     * <p>A proxy's ability to resolve a credential must not depend on an application-protocol
     * upgrade it has no part in -- identity declares no version of its own, and gating it on
     * somebody else's is gating it on an unrelated contract.
     */
    @Test
    void isNotGatedByTheApplicationProtocolVersion() throws Exception {
        RpcServer server = serverWith(resolving());
        server.setProtocolVersion("2.0.0");
        Exchange got = call(server, caller("proxy"), Identity.PROTOCOL_NAME, "introspect_token",
                new LinkedHashMap<>(Map.of("token", "good")));
        TokenIdentity id = RecordCodec.deserializeFromBytes(
                (byte[]) got.row().get("result"), TokenIdentity.class);
        assertEquals("bob", id.principal());
    }

    // --- helpers ------------------------------------------------------------

    private static String advertisedHash(RpcServer server) throws Exception {
        Exchange got = call(server, caller("proxy"), Reflection.PROTOCOL_NAME, "list_protocols",
                new LinkedHashMap<>());
        Map<String, Object> row = decodeSingleRow((byte[]) got.row().get("result"));
        for (Object p : (List<?>) row.get("protocols")) {
            Map<?, ?> summary = (Map<?, ?>) p;
            if (Identity.PROTOCOL_NAME.equals(summary.get("protocol"))) {
                return (String) summary.get("protocol_hash");
            }
        }
        throw new IllegalStateException("identity is not listed");
    }

    private static List<String> protocolNames(byte[] payload) throws Exception {
        List<String> names = new ArrayList<>();
        for (Object p : (List<?>) decodeSingleRow(payload).get("protocols")) {
            names.add((String) ((Map<?, ?>) p).get("protocol"));
        }
        return names;
    }

    /** The reflection payload rides as a nested IPC stream in the {@code result} column. */
    private static Map<String, Object> decodeSingleRow(byte[] payload) throws Exception {
        try (IpcStreamReader r = new IpcStreamReader(
                new ByteArrayInputStream(payload), Allocators.root())) {
            Map<String, String> md = r.readNextBatch();
            if (md == null) throw new IllegalStateException("empty payload stream");
            return Marshalling.decodeRow(r.root(), r.dictionaryProvider(), r.wireSchema());
        }
    }
}
