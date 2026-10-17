// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.identity;

import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.AuthScope;
import farm.query.vgirpc.ProtocolNotSupportedError;
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
                            java.time.Instant.ofEpochSecond(1000 + ttl), "g1");

    private static RpcServer serverWith(IdentityImpl impl) {
        RpcServer server = new RpcServer(App.class, new App() { }, "srv123");
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
        RpcServer bare = new RpcServer(App.class, new App() { }, "srv123");
        RpcError err = assertThrows(RpcError.class, () ->
                call(bare, caller("proxy"), Identity.PROTOCOL_NAME, "introspect_token",
                        new LinkedHashMap<>(Map.of("token", "good"))));
        // The protocol is absent in every sense -- so the answer is "this server does not host
        // that protocol", not "that protocol has no such method". A proxy probing for a
        // credential oracle depends on the difference: the second reads as "the oracle is here,
        // configured differently", and would have it retry against a worker that has none.
        //
        // This used to fall through to the application method table and report an unknown
        // METHOD, because raw dispatch accepted any routing key it was given. That was the same
        // leniency that let a client address this worker by any name at all.
        assertEquals(ProtocolNotSupportedError.ERROR_KIND, err.errorKind(), err.getMessage());
        assertTrue(err.getMessage().contains(Identity.PROTOCOL_NAME), err.getMessage());
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
        assertEquals(java.time.Instant.ofEpochSecond(4600), grant.expires_at());
    }

    /**
     * {@code expires_at} is a non-nullable {@code timestamp[us, tz="UTC"]} on the wire
     * ({@code vgi_rpc.Identity.v2}); v1 carried float64 unix seconds.
     */
    @Test
    void issuedGrantExpiresAtIsAUtcMicrosecondTimestamp() throws Exception {
        org.apache.arrow.vector.types.pojo.Field f =
                farm.query.vgirpc.schema.SchemaDerivation.schemaForRecord(IssuedGrant.class)
                        .findField("expires_at");
        assertEquals(new org.apache.arrow.vector.types.pojo.ArrowType.Timestamp(
                org.apache.arrow.vector.types.TimeUnit.MICROSECOND, "UTC"), f.getType());
        assertEquals(false, f.isNullable());
        java.time.Instant at = java.time.Instant.parse("2030-01-01T00:00:00.123456789Z");
        IssuedGrant back = RecordCodec.deserializeFromBytes(
                RecordCodec.serializeToBytes(new IssuedGrant("t", at, "g")), IssuedGrant.class);
        // Sub-microsecond precision does not survive the wire; the microseconds do.
        assertEquals(java.time.Instant.parse("2030-01-01T00:00:00.123456Z"), back.expires_at());
        assertEquals(1893456000_123456L, farm.query.vgirpc.marshal.Numbers.instantToMicros(back.expires_at()));
        assertThrows(NullPointerException.class, () -> new IssuedGrant("t", null, "g"));
    }

    /**
     * An anonymous raw transport cannot mint.
     *
     * <p>The third case of the raw-transport trio {@code IDENTITY_CONFORMANCE_FIXTURE.md} §7
     * asks every port for. The shared conformance group is HTTP-only, because Identity's guards
     * all read an authenticated caller and HTTP is the transport that carries one -- so the
     * fail-closed behaviour of a transport carrying none has to be covered here.
     *
     * <p>{@code stale_auth} rather than a refusal of its own: a transport with no authenticated
     * principal carries no {@code auth_time} either, and the freshness guard is what makes stdio
     * and unix fail closed for free rather than by a special case somebody has to remember.
     */
    @Test
    void anAnonymousRawTransportCannotMint() {
        RpcServer server = serverWith(IdentityImpl.builder().mintGrant(MINTER).build());
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("purpose", "reports");
        args.put("scopes", List.of("read"));
        args.put("ttl_seconds", 60L);
        RpcError err = assertThrows(RpcError.class, () ->
                call(server, AuthContext.ANONYMOUS, Identity.PROTOCOL_NAME, "issue_grant", args));
        assertEquals(StaleAuthError.ERROR_KIND, err.errorKind());
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
        RpcServer bare = new RpcServer(App.class, new App() { }, "srv123");
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
        assertEquals("4039fbf6db3e64ae234e0be605b977477150ace892f10b6c691920eaf925e7ac",
                advertisedHash(serverWith(resolving())));
        assertEquals("4199df7403c9ca1a02c4c524b405a999eb1489614a1480873ffc23a0e5b0d312",
                advertisedHash(serverWith(IdentityImpl.builder().mintGrant(MINTER).build())));
        assertEquals("145267755ff90d5c3313d79c7ff771306f2530626380d01cb0e57a951d0f9ec3",
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
