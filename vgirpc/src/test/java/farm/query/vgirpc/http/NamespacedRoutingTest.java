// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.ProtocolNotSpecifiedError;
import farm.query.vgirpc.Reflection;
import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.RpcMethodInfo;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.ServiceIntrospector;
import farm.query.vgirpc.identity.GrantMintHook;
import farm.query.vgirpc.identity.Identity;
import farm.query.vgirpc.identity.IdentityImpl;
import farm.query.vgirpc.identity.IssuedGrant;
import farm.query.vgirpc.identity.TokenIdentity;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * HTTP routes are namespaced by protocol: {@code {prefix}/{protocol}/{method}}.
 *
 * <p>Without the protocol segment only the primary application protocol is reachable over HTTP,
 * and the co-hosted framework protocols are raw-transport-only. That is worst for identity:
 * {@code issue_grant} requires an {@code auth_time} claim, which only arrives on an OIDC/JWT
 * credential -- i.e. over HTTP. Peer identity on TCP and unix authenticates a principal but
 * carries no {@code auth_time}, so the feature worked where it was not routed and was routed
 * where it had to refuse. The last test here is the one that closes that.
 */
@Timeout(30)
final class NamespacedRoutingTest {

    private static final String ARROW = "application/vnd.apache.arrow.stream";

    /** The application protocol. Its wire name is the interface's simple name. */
    public interface EchoService {
        String echo(String value);
    }

    static final class EchoImpl implements EchoService {
        @Override public String echo(String value) { return value; }
    }

    private static final GrantMintHook MINTER =
            (principal, purpose, scopes, ttl) ->
                    new IssuedGrant("grant-for-" + principal + "/" + purpose, 1000.0 + ttl, "g1");

    /**
     * A credential carrying {@code auth_time} -- the shape only an OIDC/JWT flow produces.
     *
     * <p>Presented on every request, because what is under test is routing, not authentication.
     */
    private static final Authenticator FRESHLY_AUTHENTICATED = request ->
            new AuthContext("test", true, "alice",
                    Map.of("auth_time", System.currentTimeMillis() / 1000.0));

    private HttpServer server;
    private String base;

    @BeforeEach
    void start() throws Exception {
        RpcServer rpc = new RpcServer(EchoService.class, new EchoImpl(), "srv-ns", true);
        rpc.setIdentity(IdentityImpl.builder()
                .resolveToken(token -> "good".equals(token) ? new TokenIdentity("bob", "ci-key") : null)
                .introspectPrincipals("alice")
                .mintGrant(MINTER)
                .build());
        server = new HttpServer(rpc, HttpServer.Config.builder()
                .prefix("/vgi")
                .authenticator(FRESHLY_AUTHENTICATED)
                .build());
        server.start();
        base = "http://127.0.0.1:" + server.port() + "/vgi";
    }

    @AfterEach
    void stop() throws Exception {
        if (server != null) server.stop();
    }

    // --- the three co-hosted protocols are all reachable ---------------------

    /** The application protocol, under its own name rather than at the bare prefix. */
    @Test
    void routesTheApplicationProtocol() throws Exception {
        HttpResponse<byte[]> resp = post("/EchoService/echo",
                request("EchoService", "echo", utf8Schema(List.of("value")),
                        new LinkedHashMap<>(Map.of("value", "hi"))));
        assertEquals(200, resp.statusCode());
        assertEquals("hi", resultOf(resp));
    }

    /**
     * Reflection, which before this was reachable only on the raw transports.
     *
     * <p>Introspection is not a special path: it is {@code {prefix}/vgi_rpc.Reflection.v1/...},
     * reached exactly the way anything else is.
     */
    @Test
    void routesReflection() throws Exception {
        HttpResponse<byte[]> resp = post("/" + Reflection.PROTOCOL_NAME + "/list_protocols",
                request(Reflection.PROTOCOL_NAME, "list_protocols", new Schema(List.of()),
                        new LinkedHashMap<>()));
        assertEquals(200, resp.statusCode());
        assertTrue(rowOf(resp).containsKey("result"), "list_protocols produced no payload");
    }

    /** Identity's read-only half. */
    @Test
    void routesIdentityIntrospection() throws Exception {
        HttpResponse<byte[]> resp = post("/" + Identity.PROTOCOL_NAME + "/introspect_token",
                identityRequest("introspect_token", new LinkedHashMap<>(Map.of("token", "good"))));
        assertEquals(200, resp.statusCode());
        TokenIdentity id = RecordCodec.deserializeFromBytes(
                (byte[]) rowOf(resp).get("result"), TokenIdentity.class);
        assertEquals("bob", id.principal());
    }

    /**
     * The whole point of the exercise: a grant minted over HTTP.
     *
     * <p>{@code issue_grant} demands a verifiable {@code auth_time}, which is what stops a grant
     * being used to mint another grant -- a grant is not an IdP-issued token, so it carries none,
     * and the lineage cannot escape the identity provider. Peer identity on TCP and unix
     * authenticates a principal but carries no {@code auth_time} either. So HTTP is the only
     * transport on which this call can succeed at all, and until the route existed it was the one
     * transport on which it could not be made.
     */
    @Test
    void mintsAGrantOverHttp() throws Exception {
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("purpose", "reports");
        args.put("scopes", List.of("read"));
        args.put("ttl_seconds", 3600L);
        HttpResponse<byte[]> resp =
                post("/" + Identity.PROTOCOL_NAME + "/issue_grant", identityRequest("issue_grant", args));
        assertEquals(200, resp.statusCode());
        IssuedGrant grant = RecordCodec.deserializeFromBytes(
                (byte[]) rowOf(resp).get("result"), IssuedGrant.class);
        assertEquals("grant-for-alice/reports", grant.token());
        assertEquals("g1", grant.grant_id());
    }

    // --- the carriers must agree --------------------------------------------

    /**
     * Path and {@code vgi_rpc.protocol} naming different protocols is a 400.
     *
     * <p>Unspecified, this is the Content-Length/Transfer-Encoding shape: the edge applies policy
     * to the protocol it can see in the path while the worker dispatches the one in the body.
     */
    @Test
    void refusesWhenThePathAndTheMetadataDisagree() throws Exception {
        HttpResponse<byte[]> resp = post("/EchoService/echo",
                request(Identity.PROTOCOL_NAME, "echo", utf8Schema(List.of("value")),
                        new LinkedHashMap<>(Map.of("value", "hi"))));
        assertEquals(400, resp.statusCode());
        RpcError err = errorOf(resp);
        assertEquals("protocol_not_supported", err.errorKind());
    }

    /**
     * Absent {@code vgi_rpc.protocol} is accepted over HTTP, and only over HTTP.
     *
     * <p>Not an exemption from the required-key rule but the single-carrier case seen from the
     * other side: the path segment already resolved the binding, so there is nothing for the
     * request to have failed to say. The rule bites where the metadata is the only carrier --
     * see {@link #refusesARawRequestThatNamesNoProtocol}.
     */
    @Test
    void acceptsAnAbsentRoutingKeyBecauseThePathCarriedIt() throws Exception {
        HttpResponse<byte[]> resp = post("/EchoService/echo",
                request(null, "echo", utf8Schema(List.of("value")),
                        new LinkedHashMap<>(Map.of("value", "hi"))));
        assertEquals(200, resp.statusCode());
        assertEquals("hi", resultOf(resp));
    }

    /**
     * On a transport where the metadata is the only carrier, absent is an error.
     *
     * <p>Required even against a server hosting exactly one protocol: an exemption would let an
     * intermediary that rebuilds a request and drops the field land silently on whichever
     * protocol happened to be first, and "silently landed on the wrong protocol" produces
     * plausible output rather than an error.
     */
    @Test
    void refusesARawRequestThatNamesNoProtocol() throws Exception {
        RpcServer rpc = new RpcServer(EchoService.class, new EchoImpl(), "srv-raw", true);
        RpcError err = assertThrows(RpcError.class, () -> serveRaw(rpc,
                request(null, "echo", utf8Schema(List.of("value")),
                        new LinkedHashMap<>(Map.of("value", "hi")))));
        assertEquals(ProtocolNotSpecifiedError.ERROR_KIND, err.errorKind());
    }

    /** The same request with the key present dispatches, so the refusal above is about the key. */
    @Test
    void servesARawRequestThatNamesTheProtocol() throws Exception {
        RpcServer rpc = new RpcServer(EchoService.class, new EchoImpl(), "srv-raw", true);
        Map<String, Object> row = serveRaw(rpc,
                request("EchoService", "echo", utf8Schema(List.of("value")),
                        new LinkedHashMap<>(Map.of("value", "hi"))));
        assertEquals("hi", row.get("result").toString());
    }

    // --- unroutable segments -------------------------------------------------

    /**
     * A percent sign in the protocol segment is refused <em>without decoding</em>.
     *
     * <p>{@code vgi%5frpc.Reflection.v1} decodes to a protocol this server really does host, so a
     * worker routing on the container's already-decoded path would serve it while an edge device
     * reading the raw request line saw something else entirely. The name charset never requires
     * percent-encoding, so a percent sign is a bug or an attack, and the answer is 404 rather
     * than a decode.
     */
    @Test
    void refusesAPercentSignWithoutDecodingIt() throws Exception {
        // vgi%5frpc.Reflection.v1 decodes to a protocol this server really does host.
        assertEquals(404, post("/vgi%5frpc.Reflection.v1/list_protocols",
                request(Reflection.PROTOCOL_NAME, "list_protocols", new Schema(List.of()),
                        new LinkedHashMap<>())).statusCode());
        // The control: the same name, unencoded, routes and answers.
        assertEquals(200, post("/" + Reflection.PROTOCOL_NAME + "/list_protocols",
                request(Reflection.PROTOCOL_NAME, "list_protocols", new Schema(List.of()),
                        new LinkedHashMap<>())).statusCode());
    }

    /**
     * And nothing else decodes the segment behind the check's back.
     *
     * <p>{@code EchoServic%65} is a percent-escape the servlet container decodes for
     * {@code getPathInfo()} without complaint -- a worker routing on that already-decoded path
     * would serve this request while the check above, reading the raw one, believed it had
     * refused it. Routing raw is what makes the {@code %} ban mean anything: comparing
     * decoded-against-raw is the bug, not the fix.
     */
    @Test
    void routesOnTheRawSegmentRatherThanTheDecodedOne() throws Exception {
        HttpResponse<byte[]> resp = post("/EchoServic%65/echo",
                request("EchoService", "echo", utf8Schema(List.of("value")),
                        new LinkedHashMap<>(Map.of("value", "hi"))));
        assertEquals(404, resp.statusCode());
        assertNotEquals(200, resp.statusCode());
    }

    /** A protocol this server does not host is a routing failure, which is a 404. */
    @Test
    void unknownProtocolIs404() throws Exception {
        assertEquals(404, post("/NoSuchService/echo",
                request("NoSuchService", "echo", utf8Schema(List.of("value")),
                        new LinkedHashMap<>(Map.of("value", "hi")))).statusCode());
    }

    /** A segment that could not be a protocol name at all is the same answer. */
    @Test
    void ungrammaticalProtocolSegmentIs404() throws Exception {
        assertEquals(404, post("/9nope/echo", new byte[0]).statusCode());
    }

    /**
     * Hosted protocol, absent method: 404, and deliberately not the same error as the two above.
     *
     * <p>A client probing for an optional method has to be able to tell "you do not speak this
     * protocol" from "you speak it but lack this method".
     */
    @Test
    void unknownMethodOnAHostedProtocolIs404() throws Exception {
        HttpResponse<byte[]> resp = post("/EchoService/nope",
                request("EchoService", "nope", new Schema(List.of()), new LinkedHashMap<>()));
        assertEquals(404, resp.statusCode());
        assertEquals("method_not_implemented", errorOf(resp).errorKind());
    }

    /**
     * A GET to a two-segment path that is not an RPC route is 404, not 405.
     *
     * <p>Otherwise every unrelated two-segment path -- a {@code /.well-known/...} document among
     * them -- matches the POST-only RPC route and is answered "method not allowed", which reads
     * as "this exists, use another verb".
     */
    @Test
    void getOnANonRouteIs404WhileGetOnARouteIs405() throws Exception {
        assertEquals(404, get("/NoSuchService/echo").statusCode());
        assertEquals(405, get("/EchoService/echo").statusCode());
    }

    // --- helpers -------------------------------------------------------------

    private HttpResponse<byte[]> post(String path, byte[] body) throws Exception {
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()) {
            return client.send(HttpRequest.newBuilder(URI.create(base + path))
                    .timeout(Duration.ofSeconds(20))
                    .header("Content-Type", ARROW)
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                    .build(), HttpResponse.BodyHandlers.ofByteArray());
        }
    }

    private HttpResponse<byte[]> get(String path) throws Exception {
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()) {
            return client.send(HttpRequest.newBuilder(URI.create(base + path))
                    .timeout(Duration.ofSeconds(20)).GET().build(),
                    HttpResponse.BodyHandlers.ofByteArray());
        }
    }

    /** Build a request body, optionally stamping the routing key ({@code null} omits it). */
    private static byte[] request(String protocol, String method, Schema params,
                                  Map<String, Object> args) throws Exception {
        Map<String, String> meta = Wire.requestMetadata(method);
        if (protocol != null) meta.put(Metadata.PROTOCOL, protocol);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (IpcStreamWriter w = new IpcStreamWriter(out)) {
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
        return out.toByteArray();
    }

    private static byte[] identityRequest(String method, Map<String, Object> args) throws Exception {
        RpcMethodInfo info = ServiceIntrospector.describe(Identity.class).get(method);
        return request(Identity.PROTOCOL_NAME, method, info.paramsSchema(), args);
    }

    /** Drive one request through the raw dispatch path, where metadata is the only carrier. */
    private static Map<String, Object> serveRaw(RpcServer rpc, byte[] body) throws Exception {
        ByteArrayOutputStream response = new ByteArrayOutputStream();
        RpcTransport transport = new RpcTransport() {
            private final InputStream in = new ByteArrayInputStream(body);
            @Override public InputStream reader() { return in; }
            @Override public OutputStream writer() { return response; }
            @Override public void close() { }
        };
        rpc.serveOne(transport);
        return decode(response.toByteArray());
    }

    private static Map<String, Object> rowOf(HttpResponse<byte[]> resp) throws Exception {
        return decode(resp.body());
    }

    private static String resultOf(HttpResponse<byte[]> resp) throws Exception {
        return rowOf(resp).get("result").toString();
    }

    private static RpcError errorOf(HttpResponse<byte[]> resp) throws Exception {
        return assertThrows(RpcError.class, () -> decode(resp.body()));
    }

    private static Map<String, Object> decode(byte[] body) throws Exception {
        try (IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(body), Allocators.root())) {
            Map<String, String> md = r.readNextBatch();
            if (md == null) throw new IllegalStateException("server wrote no batch");
            if (Wire.classify(r.root().getRowCount(), md) == Wire.BatchKind.ERROR) {
                throw Wire.errorFromMetadata(md);
            }
            return r.root().getRowCount() == 0
                    ? Map.of()
                    : Marshalling.decodeRow(r.root(), r.dictionaryProvider(), r.wireSchema());
        }
    }

    private static Schema utf8Schema(List<String> names) {
        List<Field> fields = new ArrayList<>();
        for (String name : names) {
            fields.add(new Field(name, FieldType.notNullable(new ArrowType.Utf8()), null));
        }
        return new Schema(fields);
    }
}
