// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Client-side introspection: asking a peer what it hosts, and what one of its
 * protocols looks like.
 *
 * <p>The mirror of {@link Reflection}, which is the server half. A client that
 * can only call methods it was compiled against cannot answer "what is on the
 * other end of this connection" at all — which is what a CLI, a gateway, a
 * schema registry, and any cross-implementation conformance check need before
 * they can do anything else.
 *
 * <p>Two round trips on {@code vgi_rpc.Reflection.v1}, deliberately:
 * {@code list_protocols} says what the server hosts <em>and</em> who it is, and
 * {@code describe} says what one protocol is. Server identity is not on the
 * per-protocol description because two processes serving the same protocol must
 * describe it identically, or the description is not a property of the protocol.
 *
 * <p>{@code __describe__} is retired; a peer asked for it answers with a refusal
 * naming its replacement.
 */
public final class Introspect {

    /**
     * The introspection <em>format</em> version.
     *
     * <p>Vestigial, and pinned: introspection is a protocol whose major version
     * is part of its own name ({@code vgi_rpc.Reflection.v1}), so a breaking
     * change to the format renames the protocol rather than moving this. It is
     * reported because the cross-port description comparison asserts on it.
     */
    public static final String DESCRIBE_VERSION = "5";

    /** The routing key introspection itself is hosted under. */
    public static final String REFLECTION_PROTOCOL = Reflection.PROTOCOL_NAME;

    /**
     * The prefix marking a protocol as framework-owned rather than application
     * surface. Reflection and identity are co-hosted beside an application
     * protocol, so "describe this server" has to mean the one that is not.
     */
    public static final String FRAMEWORK_PREFIX = "vgi_rpc.";

    private Introspect() {}

    /**
     * How {@link Introspect} reaches the peer: one untyped unary call.
     *
     * <p>Deliberately not a connection type. Reflection is an ordinary protocol
     * reached by an ordinary call, so binding this to
     * {@code RpcConnection.callUnaryRaw} or to the HTTP client's equivalent is
     * the caller's one-line decision — and a caller that reaches its peer some
     * third way (a test double, a relay, a recorded transcript) can answer it
     * too.
     */
    @FunctionalInterface
    public interface RawUnaryCaller {
        /**
         * Issue one unary call and return the reply, framed as a one-batch IPC stream.
         *
         * @param protocol the routing key to address
         * @param method the method name
         * @param request the request batch with its Arrow custom metadata
         * @return the reply framed as a one-batch Arrow IPC stream
         */
        byte[] call(String protocol, String method, AnnotatedBatch request);
    }

    /**
     * One method of a described protocol.
     *
     * @param name the method name
     * @param methodType whether the method is unary or streaming
     * @param hasReturn whether a unary method returns a value to its caller
     * @param hasHeader whether a streaming method sends a header ahead of its body
     * @param isExchange {@code true} for an exchange stream, {@code false} for a
     *     producer, {@code null} for a unary method or when the peer cannot say
     * @param paramsSchemaIpc the params schema as the peer serialised it, or an empty array
     * @param resultSchemaIpc the result schema as the peer serialised it, or an empty array
     * @param headerSchemaIpc the header schema as the peer serialised it, or an empty array
     * @param idempotency the peer's idempotency claim, or {@code "unknown"}
     * @param deprecated whether the peer marks this method deprecated
     * @param deprecationMessage the deprecation note, or {@code ""}
     */
    public record MethodDescription(
            String name,
            MethodType methodType,
            boolean hasReturn,
            boolean hasHeader,
            Boolean isExchange,
            byte[] paramsSchemaIpc,
            byte[] resultSchemaIpc,
            byte[] headerSchemaIpc,
            String idempotency,
            boolean deprecated,
            String deprecationMessage) {}

    /**
     * A peer's description of one hosted protocol, plus the identity of the peer that answered.
     *
     * @param protocolName the routing key described
     * @param requestVersion the wire protocol version, from the listing hop
     * @param describeVersion the introspection format version
     * @param protocolHash the canonical fingerprint of the binding
     * @param serverId the answering server's instance id, from the listing hop
     * @param protocolVersion the application protocol surface version
     * @param methods the described methods, keyed by name
     */
    public record ServiceDescription(
            String protocolName,
            String requestVersion,
            String describeVersion,
            String protocolHash,
            String serverId,
            String protocolVersion,
            Map<String, MethodDescription> methods) {}

    /**
     * One entry of a peer's protocol listing.
     *
     * @param protocol the routing key
     * @param protocolVersion the application protocol surface version
     * @param protocolHash the canonical fingerprint of the binding
     * @param deprecated whether the peer marks this protocol deprecated
     * @param deprecationMessage the deprecation note, or {@code ""}
     * @param features the optional capabilities the peer claims for it
     */
    public record ProtocolSummary(
            String protocol,
            String protocolVersion,
            String protocolHash,
            boolean deprecated,
            String deprecationMessage,
            List<String> features) {}

    /**
     * What a peer hosts, and who the peer is.
     *
     * @param serverId the answering server's instance id
     * @param serverVersion the answering server's implementation version
     * @param requestVersion the wire protocol version
     * @param protocols the hosted protocols
     */
    public record ProtocolList(
            String serverId,
            String serverVersion,
            String requestVersion,
            List<ProtocolSummary> protocols) {}

    /**
     * Ask the peer what it hosts.
     *
     * @param caller how to reach the peer
     * @return the peer's listing
     * @throws RpcError if the peer refused or answered something undecodable
     */
    public static ProtocolList listProtocols(RawUnaryCaller caller) {
        byte[] payload = unwrapPayload(
                caller.call(REFLECTION_PROTOCOL, "list_protocols", emptyRequest()), "list_protocols");
        try (IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(payload), Allocators.root())) {
            if (r.readNextBatch() == null) {
                throw new RpcError("ProtocolError", "list_protocols carried no batch", "");
            }
            VectorSchemaRoot root = r.root();
            List<ProtocolSummary> protocols = new ArrayList<>();
            for (Object entry : (List<?>) ((ListVector) root.getVector("protocols")).getObject(0)) {
                Map<?, ?> p = (Map<?, ?>) entry;
                protocols.add(new ProtocolSummary(
                        text(p.get("protocol")),
                        text(p.get("protocol_version")),
                        text(p.get("protocol_hash")),
                        Boolean.TRUE.equals(p.get("deprecated")),
                        text(p.get("deprecation_message")),
                        strings(p.get("features"))));
            }
            return new ProtocolList(
                    text(root.getVector("server_id").getObject(0)),
                    text(root.getVector("server_version").getObject(0)),
                    text(root.getVector("request_version").getObject(0)),
                    List.copyOf(protocols));
        } catch (RpcError e) {
            throw e;
        } catch (Exception e) {
            throw new RpcError("ProtocolError", "could not decode list_protocols: " + e, "");
        }
    }

    /**
     * Describe one named protocol the peer hosts.
     *
     * @param caller how to reach the peer
     * @param protocol the routing key to describe
     * @param listing the peer's listing, whose identity fields the description
     *     itself does not carry
     * @return the description
     * @throws RpcError if the peer refused or answered something undecodable
     */
    public static ServiceDescription describe(RawUnaryCaller caller, String protocol, ProtocolList listing) {
        byte[] payload = unwrapPayload(
                caller.call(REFLECTION_PROTOCOL, "describe", utf8Request("protocol", protocol)), "describe");
        try (IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(payload), Allocators.root())) {
            if (r.readNextBatch() == null) {
                throw new RpcError("ProtocolError", "describe carried no batch", "");
            }
            VectorSchemaRoot root = r.root();
            Map<String, MethodDescription> methods = new LinkedHashMap<>();
            for (Object entry : (List<?>) ((ListVector) root.getVector("methods")).getObject(0)) {
                Map<?, ?> m = (Map<?, ?>) entry;
                String name = text(m.get("name"));
                methods.put(name, new MethodDescription(
                        name,
                        "stream".equals(text(m.get("method_type"))) ? MethodType.STREAM : MethodType.UNARY,
                        Boolean.TRUE.equals(m.get("has_return")),
                        Boolean.TRUE.equals(m.get("has_header")),
                        isExchange(text(m.get("method_type")), text(m.get("stream_kind"))),
                        bytes(m.get("params_schema_ipc")),
                        bytes(m.get("result_schema_ipc")),
                        bytes(m.get("header_schema_ipc")),
                        text(m.get("idempotency")),
                        Boolean.TRUE.equals(m.get("deprecated")),
                        text(m.get("deprecation_message"))));
            }
            return new ServiceDescription(
                    text(root.getVector("protocol").getObject(0)),
                    listing == null ? "" : listing.requestVersion(),
                    DESCRIBE_VERSION,
                    text(root.getVector("protocol_hash").getObject(0)),
                    listing == null ? "" : listing.serverId(),
                    text(root.getVector("protocol_version").getObject(0)),
                    methods);
        } catch (RpcError e) {
            throw e;
        } catch (Exception e) {
            throw new RpcError("ProtocolError", "could not decode describe: " + e, "");
        }
    }

    /**
     * List, then describe the peer's application protocol.
     *
     * <p>"The application protocol" is the first hosted protocol whose name does
     * not start with {@link #FRAMEWORK_PREFIX}. A peer hosting only
     * framework-owned protocols is an error rather than an empty description:
     * "this server exposes nothing of its own" is a real answer, and returning
     * a blank one would let it pass for a description.
     *
     * @param caller how to reach the peer
     * @return the description of the peer's application protocol
     * @throws RpcError if the peer hosts no application protocol
     */
    public static ServiceDescription describe(RawUnaryCaller caller) {
        ProtocolList listing = listProtocols(caller);
        for (ProtocolSummary summary : listing.protocols()) {
            if (!summary.protocol().startsWith(FRAMEWORK_PREFIX)) {
                return describe(caller, summary.protocol(), listing);
            }
        }
        throw new RpcError("ProtocolError",
                "the peer hosts no application protocol; every protocol it listed is framework-owned ("
                        + listing.protocols().stream().map(ProtocolSummary::protocol).toList() + ")", "");
    }

    // ------------------------------------------------------------------

    /**
     * Pull the nested payload out of a reflection reply.
     *
     * <p>Reflection answers with a {@code result} binary column whose contents
     * are themselves an IPC stream — a payload inside a payload, because the
     * description is a nested structure and the unary envelope is flat.
     */
    private static byte[] unwrapPayload(byte[] replyStream, String method) {
        if (replyStream == null) {
            throw new RpcError("ProtocolError", method + " returned no reply payload", "");
        }
        try (IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(replyStream), Allocators.root())) {
            if (r.readNextBatch() == null) {
                throw new RpcError("ProtocolError", method + " reply carried no batch", "");
            }
            Object value = r.root().getVector("result").getObject(0);
            byte[] payload = bytes(value);
            if (payload.length == 0) {
                throw new RpcError("ProtocolError", method + " reply carried an empty payload", "");
            }
            return payload;
        } catch (RpcError e) {
            throw e;
        } catch (Exception e) {
            throw new RpcError("ProtocolError", "could not read the " + method + " reply: " + e, "");
        }
    }

    /**
     * Whether a described method is an exchange stream.
     *
     * <p>Three-valued on purpose: a unary method is not a stream at all, and a
     * peer that reports {@code unknown} is saying it cannot tell — neither is
     * "producer", and collapsing either into {@code false} would let a caller
     * open the wrong stream shape and blame the peer.
     */
    private static Boolean isExchange(String methodType, String streamKind) {
        if (!"stream".equals(methodType)) return null;
        return switch (streamKind) {
            case "exchange" -> Boolean.TRUE;
            case "producer" -> Boolean.FALSE;
            default -> null;
        };
    }

    private static String text(Object value) {
        return value == null ? "" : value.toString();
    }

    private static byte[] bytes(Object value) {
        return value instanceof byte[] b ? b : new byte[0];
    }

    private static List<String> strings(Object value) {
        if (!(value instanceof List<?> items)) return List.of();
        List<String> out = new ArrayList<>(items.size());
        for (Object item : items) out.add(text(item));
        return List.copyOf(out);
    }

    /** A one-row batch of the empty schema — the request shape for a no-argument method. */
    private static AnnotatedBatch emptyRequest() {
        Schema schema = new Schema(List.of());
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root());
        root.allocateNew();
        root.setRowCount(1);
        return new AnnotatedBatch(root, Map.of());
    }

    /** A one-row batch carrying a single utf8 argument. */
    private static AnnotatedBatch utf8Request(String field, String value) {
        Schema schema = new Schema(List.of(
                new Field(field, FieldType.notNullable(new ArrowType.Utf8()), null)));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root());
        root.allocateNew();
        ((org.apache.arrow.vector.VarCharVector) root.getVector(field))
                .setSafe(0, value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        root.setRowCount(1);
        return new AnnotatedBatch(root, Map.of());
    }
}
