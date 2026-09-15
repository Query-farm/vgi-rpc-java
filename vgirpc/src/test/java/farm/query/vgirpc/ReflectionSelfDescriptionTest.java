// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.transport.RpcTransport;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import farm.query.vgirpc.wire.IpcStreamWriter;
import farm.query.vgirpc.wire.Metadata;
import farm.query.vgirpc.wire.Wire;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code vgi_rpc.Reflection.v1} describes its own two methods.
 *
 * <p>A client discovers a server the documented way: {@code list_protocols}, then {@code describe}
 * for each protocol it cares about. This port used to advertise reflection in the first answer and
 * then report it as having <em>no methods</em> in the second, so a client could not learn from
 * reflection how to call the protocol it was already calling. The reasoning was that reflection's
 * methods are framework-owned rather than registered, so the "honest" table is empty. That inverts
 * the contract: the table is what {@code describe} reports and what the hash is computed over, so
 * an empty one is not honesty about an empty protocol -- it is a protocol lying about itself.
 *
 * <p>Nothing local could see it. Four ports shipped the empty table and every one of their suites
 * stayed green, because each port was internally <em>self-consistent</em>: {@code list_protocols}
 * advertised exactly the digest that port's own {@code describe} returned, and a test comparing
 * the two agrees with itself whatever it contains. Only a port-to-port comparison could catch it.
 * That is why the assertions below pin a <strong>literal</strong> cross-port digest and a literal
 * canonical preimage rather than recomputing either: a recomputed expectation would have passed
 * on the broken code, and will pass again the next time somebody empties the table.
 */
@Timeout(30)
final class ReflectionSelfDescriptionTest {

    /** The application protocol under test; reflection is co-hosted beside it. */
    public interface Ledger {
        String echo(String value);
    }

    static final class LedgerImpl implements Ledger {
        @Override public String echo(String value) { return value; }
    }

    /**
     * Reflection's canonical digest, shared by every port.
     *
     * <p>A literal, taken from the reference (python/go/typescript). See
     * {@code REFLECTION_SELF_DESCRIPTION.md} in {@code vgi-rpc-sync}. The empty-table digest this
     * port used to produce was {@code fafffd66fd1b98ee355cbcbd6a0fbe6f32b09088512bd1f04268a0a9427d79e9}.
     */
    private static final String REFLECTION_CANONICAL_HASH =
            "3c7db4cae8cdfc93dc4a76e73b8b759e18e45e6a5811adba4e520366344b919a";

    /**
     * The exact JSON the digest is taken over.
     *
     * <p>Pinned beside the digest because a digest mismatch is one bit of information: with the
     * preimage in hand, a failing port diffs two JSON documents and sees which method, field or
     * type token it spells differently.
     */
    private static final String REFLECTION_CANONICAL_PREIMAGE =
            "{\"methods\":["
                    + "{\"has_header\":false,\"has_return\":true,\"name\":\"describe\","
                    + "\"params\":[{\"name\":\"protocol\",\"nullable\":false,\"type\":\"utf8\"}],"
                    + "\"result\":[{\"name\":\"result\",\"nullable\":false,\"type\":\"binary\"}],"
                    + "\"type\":\"unary\"},"
                    + "{\"has_header\":false,\"has_return\":true,\"name\":\"list_protocols\","
                    + "\"params\":[],"
                    + "\"result\":[{\"name\":\"result\",\"nullable\":false,\"type\":\"binary\"}],"
                    + "\"type\":\"unary\"}"
                    + "],\"protocol\":\"vgi_rpc.Reflection.v1\"}";

    // --- the binding ---------------------------------------------------------

    @Test
    void reflectionsBindingCarriesItsTwoMethods() {
        assertEquals(List.of("describe", "list_protocols"),
                Reflection.methodTable().keySet().stream().sorted().toList(),
                "reflection answers describe and list_protocols, so its binding must contain them");
    }

    @Test
    void everyRoutableNameIsADescribedName() {
        // The two used to be independent spellings: METHOD_NAMES gated path routing while the
        // table gated description. A name routable but undescribed is exactly the drift this
        // protocol exists to eliminate.
        assertEquals(Reflection.methodTable().keySet(), Reflection.METHOD_NAMES);
    }

    @Test
    void theBindingHashesToTheCrossPortDigest() {
        assertEquals(REFLECTION_CANONICAL_HASH,
                Reflection.bindingHash(Reflection.PROTOCOL_NAME, Reflection.methodTable()),
                () -> "preimage: "
                        + Reflection.bindingPreimage(
                                Reflection.PROTOCOL_NAME, Reflection.methodTable()));
    }

    @Test
    void thePreimageIsTheAgreedOne() {
        assertEquals(REFLECTION_CANONICAL_PREIMAGE,
                Reflection.bindingPreimage(Reflection.PROTOCOL_NAME, Reflection.methodTable()));
    }

    @Test
    void theShapeOfEachMethodIsTheAgreedShape() {
        // Spelled out rather than left implicit in the digest, so a failure says which of the
        // six facts moved instead of only that something did.
        Map<String, RpcMethodInfo> table = Reflection.methodTable();

        RpcMethodInfo describe = table.get("describe");
        assertNotNull(describe);
        assertEquals(MethodType.UNARY, describe.methodType());
        assertTrue(Reflection.unaryHasReturn(describe));
        assertNull(describe.headerType());
        assertEquals(utf8("protocol"), describe.paramsSchema());
        assertEquals(binaryResult(), describe.resultSchema());

        RpcMethodInfo list = table.get("list_protocols");
        assertNotNull(list);
        assertEquals(MethodType.UNARY, list.methodType());
        assertTrue(Reflection.unaryHasReturn(list));
        assertNull(list.headerType());
        assertEquals(List.of(), list.paramsSchema().getFields());
        assertEquals(binaryResult(), list.resultSchema());
    }

    @Test
    void theAccessLogIdentityCarriesTheSameDigest() {
        // The accessor is what stamps protocol_hash onto every access record, and protocol_hash
        // is the registry key a consumer decodes archived records against. A record naming
        // reflection while carrying the digest of a method-less protocol is decoded against a
        // description that has nothing in it.
        RpcServer srv = new RpcServer(Ledger.class, new LedgerImpl(), "srv-refl");
        assertEquals(REFLECTION_CANONICAL_HASH,
                srv.protocolIdentityFor(Reflection.PROTOCOL_NAME).protocolHash());
    }

    // --- end to end, through real dispatch -----------------------------------

    @Test
    void describeReturnsBothMethodsOverTheWire() throws Exception {
        RpcServer srv = new RpcServer(Ledger.class, new LedgerImpl(), "srv-refl");
        Map<String, Object> row = call(srv, "describe", utf8("protocol"),
                Map.of("protocol", Reflection.PROTOCOL_NAME));

        Description desc = readDescription((byte[]) row.get("result"));
        assertEquals(Reflection.PROTOCOL_NAME, desc.protocol);
        assertEquals(REFLECTION_CANONICAL_HASH, desc.protocolHash);
        assertEquals(List.of("describe", "list_protocols"),
                desc.methods.stream().map(m -> m.name).toList(),
                "an empty method list tells a client the protocol it is calling has no methods");

        MethodRow describe = desc.method("describe");
        assertEquals("unary", describe.methodType);
        assertTrue(describe.hasReturn);
        assertFalse(describe.hasHeader);
        assertEquals("", describe.streamKind);
        assertEquals(utf8("protocol"), describe.paramsSchema());
        assertEquals(binaryResult(), describe.resultSchema());

        MethodRow list = desc.method("list_protocols");
        assertEquals("unary", list.methodType);
        assertTrue(list.hasReturn);
        assertFalse(list.hasHeader);
        assertEquals("", list.streamKind);
        assertEquals(List.of(), list.paramsSchema().getFields());
        assertEquals(binaryResult(), list.resultSchema());
    }

    @Test
    void listProtocolsAdvertisesTheDigestDescribeReturns() throws Exception {
        // Self-consistency alone is what let four ports be wrong with green suites, so this is
        // only worth asserting alongside the literal pin above -- together they say the two
        // answers agree *and* that they agree on the value every other port produces.
        RpcServer srv = new RpcServer(Ledger.class, new LedgerImpl(), "srv-refl");
        Map<String, Object> row = call(srv, "list_protocols", new Schema(List.of()), Map.of());

        Map<String, String> advertised = readProtocolList((byte[]) row.get("result"));
        assertTrue(advertised.containsKey(Reflection.PROTOCOL_NAME),
                "reflection appears in its own output: self-description is not special-cased");
        assertEquals(REFLECTION_CANONICAL_HASH, advertised.get(Reflection.PROTOCOL_NAME));
    }

    @Test
    void describingReflectionDoesNotDisturbTheApplicationProtocol() {
        // Only reflection's digest moves. The application binding is hashed over its own methods
        // and has nothing to do with how the framework protocols describe themselves.
        RpcServer srv = new RpcServer(Ledger.class, new LedgerImpl(), "srv-refl");
        assertEquals("40afc20052d9c1f5b1e6966f81b8a9125773e628c0a5aeeef154b7d6346ecda1",
                srv.protocolHash(),
                () -> "preimage: " + Reflection.bindingPreimage("Ledger", srv.methods()));
    }

    // --- decoding ------------------------------------------------------------

    /** One row of the {@code methods} list on a {@code ServiceDescription}. */
    private record MethodRow(String name, String methodType, boolean hasReturn, boolean hasHeader,
                             String streamKind, byte[] paramsIpc, byte[] resultIpc) {
        Schema paramsSchema() { return deserialize(paramsIpc); }

        Schema resultSchema() { return deserialize(resultIpc); }
    }

    /** A decoded {@code ServiceDescription} batch. */
    private record Description(String protocol, String protocolHash, List<MethodRow> methods) {
        MethodRow method(String name) {
            return methods.stream().filter(m -> m.name.equals(name)).findFirst()
                    .orElseThrow(() -> new AssertionError("no method '" + name + "' in " + methods));
        }
    }

    private static Description readDescription(byte[] ipc) throws Exception {
        try (BufferAllocator alloc = new RootAllocator();
             ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), alloc)) {
            assertTrue(reader.loadNextBatch(), "description carried no batch");
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            String protocol = root.getVector("protocol").getObject(0).toString();
            String hash = root.getVector("protocol_hash").getObject(0).toString();
            List<MethodRow> methods = new ArrayList<>();
            for (Object entry : (List<?>) ((ListVector) root.getVector("methods")).getObject(0)) {
                Map<?, ?> m = (Map<?, ?>) entry;
                methods.add(new MethodRow(
                        m.get("name").toString(),
                        m.get("method_type").toString(),
                        (Boolean) m.get("has_return"),
                        (Boolean) m.get("has_header"),
                        m.get("stream_kind").toString(),
                        (byte[]) m.get("params_schema_ipc"),
                        (byte[]) m.get("result_schema_ipc")));
            }
            return new Description(protocol, hash, methods);
        }
    }

    /** Protocol name to advertised hash, from a {@code ProtocolList} batch. */
    private static Map<String, String> readProtocolList(byte[] ipc) throws Exception {
        try (BufferAllocator alloc = new RootAllocator();
             ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), alloc)) {
            assertTrue(reader.loadNextBatch(), "protocol list carried no batch");
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            Map<String, String> out = new LinkedHashMap<>();
            for (Object entry : (List<?>) ((ListVector) root.getVector("protocols")).getObject(0)) {
                Map<?, ?> p = (Map<?, ?>) entry;
                out.put(p.get("protocol").toString(), p.get("protocol_hash").toString());
            }
            return out;
        }
    }

    private static Schema deserialize(byte[] schemaIpc) {
        try {
            return MessageSerializer.deserializeSchema(
                    new ReadChannel(Channels.newChannel(new ByteArrayInputStream(schemaIpc))));
        } catch (Exception e) {
            throw new AssertionError("could not decode a schema the description carried", e);
        }
    }

    // --- harness -------------------------------------------------------------

    private static Schema utf8(String name) {
        return new Schema(List.of(
                new Field(name, FieldType.notNullable(new ArrowType.Utf8()), null)));
    }

    private static Schema binaryResult() {
        return new Schema(List.of(
                new Field("result", FieldType.notNullable(new ArrowType.Binary()), null)));
    }

    /** Dispatch one reflection call through the raw transport and decode the response row. */
    private static Map<String, Object> call(RpcServer srv, String method, Schema params,
                                            Map<String, Object> args) throws Exception {
        Map<String, String> meta = Wire.requestMetadata(method);
        meta.put(Metadata.PROTOCOL, Reflection.PROTOCOL_NAME);
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        try (IpcStreamWriter w = new IpcStreamWriter(body)) {
            w.writeSchema(params);
            if (params.getFields().isEmpty()) {
                try (VectorSchemaRoot zero = VectorSchemaRoot.create(params, Allocators.root())) {
                    zero.setRowCount(1);
                    w.writeBatch(zero, meta);
                }
            } else {
                try (Marshalling.EncodedRow enc = Marshalling.encodeRowForWire(
                        params, new LinkedHashMap<>(args), Allocators.root())) {
                    w.writeBatch(enc.root(), meta, enc.provider());
                }
            }
        }

        ByteArrayOutputStream response = new ByteArrayOutputStream();
        RpcTransport transport = new RpcTransport() {
            private final InputStream in = new ByteArrayInputStream(body.toByteArray());
            @Override public InputStream reader() { return in; }
            @Override public OutputStream writer() { return response; }
            @Override public void close() { }
        };
        srv.serveOne(transport);

        try (IpcStreamReader r = new IpcStreamReader(
                new ByteArrayInputStream(response.toByteArray()), Allocators.root())) {
            Map<String, String> md = r.readNextBatch();
            assertNotNull(md, "server wrote no batch");
            if (Wire.classify(r.root().getRowCount(), md) == Wire.BatchKind.ERROR) {
                throw Wire.errorFromMetadata(md);
            }
            return Marshalling.decodeRow(r.root(), r.dictionaryProvider(), r.wireSchema());
        }
    }
}
