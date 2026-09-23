package farm.query.vgirpc;

import farm.query.vgirpc.hash.ProtocolHash;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.SchemaDerivation;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.hash.ProtocolHash.HashMethod;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * {@code vgi_rpc.Reflection.v1} -- discovery as an ordinary co-hosted protocol.
 *
 * <p>Introspection used to be a hardcoded method name, {@code __describe__}, answered from a
 * pre-built batch before dispatch. That made it a thing every port had to hand-implement, in a
 * bespoke format, outside the machinery that serves every other method -- which is how the ports
 * drifted. Here it is a protocol like any other, addressed by the same routing key.
 *
 * <p>Following gRPC's reflection service and D-Bus's {@code org.freedesktop.DBus}, it is
 * co-hosted rather than special-cased. Its own major version sits in its name, so an
 * incompatible reflection is a routing failure a client can act on rather than a mis-parse.
 *
 * <p>Exempt from the {@code protocol_version} gate: this is the protocol a version-mismatched
 * client calls to learn <em>what</em> mismatched, and gating it would deny the client the
 * diagnosis it came for.
 */
public final class Reflection {

    private Reflection() {}

    /**
     * The wire name of the reflection protocol.
     *
     * <p>Fixed, and the one protocol name a client may know a priori: it is the bootstrap, so
     * there is nothing to discover it with.
     */
    public static final String PROTOCOL_NAME = "vgi_rpc.Reflection.v1";

    /**
     * The signatures of the two methods {@code vgi_rpc.Reflection.v1} offers.
     *
     * <p>Declared as an ordinary service interface, and introspected by the ordinary
     * {@link ServiceIntrospector}, so reflection's method table is derived by the same machinery
     * that derives every other protocol's. Hand-writing the table would put reflection's
     * parameter and result schemas on a second code path -- the arrangement that let this port
     * describe reflection as having no methods at all.
     *
     * <p>{@code byte[]} rather than a record type because that is what reflection actually puts
     * on the wire: both methods answer with their payload serialized into a single {@code result}
     * binary column, which is the framework's ordinary convention for a structured return, and
     * which is what {@link #protocolListSchema()} and {@link #serviceDescriptionSchema()} describe
     * the contents of. The interface is never implemented or invoked -- {@code RpcServer} answers
     * both methods directly -- so it exists to state the surface, not to serve it.
     */
    interface Service {
        /** Every protocol this server hosts, as a serialized {@code ProtocolList} batch. */
        byte[] list_protocols();

        /**
         * One protocol's description, as a serialized {@code ServiceDescription} batch.
         *
         * @param protocol the wire name to describe
         * @return the description
         */
        byte[] describe(String protocol);
    }

    /**
     * The method table {@code vgi_rpc.Reflection.v1} describes itself with, and hashes over.
     *
     * <p>Self-description is not special-cased: a client discovers reflection the documented way
     * -- {@code list_protocols}, then {@code describe} -- and must be able to learn from that how
     * to call the protocol it is already calling. This port used to hash and describe reflection
     * over an <em>empty</em> table, on the reasoning that its methods are framework-owned rather
     * than registered. That inverts the contract: the table is what {@code describe} reports and
     * what the hash is computed over, so an empty one is not honesty about an empty protocol, it
     * is a protocol lying about itself. Four ports shipped it, and no port's own suite could see
     * it, because each was internally consistent -- {@code list_protocols} advertised exactly the
     * digest its own {@code describe} returned.
     *
     * @return the two-method table, keyed by method name
     */
    public static Map<String, RpcMethodInfo> methodTable() {
        return ServiceIntrospector.describe(Service.class);
    }

    /**
     * The methods {@code vgi_rpc.Reflection.v1} offers.
     *
     * <p>Two, deliberately: {@code list_protocols} is the cheap question -- what is here -- and
     * {@code describe} is the expensive one. Named here rather than only inside the dispatch
     * switch because a transport that routes on the path ({@code {prefix}/{protocol}/{method}})
     * has to answer "no such method" for anything else <em>before</em> it reads a body.
     *
     * <p>Taken from {@link #methodTable()} rather than spelled a second time: a name routable but
     * undescribed, or described but unroutable, is the drift this protocol exists to eliminate.
     */
    public static final java.util.Set<String> METHOD_NAMES = methodTable().keySet();

    /** The default {@code idempotency}: a caller must assume the worst. */
    public static final String IDEMPOTENCY_UNKNOWN = "unknown";

    private static Field utf8(String name) {
        return new Field(name, FieldType.notNullable(new ArrowType.Utf8()), null);
    }

    private static Field bool(String name) {
        return new Field(name, FieldType.notNullable(new ArrowType.Bool()), null);
    }

    private static Field binary(String name) {
        return new Field(name, FieldType.notNullable(new ArrowType.Binary()), null);
    }

    private static Field listOf(String name, Field item) {
        return new Field(name, FieldType.notNullable(new ArrowType.List()), List.of(item));
    }

    private static Field structOf(String name, List<Field> children) {
        return new Field(name, FieldType.nullable(new ArrowType.Struct()), children);
    }

    /** The {@code ProtocolSummary} struct fields, mirroring the reference field for field. */
    private static List<Field> protocolSummaryFields() {
        return List.of(
                utf8("protocol"),
                utf8("protocol_version"),
                utf8("protocol_hash"),
                bool("deprecated"),
                utf8("deprecation_message"),
                listOf("features", new Field("item", FieldType.nullable(new ArrowType.Utf8()), null)));
    }

    /** The {@code MethodInfo} struct fields. */
    private static List<Field> methodInfoFields() {
        return List.of(
                utf8("name"),
                utf8("method_type"),
                bool("has_return"),
                bool("has_header"),
                utf8("stream_kind"),
                binary("params_schema_ipc"),
                binary("result_schema_ipc"),
                binary("header_schema_ipc"),
                utf8("idempotency"),
                bool("deprecated"),
                utf8("deprecation_message"));
    }

    /** The {@code ProtocolList} payload schema. */
    public static Schema protocolListSchema() {
        return new Schema(
                List.of(
                        utf8("server_id"),
                        utf8("server_version"),
                        utf8("request_version"),
                        listOf("protocols", structOf("item", protocolSummaryFields()))));
    }

    /**
     * The {@code ServiceDescription} payload schema.
     *
     * <p>Carries no server identity: two processes serving the same protocol must describe it
     * identically, or the description is not a property of the protocol. Server identity lives on
     * {@code ProtocolList}, which is a statement about a server.
     */
    public static Schema serviceDescriptionSchema() {
        List<Field> fields = new ArrayList<>(protocolSummaryFields());
        fields.add(listOf("methods", structOf("item", methodInfoFields())));
        return new Schema(fields);
    }

    /**
     * Whether a method returns a value to its caller.
     *
     * <p>A stream's result schema is the (empty) protocol-level return, not something the caller
     * receives, so the method type is part of the question being asked.
     */
    public static boolean unaryHasReturn(RpcMethodInfo info) {
        return info.methodType() == MethodType.UNARY
                && info.hasReturn()
                && info.resultSchema() != null
                && !info.resultSchema().getFields().isEmpty();
    }

    /**
     * The stream kind, or {@code ""} for a unary method.
     *
     * <p>A string rather than a nullable boolean because the state is genuinely three-valued:
     * whether a stream is an exchange is an implementation property, and "unknown" should be said
     * rather than encoded as absence.
     */
    public static String streamKindFor(RpcMethodInfo info) {
        if (info.methodType() == MethodType.UNARY) return "";
        return switch (info.streamKind()) {
            case PRODUCER -> "producer";
            case EXCHANGE -> "exchange";
            case UNKNOWN -> "unknown";
        };
    }

    /**
     * Serialise a Schema to the minimal Arrow IPC schema-message bytes.
     *
     * <p>Inherited from {@code Introspect}, which went out with {@code __describe__}. It is a
     * plain Arrow helper rather than anything describe owned, and reflection is now its only
     * caller -- so it moved here rather than leaving a retired class alive to hold it.
     */
    static byte[] serializeSchema(Schema schema) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            WriteChannel ch = new WriteChannel(Channels.newChannel(bos));
            MessageSerializer.serialize(ch, schema);
            return bos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("schema serialize failed", e);
        }
    }

    /** Serialize a schema, or return empty bytes when there is none. */
    private static byte[] schemaIpc(Schema schema) {
        return schema == null ? new byte[0] : serializeSchema(schema);
    }

    private static byte[] headerIpc(RpcMethodInfo info) {
        Class<?> headerType = info.headerType();
        if (headerType == null || !ArrowSerializableRecord.class.isAssignableFrom(headerType)) {
            return new byte[0];
        }
        return serializeSchema(
                SchemaDerivation.schemaForRecord(headerType.asSubclass(ArrowSerializableRecord.class)));
    }

    /** One protocol's canonical fingerprint. */
    public static String bindingHash(String name, Map<String, RpcMethodInfo> methods) {
        return ProtocolHash.computeProtocolHash(name, hashMethods(methods));
    }

    /**
     * The exact preimage {@link #bindingHash} digests.
     *
     * <p>A hash mismatch between two ports is otherwise one bit of information. With the preimage
     * in hand a failing port diffs two JSON documents and sees which method, field or type token
     * it spells differently -- so a test that asserts a digest should report this on failure.
     *
     * @param name the protocol's wire name
     * @param methods the protocol's method table
     * @return the canonical JSON description
     */
    public static String bindingPreimage(String name, Map<String, RpcMethodInfo> methods) {
        return ProtocolHash.canonicalDescription(name, hashMethods(methods));
    }

    private static List<HashMethod> hashMethods(Map<String, RpcMethodInfo> methods) {
        List<HashMethod> entries = new ArrayList<>(methods.size());
        for (RpcMethodInfo info : methods.values()) {
            boolean hasHeader = info.headerType() != null;
            entries.add(
                    new HashMethod(
                            info.name(),
                            info.methodType() == MethodType.UNARY ? "unary" : "stream",
                            unaryHasReturn(info),
                            hasHeader,
                            info.paramsSchema(),
                            info.resultSchema(),
                            hasHeader ? headerSchemaOf(info) : null));
        }
        return entries;
    }

    private static Schema headerSchemaOf(RpcMethodInfo info) {
        Class<?> headerType = info.headerType();
        if (headerType == null || !ArrowSerializableRecord.class.isAssignableFrom(headerType)) {
            return null;
        }
        return SchemaDerivation.schemaForRecord(
                headerType.asSubclass(ArrowSerializableRecord.class));
    }

    /** One hosted protocol, as it appears in {@code list_protocols}. */
    public record Summary(String protocol, String version, String hash) {}

    /** Build the single-row {@code ProtocolList} batch, serialized as an IPC stream. */
    public static byte[] buildProtocolList(
            String serverId, String serverVersion, String requestVersion, List<Summary> protocols)
            throws IOException {
        Schema schema = protocolListSchema();
        try (BufferAllocator allocator = Allocators.root().newChildAllocator("reflection", 0, Long.MAX_VALUE);
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            setUtf8(root, "server_id", serverId);
            setUtf8(root, "server_version", serverVersion);
            setUtf8(root, "request_version", requestVersion);

            ListVector list = (ListVector) root.getVector("protocols");
            UnionListWriter lw = list.getWriter();
            lw.startList();
            for (Summary s : protocols) {
                BaseWriter.StructWriter sw = lw.struct();
                sw.start();
                writeUtf8(sw, allocator, "protocol", s.protocol());
                writeUtf8(sw, allocator, "protocol_version", s.version());
                writeUtf8(sw, allocator, "protocol_hash", s.hash());
                sw.bit("deprecated").writeBit(0);
                writeUtf8(sw, allocator, "deprecation_message", "");
                // An empty but present feature list: additive capabilities
                // announce here rather than consuming version numbers.
                BaseWriter.ListWriter fw = sw.list("features");
                fw.startList();
                fw.endList();
                sw.end();
            }
            lw.endList();
            list.setValueCount(1);
            root.setRowCount(1);
            return serialize(root, allocator);
        }
    }

    /** Build the single-row {@code ServiceDescription} batch, serialized as an IPC stream. */
    public static byte[] buildServiceDescription(
            String protocol, String version, String hash, Map<String, RpcMethodInfo> methods)
            throws IOException {
        Schema schema = serviceDescriptionSchema();
        try (BufferAllocator allocator = Allocators.root().newChildAllocator("reflection", 0, Long.MAX_VALUE);
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            setUtf8(root, "protocol", protocol);
            setUtf8(root, "protocol_version", version);
            setUtf8(root, "protocol_hash", hash);
            ((BitVector) root.getVector("deprecated")).setSafe(0, 0);
            setUtf8(root, "deprecation_message", "");
            ListVector features = (ListVector) root.getVector("features");
            UnionListWriter fw = features.getWriter();
            fw.startList();
            fw.endList();
            features.setValueCount(1);

            ListVector list = (ListVector) root.getVector("methods");
            UnionListWriter lw = list.getWriter();
            lw.startList();
            // Sorted so two ports iterating differently-ordered maps still agree.
            for (RpcMethodInfo info : new TreeMap<>(methods).values()) {
                BaseWriter.StructWriter sw = lw.struct();
                sw.start();
                writeUtf8(sw, allocator, "name", info.name());
                writeUtf8(
                        sw, allocator, "method_type",
                        info.methodType() == MethodType.UNARY ? "unary" : "stream");
                sw.bit("has_return").writeBit(unaryHasReturn(info) ? 1 : 0);
                sw.bit("has_header").writeBit(info.headerType() != null ? 1 : 0);
                writeUtf8(sw, allocator, "stream_kind", streamKindFor(info));
                writeBinary(sw, allocator, "params_schema_ipc", schemaIpc(info.paramsSchema()));
                // Empty rather than null when absent: a nullable column costs
                // every port a null check on a value it will only ever treat as
                // absent.
                writeBinary(
                        sw, allocator, "result_schema_ipc",
                        unaryHasReturn(info) ? schemaIpc(info.resultSchema()) : new byte[0]);
                writeBinary(sw, allocator, "header_schema_ipc", headerIpc(info));
                writeUtf8(sw, allocator, "idempotency", IDEMPOTENCY_UNKNOWN);
                sw.bit("deprecated").writeBit(0);
                writeUtf8(sw, allocator, "deprecation_message", "");
                sw.end();
            }
            lw.endList();
            list.setValueCount(1);
            root.setRowCount(1);
            return serialize(root, allocator);
        }
    }

    private static void setUtf8(VectorSchemaRoot root, String name, String value) {
        ((VarCharVector) root.getVector(name)).setSafe(0, value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static void writeUtf8(
            BaseWriter.StructWriter sw, BufferAllocator allocator, String name, String value) {
        byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        try (org.apache.arrow.memory.ArrowBuf buf = allocator.buffer(Math.max(bytes.length, 1))) {
            buf.setBytes(0, bytes);
            sw.varChar(name).writeVarChar(0, bytes.length, buf);
        }
    }

    private static void writeBinary(
            BaseWriter.StructWriter sw, BufferAllocator allocator, String name, byte[] value) {
        try (org.apache.arrow.memory.ArrowBuf buf = allocator.buffer(Math.max(value.length, 1))) {
            buf.setBytes(0, value);
            sw.varBinary(name).writeVarBinary(0, value.length, buf);
        }
    }

    private static byte[] serialize(VectorSchemaRoot root, BufferAllocator allocator)
            throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ArrowStreamWriter w = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
            w.start();
            w.writeBatch();
            w.end();
        }
        return out.toByteArray();
    }
}
