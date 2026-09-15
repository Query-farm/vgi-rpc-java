package farm.query.vgirpc;

import farm.query.vgirpc.hash.ProtocolHash;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.SchemaDerivation;
import farm.query.vgirpc.hash.ProtocolHash.HashMethod;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
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

    /** Serialize a schema, or return empty bytes when there is none. */
    private static byte[] schemaIpc(Schema schema) {
        return schema == null ? new byte[0] : Introspect.serializeSchema(schema);
    }

    private static byte[] headerIpc(RpcMethodInfo info) {
        Class<?> headerType = info.headerType();
        if (headerType == null || !ArrowSerializableRecord.class.isAssignableFrom(headerType)) {
            return new byte[0];
        }
        return Introspect.serializeSchema(
                SchemaDerivation.schemaForRecord(headerType.asSubclass(ArrowSerializableRecord.class)));
    }

    /** One protocol's canonical fingerprint. */
    public static String bindingHash(String name, Map<String, RpcMethodInfo> methods) {
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
        return ProtocolHash.computeProtocolHash(name, entries);
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
        try (BufferAllocator allocator = new RootAllocator();
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
        try (BufferAllocator allocator = new RootAllocator();
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
