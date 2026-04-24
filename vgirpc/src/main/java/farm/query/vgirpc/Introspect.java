// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.SchemaDerivation;
import farm.query.vgirpc.transport.RpcTransport;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import farm.query.vgirpc.wire.IpcStreamWriter;
import farm.query.vgirpc.wire.Metadata;
import farm.query.vgirpc.wire.Wire;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Implementation of the {@code __describe__} synthetic RPC method used for
 * runtime service introspection. Matches Python {@code vgi_rpc.introspect}
 * on the wire (describe-version 3, 12-field schema).
 */
public final class Introspect {

    public static final String METHOD_NAME = "__describe__";
    public static final String DESCRIBE_VERSION = "3";

    private static final ObjectMapper JSON = new ObjectMapper();

    public static final Schema DESCRIBE_SCHEMA = new Schema(List.of(
            field("name", new ArrowType.Utf8(), false),
            field("method_type", new ArrowType.Utf8(), false),
            field("doc", new ArrowType.Utf8(), true),
            field("has_return", new ArrowType.Bool(), false),
            field("params_schema_ipc", new ArrowType.Binary(), false),
            field("result_schema_ipc", new ArrowType.Binary(), false),
            field("param_types_json", new ArrowType.Utf8(), true),
            field("param_defaults_json", new ArrowType.Utf8(), true),
            field("has_header", new ArrowType.Bool(), false),
            field("header_schema_ipc", new ArrowType.Binary(), true),
            field("is_exchange", new ArrowType.Bool(), true),
            field("param_docs_json", new ArrowType.Utf8(), true)));

    private static Field field(String name, ArrowType t, boolean nullable) {
        return new Field(name, new FieldType(nullable, t, null), Collections.emptyList());
    }

    private Introspect() {}

    /** Build the describe batch + metadata for an {@link RpcServer}. */
    public static Built build(String protocolName, Map<String, RpcMethodInfo> methods, String serverId) {
        // Sort by method name to match Python behaviour.
        Map<String, RpcMethodInfo> sorted = new TreeMap<>(methods);
        VectorSchemaRoot root = VectorSchemaRoot.create(DESCRIBE_SCHEMA, Allocators.root());
        root.allocateNew();
        int i = 0;
        for (var e : sorted.entrySet()) {
            RpcMethodInfo info = e.getValue();
            setUtf8(root, 0, i, info.name());
            setUtf8(root, 1, i, info.methodType().wireValue());
            if (info.doc() != null) setUtf8(root, 2, i, info.doc()); else ((VarCharVector) root.getVector(2)).setNull(i);
            ((BitVector) root.getVector(3)).setSafe(i, info.hasReturn() ? 1 : 0);
            setBinary(root, 4, i, serializeSchema(info.paramsSchema()));
            setBinary(root, 5, i, serializeSchema(info.resultSchema()));
            // param_types_json
            if (!info.paramTypes().isEmpty()) {
                Map<String, String> pt = new LinkedHashMap<>();
                for (var pe : info.paramTypes().entrySet()) pt.put(pe.getKey(), typeName(pe.getValue()));
                setUtf8(root, 6, i, toJson(pt));
            } else ((VarCharVector) root.getVector(6)).setNull(i);
            // param_defaults_json: we don't currently surface defaults from Java interfaces.
            ((VarCharVector) root.getVector(7)).setNull(i);
            ((BitVector) root.getVector(8)).setSafe(i, info.headerType() != null ? 1 : 0);
            if (info.headerType() != null && ArrowSerializableRecord.class.isAssignableFrom(info.headerType())) {
                setBinary(root, 9, i, serializeSchema(SchemaDerivation.schemaForRecord(
                        info.headerType().asSubclass(ArrowSerializableRecord.class))));
            } else ((VarBinaryVector) root.getVector(9)).setNull(i);
            if (info.isExchange() != null) ((BitVector) root.getVector(10)).setSafe(i, info.isExchange() ? 1 : 0);
            else ((BitVector) root.getVector(10)).setNull(i);
            ((VarCharVector) root.getVector(11)).setNull(i);
            i++;
        }
        root.setRowCount(sorted.size());

        Map<String, String> md = new LinkedHashMap<>();
        md.put(Metadata.PROTOCOL_NAME, protocolName);
        md.put(Metadata.REQUEST_VERSION_KEY, Metadata.REQUEST_VERSION);
        md.put(Metadata.DESCRIBE_VERSION_KEY, DESCRIBE_VERSION);
        md.put(Metadata.SERVER_ID, serverId);
        return new Built(root, md);
    }

    /** Result of {@link #build}. Caller owns {@link #root} and must close it. */
    public static final class Built {
        public final VectorSchemaRoot root;
        public final Map<String, String> customMetadata;
        Built(VectorSchemaRoot root, Map<String, String> md) { this.root = root; this.customMetadata = md; }
    }

    private static void setUtf8(VectorSchemaRoot root, int col, int row, String v) {
        ((VarCharVector) root.getVector(col)).setSafe(row, v.getBytes(StandardCharsets.UTF_8));
    }

    private static void setBinary(VectorSchemaRoot root, int col, int row, byte[] v) {
        ((VarBinaryVector) root.getVector(col)).setSafe(row, v);
    }

    /** Serialise a Schema to the minimal Arrow IPC schema-message bytes. */
    public static byte[] serializeSchema(Schema schema) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            WriteChannel ch = new WriteChannel(Channels.newChannel(bos));
            MessageSerializer.serialize(ch, schema);
            return bos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("schema serialize failed", e);
        }
    }

    private static String toJson(Object v) {
        try { return JSON.writeValueAsString(v); }
        catch (Exception e) { throw new RuntimeException("json serialize failed", e); }
    }

    /** Produce a Python-style type name for a Java Type. */
    public static String typeName(Type t) {
        if (t == null || t == void.class || t == Void.class) return "None";
        if (t instanceof ParameterizedType pt) {
            Class<?> raw = (Class<?>) pt.getRawType();
            Type[] args = pt.getActualTypeArguments();
            if (raw == Optional.class) return typeName(args[0]) + " | None";
            if (java.util.List.class.isAssignableFrom(raw)) return "list[" + typeName(args[0]) + "]";
            if (java.util.Map.class.isAssignableFrom(raw)) return "dict[" + typeName(args[0]) + ", " + typeName(args[1]) + "]";
        }
        if (t instanceof Class<?> c) {
            if (c == String.class) return "str";
            if (c == byte[].class) return "bytes";
            if (c == long.class || c == Long.class || c == int.class || c == Integer.class) return "int";
            if (c == double.class || c == Double.class || c == float.class || c == Float.class) return "float";
            if (c == boolean.class || c == Boolean.class) return "bool";
            return c.getSimpleName();
        }
        return t.toString();
    }

    // --- Client-side introspect(transport) --------------------------------

    /** Send a {@code __describe__} request over the transport. */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static ServiceDescription introspect(RpcTransport transport) {
        // Send request
        try (IpcStreamWriter w = new IpcStreamWriter(transport.writer())) {
            w.writeSchema(Stream.EMPTY_SCHEMA);
            try (VectorSchemaRoot zero = VectorSchemaRoot.create(Stream.EMPTY_SCHEMA, Allocators.root())) {
                zero.allocateNew();
                zero.setRowCount(1);
                Map<String, String> md = new LinkedHashMap<>();
                md.put(Metadata.RPC_METHOD, METHOD_NAME);
                md.put(Metadata.REQUEST_VERSION_KEY, Metadata.REQUEST_VERSION);
                w.writeBatch(zero, md);
            }
        } catch (Exception e) {
            throw new RpcError("TransportError", "introspect send failed: " + e.getMessage(), "");
        }
        try { transport.writer().flush(); } catch (Exception ignore) {}

        // Read response
        try (IpcStreamReader r = new IpcStreamReader(transport.reader(), Allocators.root())) {
            while (true) {
                Map<String, String> md = r.readNextBatch();
                if (md == null) throw new RpcError("ProtocolError", "describe stream empty", "");
                Wire.BatchKind kind = Wire.classify(r.root().getRowCount(), md);
                if (kind == Wire.BatchKind.LOG) continue;
                if (kind == Wire.BatchKind.ERROR) throw Wire.errorFromMetadata(md);
                return parse(r.root(), md);
            }
        } catch (RpcError e) { throw e;
        } catch (Exception e) { throw new RpcError("TransportError", "introspect read failed: " + e.getMessage(), ""); }
    }

    public static ServiceDescription parse(VectorSchemaRoot root, Map<String, String> meta) {
        String protocolName = meta.getOrDefault(Metadata.PROTOCOL_NAME, "");
        String requestVersion = meta.getOrDefault(Metadata.REQUEST_VERSION_KEY, "");
        String describeVersion = meta.getOrDefault(Metadata.DESCRIBE_VERSION_KEY, "");
        String serverId = meta.getOrDefault(Metadata.SERVER_ID, "");

        Map<String, MethodDescription> map = new LinkedHashMap<>();
        int rows = root.getRowCount();
        for (int i = 0; i < rows; i++) {
            String name = new String(((VarCharVector) root.getVector(0)).get(i), StandardCharsets.UTF_8);
            String mt = new String(((VarCharVector) root.getVector(1)).get(i), StandardCharsets.UTF_8);
            String doc = root.getVector(2).isNull(i) ? null
                    : new String(((VarCharVector) root.getVector(2)).get(i), StandardCharsets.UTF_8);
            boolean hasReturn = ((BitVector) root.getVector(3)).get(i) == 1;
            byte[] paramsBytes = ((VarBinaryVector) root.getVector(4)).get(i);
            byte[] resultBytes = ((VarBinaryVector) root.getVector(5)).get(i);
            Schema params = deserializeSchema(paramsBytes);
            Schema result = deserializeSchema(resultBytes);
            boolean hasHeader = ((BitVector) root.getVector(8)).get(i) == 1;
            Schema headerSchema = null;
            if (hasHeader && !root.getVector(9).isNull(i)) {
                byte[] b = ((VarBinaryVector) root.getVector(9)).get(i);
                headerSchema = deserializeSchema(b);
            }
            Boolean isExchange = null;
            if (!root.getVector(10).isNull(i)) {
                isExchange = ((BitVector) root.getVector(10)).get(i) == 1;
            }
            MethodType methodType = "stream".equals(mt) ? MethodType.STREAM : MethodType.UNARY;
            map.put(name, new MethodDescription(name, methodType, doc, hasReturn, params, result,
                    hasHeader, headerSchema, isExchange));
        }
        return new ServiceDescription(protocolName, requestVersion, describeVersion, serverId, map);
    }

    private static Schema deserializeSchema(byte[] bytes) {
        try {
            org.apache.arrow.vector.ipc.ReadChannel rc = new org.apache.arrow.vector.ipc.ReadChannel(
                    Channels.newChannel(new java.io.ByteArrayInputStream(bytes)));
            return MessageSerializer.deserializeSchema(rc);
        } catch (Exception e) {
            throw new RuntimeException("schema deserialize failed", e);
        }
    }

    // --- Result records ---------------------------------------------------

    public record MethodDescription(
            String name,
            MethodType methodType,
            String doc,
            boolean hasReturn,
            Schema paramsSchema,
            Schema resultSchema,
            boolean hasHeader,
            Schema headerSchema,
            Boolean isExchange
    ) {}

    public record ServiceDescription(
            String protocolName,
            String requestVersion,
            String describeVersion,
            String serverId,
            Map<String, MethodDescription> methods
    ) {}
}
