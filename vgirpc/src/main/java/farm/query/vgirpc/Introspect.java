// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

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
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Implementation of the {@code __describe__} synthetic RPC method used for
 * runtime service introspection. Matches Python {@code vgi_rpc.introspect}
 * on the wire: DESCRIBE_VERSION 4, slim 8-field schema, plus a SHA-256
 * {@code protocol_hash} carried in batch custom_metadata.
 *
 * <p>Python-flavoured fields ({@code doc}, {@code param_types_json},
 * {@code param_defaults_json}, {@code param_docs_json}) are not on the
 * wire; the Arrow IPC schema bytes are the authoritative type information.
 */
public final class Introspect {

    public static final String METHOD_NAME = "__describe__";
    public static final String DESCRIBE_VERSION = "4";

    public static final Schema DESCRIBE_SCHEMA = new Schema(List.of(
            field("name", new ArrowType.Utf8(), false),
            field("method_type", new ArrowType.Utf8(), false),
            field("has_return", new ArrowType.Bool(), false),
            field("params_schema_ipc", new ArrowType.Binary(), false),
            field("result_schema_ipc", new ArrowType.Binary(), false),
            field("has_header", new ArrowType.Bool(), false),
            field("header_schema_ipc", new ArrowType.Binary(), true),
            field("is_exchange", new ArrowType.Bool(), true)));

    private static Field field(String name, ArrowType t, boolean nullable) {
        return new Field(name, new FieldType(nullable, t, null), Collections.emptyList());
    }

    private Introspect() {}

    /** Build the describe batch + metadata for an {@link RpcServer}. */
    public static Built build(String protocolName, Map<String, RpcMethodInfo> methods, String serverId) {
        Map<String, RpcMethodInfo> sorted = new TreeMap<>(methods);
        VectorSchemaRoot root = VectorSchemaRoot.create(DESCRIBE_SCHEMA, Allocators.root());
        root.allocateNew();

        // Track per-row canonical inputs to feed into the hash.
        int n = sorted.size();
        String[] hashNames = new String[n];
        String[] hashMethodTypes = new String[n];
        boolean[] hashHasReturns = new boolean[n];
        boolean[] hashHasHeaders = new boolean[n];
        Boolean[] hashIsExchanges = new Boolean[n];
        byte[][] hashParamsIpc = new byte[n][];
        byte[][] hashResultIpc = new byte[n][];
        byte[][] hashHeaderIpc = new byte[n][];

        int i = 0;
        for (var e : sorted.entrySet()) {
            RpcMethodInfo info = e.getValue();
            setUtf8(root, 0, i, info.name());
            String methodType = info.methodType().wireValue();
            setUtf8(root, 1, i, methodType);
            ((BitVector) root.getVector(2)).setSafe(i, info.hasReturn() ? 1 : 0);
            byte[] paramsIpc = serializeSchema(info.paramsSchema());
            byte[] resultIpc = serializeSchema(info.resultSchema());
            setBinary(root, 3, i, paramsIpc);
            setBinary(root, 4, i, resultIpc);
            boolean hasHeader = info.headerType() != null;
            ((BitVector) root.getVector(5)).setSafe(i, hasHeader ? 1 : 0);
            byte[] headerIpc = null;
            if (hasHeader && ArrowSerializableRecord.class.isAssignableFrom(info.headerType())) {
                headerIpc = serializeSchema(SchemaDerivation.schemaForRecord(
                        info.headerType().asSubclass(ArrowSerializableRecord.class)));
                setBinary(root, 6, i, headerIpc);
            } else {
                ((VarBinaryVector) root.getVector(6)).setNull(i);
            }
            Boolean isExchange = null;
            switch (info.streamKind()) {
                case EXCHANGE -> { ((BitVector) root.getVector(7)).setSafe(i, 1); isExchange = Boolean.TRUE; }
                case PRODUCER -> { ((BitVector) root.getVector(7)).setSafe(i, 0); isExchange = Boolean.FALSE; }
                case UNKNOWN  -> ((BitVector) root.getVector(7)).setNull(i);
            }

            hashNames[i] = info.name();
            hashMethodTypes[i] = methodType;
            hashHasReturns[i] = info.hasReturn();
            hashHasHeaders[i] = hasHeader;
            hashIsExchanges[i] = isExchange;
            hashParamsIpc[i] = paramsIpc;
            hashResultIpc[i] = resultIpc;
            hashHeaderIpc[i] = headerIpc;
            i++;
        }
        root.setRowCount(n);

        String protocolHash = computeProtocolHash(
                protocolName, hashNames, hashMethodTypes, hashHasReturns,
                hashHasHeaders, hashIsExchanges, hashParamsIpc, hashResultIpc, hashHeaderIpc);

        Map<String, String> md = new LinkedHashMap<>();
        md.put(Metadata.PROTOCOL_NAME, protocolName);
        md.put(Metadata.REQUEST_VERSION_KEY, Metadata.REQUEST_VERSION);
        md.put(Metadata.DESCRIBE_VERSION_KEY, DESCRIBE_VERSION);
        md.put(Metadata.PROTOCOL_HASH_KEY, protocolHash);
        md.put(Metadata.SERVER_ID, serverId);
        return new Built(root, md);
    }

    /**
     * Compute the SHA-256 hex digest of the canonical describe payload.
     * Mirrors Python {@code vgi_rpc.introspect.compute_protocol_hash}
     * byte-for-byte.
     */
    public static String computeProtocolHash(
            String protocolName,
            String[] names, String[] methodTypes,
            boolean[] hasReturns, boolean[] hasHeaders,
            Boolean[] isExchanges,
            byte[][] paramsIpc, byte[][] resultIpc, byte[][] headerIpc) {
        try {
            MessageDigest h = MessageDigest.getInstance("SHA-256");
            h.update("vgi_rpc.describe.v".getBytes(StandardCharsets.UTF_8));
            h.update(DESCRIBE_VERSION.getBytes(StandardCharsets.UTF_8));
            h.update((byte) '|');
            h.update(Metadata.REQUEST_VERSION.getBytes(StandardCharsets.UTF_8));
            h.update((byte) '|');
            h.update(protocolName.getBytes(StandardCharsets.UTF_8));
            h.update((byte) '|');
            for (int i = 0; i < names.length; i++) {
                h.update((byte) 0x1f);
                h.update(names[i].getBytes(StandardCharsets.UTF_8));
                h.update((byte) 0x1e);
                h.update(methodTypes[i].getBytes(StandardCharsets.UTF_8));
                h.update((byte) 0x1e);
                h.update((byte) (hasReturns[i] ? '1' : '0'));
                h.update((byte) 0x1e);
                h.update((byte) (hasHeaders[i] ? '1' : '0'));
                h.update((byte) 0x1e);
                if (isExchanges[i] == null) h.update((byte) '-');
                else h.update((byte) (isExchanges[i] ? '1' : '0'));
                h.update((byte) 0x1e);
                h.update(paramsIpc[i]);
                h.update((byte) 0x1e);
                h.update(resultIpc[i]);
                h.update((byte) 0x1e);
                if (headerIpc[i] != null) h.update(headerIpc[i]);
            }
            return HexFormat.of().formatHex(h.digest());
        } catch (Exception e) {
            throw new RuntimeException("protocol_hash failed", e);
        }
    }

    /** Result of {@link #build}. Caller owns {@link #root()} and must close it. */
    public record Built(VectorSchemaRoot root, Map<String, String> customMetadata) {}

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

    // --- Client-side introspect(transport) --------------------------------

    /** Send a {@code __describe__} request over the transport. */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static ServiceDescription introspect(RpcTransport transport) {
        try (IpcStreamWriter w = new IpcStreamWriter(transport.writer())) {
            w.writeSchema(RpcStream.EMPTY_SCHEMA);
            try (VectorSchemaRoot zero = VectorSchemaRoot.create(RpcStream.EMPTY_SCHEMA, Allocators.root())) {
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

    /**
     * Decode a {@code __describe__} response batch (the slim 8-column
     * {@link #DESCRIBE_SCHEMA}) plus its metadata into a {@link ServiceDescription}.
     *
     * @param root the describe response batch
     * @param meta the batch's custom metadata (protocol name/version, hash, server id)
     * @return the parsed service description
     */
    public static ServiceDescription parse(VectorSchemaRoot root, Map<String, String> meta) {
        String protocolName = meta.getOrDefault(Metadata.PROTOCOL_NAME, "");
        String requestVersion = meta.getOrDefault(Metadata.REQUEST_VERSION_KEY, "");
        String describeVersion = meta.getOrDefault(Metadata.DESCRIBE_VERSION_KEY, "");
        String protocolHash = meta.getOrDefault(Metadata.PROTOCOL_HASH_KEY, "");
        String serverId = meta.getOrDefault(Metadata.SERVER_ID, "");

        Map<String, MethodDescription> map = new LinkedHashMap<>();
        int rows = root.getRowCount();
        for (int i = 0; i < rows; i++) {
            String name = new String(((VarCharVector) root.getVector(0)).get(i), StandardCharsets.UTF_8);
            String mt = new String(((VarCharVector) root.getVector(1)).get(i), StandardCharsets.UTF_8);
            boolean hasReturn = ((BitVector) root.getVector(2)).get(i) == 1;
            byte[] paramsBytes = ((VarBinaryVector) root.getVector(3)).get(i);
            byte[] resultBytes = ((VarBinaryVector) root.getVector(4)).get(i);
            Schema params = deserializeSchema(paramsBytes);
            Schema result = deserializeSchema(resultBytes);
            boolean hasHeader = ((BitVector) root.getVector(5)).get(i) == 1;
            Schema headerSchema = null;
            if (hasHeader && !root.getVector(6).isNull(i)) {
                byte[] b = ((VarBinaryVector) root.getVector(6)).get(i);
                headerSchema = deserializeSchema(b);
            }
            Boolean isExchange = null;
            if (!root.getVector(7).isNull(i)) {
                isExchange = ((BitVector) root.getVector(7)).get(i) == 1;
            }
            MethodType methodType = "stream".equals(mt) ? MethodType.STREAM : MethodType.UNARY;
            map.put(name, new MethodDescription(name, methodType, hasReturn, params, result,
                    hasHeader, headerSchema, isExchange));
        }
        return new ServiceDescription(protocolName, requestVersion, describeVersion, protocolHash, serverId, map);
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

    /**
     * The introspected description of a single method, as returned by
     * {@code __describe__}: its name, {@link MethodType}, whether it returns a
     * value, the params/result/header Arrow schemas, and (for streams) whether it
     * is an exchange.
     */
    public record MethodDescription(
            String name,
            MethodType methodType,
            boolean hasReturn,
            Schema paramsSchema,
            Schema resultSchema,
            boolean hasHeader,
            Schema headerSchema,
            Boolean isExchange
    ) {}

    /**
     * The introspected description of a whole service, as returned by
     * {@code __describe__}: the protocol name, wire/describe versions, the
     * protocol hash (byte-identical to the Python reference), the server id, and
     * the per-method {@link MethodDescription}s keyed by method name.
     */
    public record ServiceDescription(
            String protocolName,
            String requestVersion,
            String describeVersion,
            String protocolHash,
            String serverId,
            Map<String, MethodDescription> methods
    ) {}
}
