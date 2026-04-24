// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.wire;

import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.VersionError;
import farm.query.vgirpc.log.Level;
import farm.query.vgirpc.log.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Helpers shared by the server and client wire paths. */
public final class Wire {

    private static final ObjectMapper JSON = new ObjectMapper();

    private Wire() {}

    /** Build the request batch metadata: method name + protocol version. */
    public static Map<String, String> requestMetadata(String methodName) {
        Map<String, String> m = new LinkedHashMap<>();
        m.put(Metadata.RPC_METHOD, methodName);
        m.put(Metadata.REQUEST_VERSION_KEY, Metadata.REQUEST_VERSION);
        return m;
    }

    /** Validate the {@code vgi_rpc.request_version} key on a request batch. */
    public static void validateRequestVersion(Map<String, String> meta) {
        String v = meta.get(Metadata.REQUEST_VERSION_KEY);
        if (v == null) {
            throw new VersionError("Missing 'vgi_rpc.request_version' in request batch custom_metadata. "
                    + "Set the value to '" + Metadata.REQUEST_VERSION + "'.");
        }
        if (!Metadata.REQUEST_VERSION.equals(v)) {
            throw new VersionError("Unsupported request version '" + v
                    + "', expected '" + Metadata.REQUEST_VERSION + "'.");
        }
    }

    /** Extract the method name from request metadata, or throw {@link RpcError}. */
    public static String requireMethodName(Map<String, String> meta) {
        String name = meta.get(Metadata.RPC_METHOD);
        if (name == null) {
            throw new RpcError("ProtocolError",
                    "Missing 'vgi_rpc.method' in request batch custom_metadata.", "");
        }
        return name;
    }

    /** Build a complete error stream (schema + zero-row error batch + EOS). */
    public static void writeErrorStream(OutputStream out, Schema schema,
                                         Throwable t, String serverId) throws IOException {
        try (IpcStreamWriter w = new IpcStreamWriter(out)) {
            w.writeSchema(schema);
            try (VectorSchemaRoot zero = VectorSchemaRoot.create(schema, Allocators.root())) {
                zero.allocateNew();
                zero.setRowCount(0);
                w.writeBatch(zero, errorMetadata(t, serverId));
            }
        }
    }

    /** Build the metadata for an error/log batch. */
    public static Map<String, String> errorMetadata(Throwable t, String serverId) {
        Message msg = Message.fromException(t);
        Map<String, String> md = msg.addToMetadata(null);
        if (serverId != null) md.put(Metadata.SERVER_ID, serverId);
        return md;
    }

    /** Classify a zero-row batch as data, log, or error. Returns {@link BatchKind#DATA} otherwise. */
    public enum BatchKind { DATA, LOG, ERROR }

    /** Inspect zero-row batch metadata to decide whether it carries log/error semantics. */
    public static BatchKind classify(int rows, Map<String, String> meta) {
        if (meta == null || rows != 0) return BatchKind.DATA;
        String level = meta.get(Metadata.LOG_LEVEL);
        String text = meta.get(Metadata.LOG_MESSAGE);
        if (level == null || text == null) return BatchKind.DATA;
        return Level.EXCEPTION.name().equals(level) ? BatchKind.ERROR : BatchKind.LOG;
    }

    /** Construct an {@link RpcError} from an EXCEPTION-level batch's metadata. */
    public static RpcError errorFromMetadata(Map<String, String> meta) {
        String text = meta.get(Metadata.LOG_MESSAGE);
        String extraJson = meta.get(Metadata.LOG_EXTRA);
        String exType = "RpcError";
        String tb = "";
        if (extraJson != null) {
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> extra = JSON.readValue(extraJson, Map.class);
                Object t = extra.get("exception_type");
                if (t != null) exType = t.toString();
                Object trace = extra.get("traceback");
                if (trace != null) tb = trace.toString();
            } catch (Exception ignore) {}
        }
        return new RpcError(exType, text != null ? text : "", tb);
    }

    /** Construct a {@link Message} from non-exception log-batch metadata. */
    public static Message messageFromMetadata(Map<String, String> meta) {
        Level lvl = Level.fromWire(meta.get(Metadata.LOG_LEVEL));
        String text = meta.get(Metadata.LOG_MESSAGE);
        Map<String, Object> extra = null;
        String extraJson = meta.get(Metadata.LOG_EXTRA);
        if (extraJson != null) {
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> parsed = JSON.readValue(extraJson, Map.class);
                extra = parsed;
            } catch (Exception ignore) {}
        }
        return new Message(lvl, text, extra);
    }
}
