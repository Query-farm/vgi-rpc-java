// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.wire;

import farm.query.vgirpc.HasErrorKind;
import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.VersionError;
import farm.query.vgirpc.log.Level;
import farm.query.vgirpc.log.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
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
            writeZeroBatch(w, schema, errorMetadata(t, serverId));
        }
    }

    /**
     * Write a single zero-row batch with the given schema and metadata.
     *
     * <p>Centralises the {@code VectorSchemaRoot.create + allocateNew + setRowCount(0) + writeBatch}
     * idiom used by log, tick, and pointer batches throughout the wire path.
     */
    public static void writeZeroBatch(IpcStreamWriter w, Schema schema,
                                      Map<String, String> customMetadata) throws IOException {
        try (VectorSchemaRoot zero = VectorSchemaRoot.create(schema, Allocators.root())) {
            zero.allocateNew();
            zero.setRowCount(0);
            // A dict-encoded (ENUM) field needs *some* dictionary registered for
            // Arrow to render even the schema message of this zero-row batch
            // (DictionaryUtility.toMessageFormat). Data batches carry their own
            // provider; these zero-row token/continuation batches don't, so
            // supply empty dictionaries.
            List<FieldVector> owned = new ArrayList<>();
            DictionaryProvider provider = emptyDictionaries(schema, owned);
            try {
                if (provider != null) w.writeBatch(zero, customMetadata, provider);
                else w.writeBatch(zero, customMetadata);
            } finally {
                for (FieldVector v : owned) v.close();
            }
        }
    }

    /** Build empty dictionaries for every dict-encoded field in {@code schema}
     *  (recursively), or {@code null} when there are none. Created vectors are
     *  added to {@code owned} for the caller to close after the write. */
    private static DictionaryProvider emptyDictionaries(Schema schema, List<FieldVector> owned) {
        DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
        boolean any = collectEmptyDictionaries(schema.getFields(), provider, owned);
        return any ? provider : null;
    }

    private static boolean collectEmptyDictionaries(List<Field> fields,
            DictionaryProvider.MapDictionaryProvider provider, List<FieldVector> owned) {
        boolean any = false;
        for (Field f : fields) {
            DictionaryEncoding enc = f.getDictionary();
            if (enc != null && provider.lookup(enc.getId()) == null) {
                Field valueField = new Field(f.getName(),
                        new FieldType(f.isNullable(), f.getType(), null, f.getMetadata()), f.getChildren());
                FieldVector vec = valueField.createVector(Allocators.root());
                vec.allocateNew();
                vec.setValueCount(0);
                owned.add(vec);
                provider.put(new Dictionary(vec, enc));
                any = true;
            }
            if (!f.getChildren().isEmpty()) {
                any |= collectEmptyDictionaries(f.getChildren(), provider, owned);
            }
        }
        return any;
    }

    /** Build the metadata for an error/log batch. */
    public static Map<String, String> errorMetadata(Throwable t, String serverId) {
        Message msg = Message.fromException(t);
        Map<String, String> md = msg.addToMetadata(null);
        if (serverId != null) md.put(Metadata.SERVER_ID, serverId);
        if (t instanceof HasErrorKind hk) {
            md.put(Metadata.ERROR_KIND, hk.errorKind());
        }
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
        // Hoist the stable error_kind metadata into RpcError so Java-to-Java
        // callers can pattern-match without parsing message text — parity
        // with the Python client's RpcError.error_kind attribute.
        String kind = meta.get(Metadata.ERROR_KIND);
        return new RpcError(exType, text != null ? text : "", tb, "", kind);
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
