// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.wire;

import farm.query.vgirpc.HasErrorKind;
import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.VersionError;
import farm.query.vgirpc.external.LocationResolver;
import farm.query.vgirpc.log.Level;
import farm.query.vgirpc.log.Message;
import farm.query.vgirpc.marshal.Marshalling;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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

    // ------------------------------------------------------------------
    // Intermediary framing helpers
    //
    // vgirpc has two first-class roles: client ({@link RpcConnection}) and
    // server ({@link farm.query.vgirpc.RpcServer}). A third role — an
    // *intermediary* (proxy, router, gateway, test harness) — needs to read a
    // request off the wire, rewrite it, re-frame it for forwarding, and
    // synthesize in-band error responses without standing up either. These are
    // the stable public surface for that role. Mirrors vgi-rpc's `vgi_rpc.wire`.
    // ------------------------------------------------------------------

    /**
     * A request parsed off the wire.
     *
     * @param method the dispatched RPC method name
     * @param kwargs the parameter values, keyed by field name
     * @param protocolVersion the application protocol version stamped on the
     *     request, or {@code null} when it carried none (structurally exempt
     *     from the backend's dispatch-boundary version check)
     */
    public record Request(String method, Map<String, Object> kwargs, String protocolVersion) {}

    /**
     * A unary response unwrapped to its envelope schema and raw result bytes.
     *
     * @param envelopeSchema the response envelope schema (a single {@code result} field)
     * @param result the serialized response object
     */
    public record UnaryResult(Schema envelopeSchema, byte[] result) {}

    /**
     * Parse a request IPC body into its method name and keyword arguments.
     *
     * @param data the complete request IPC stream bytes
     * @return the parsed request
     * @throws IOException if the body is not a readable IPC stream
     * @throws RpcError if the request carries no method name
     * @throws VersionError if the request version is missing or unsupported
     */
    public static Request readRequest(byte[] data) throws IOException {
        return readRequest(data, null);
    }

    /**
     * Parse a request IPC body, optionally resolving an externalized
     * ({@code vgi_rpc.location}) pointer request by fetching the referenced bytes.
     *
     * @param data the complete request IPC stream bytes
     * @param resolver resolves pointer requests; {@code null} disables resolution,
     *     so a pointer request parses to its zero-row (empty-kwargs) form — callers
     *     that require resolution should treat that as a fail-closed denial
     * @return the parsed request
     * @throws IOException if the body is not a readable IPC stream
     * @throws RpcError if the request carries no method name
     * @throws VersionError if the request version is missing or unsupported
     */
    public static Request readRequest(byte[] data, LocationResolver resolver) throws IOException {
        try (IpcStreamReader reader = new IpcStreamReader(
                new ByteArrayInputStream(data), Allocators.root())) {
            Map<String, String> meta = reader.readNextBatch();
            if (meta == null) throw new RpcError("ProtocolError", "Empty request stream.", "");
            validateRequestVersion(meta);
            String method = requireMethodName(meta);

            VectorSchemaRoot root = reader.root();
            Map<String, Object> kwargs;
            if (resolver != null && LocationResolver.isPointer(root.getRowCount(), meta)) {
                LocationResolver.Resolved resolved;
                try {
                    resolved = resolver.resolve(meta);
                } catch (Exception e) {
                    throw new IOException("failed to resolve externalized request", e);
                }
                try (VectorSchemaRoot resolvedRoot = resolved.root()) {
                    kwargs = resolvedRoot.getRowCount() == 0
                            ? new LinkedHashMap<>()
                            : Marshalling.decodeRow(resolvedRoot, null, resolvedRoot.getSchema());
                }
            } else if (root.getRowCount() == 0) {
                kwargs = new LinkedHashMap<>();
            } else {
                kwargs = Marshalling.decodeRow(root, reader.dictionaryProvider(), reader.wireSchema());
            }
            return new Request(method, kwargs, meta.get(Metadata.PROTOCOL_VERSION_KEY));
        }
    }

    /**
     * Frame a request as a complete IPC stream body for forwarding.
     *
     * @param method the RPC method name (e.g. {@code "bind"})
     * @param paramsSchema the method's parameter schema
     * @param kwargs the parameter values, keyed by field name
     * @param protocolVersion the application protocol version to stamp, so a
     *     versioned backend's dispatch-boundary check still sees the originating
     *     client's version; {@code null} omits the key
     * @return the framed request IPC stream bytes
     * @throws IOException on a write failure
     */
    public static byte[] writeRequest(String method, Schema paramsSchema,
                                        Map<String, Object> kwargs, String protocolVersion)
            throws IOException {
        Map<String, String> meta = requestMetadata(method);
        if (protocolVersion != null) meta.put(Metadata.PROTOCOL_VERSION_KEY, protocolVersion);
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try (IpcStreamWriter w = new IpcStreamWriter(buf)) {
            w.writeSchema(paramsSchema);
            if (paramsSchema.getFields().isEmpty()) {
                try (VectorSchemaRoot zero = VectorSchemaRoot.create(paramsSchema, Allocators.root())) {
                    zero.allocateNew();
                    zero.setRowCount(1);
                    w.writeBatch(zero, meta);
                }
            } else {
                try (VectorSchemaRoot root = Marshalling.encodeRow(paramsSchema, kwargs, Allocators.root())) {
                    w.writeBatch(root, meta);
                }
            }
        }
        return buf.toByteArray();
    }

    /**
     * Build a complete IPC stream carrying a single error batch — the wire shape
     * an intermediary returns to deny or abort a call in-band. The client decodes
     * it back into a raised exception.
     *
     * @param t the exception to encode; its type and message reach the client
     * @param schema the stream schema, or {@code null} for an empty schema
     * @param serverId optional server id to stamp on the error batch
     * @return the error IPC stream bytes
     * @throws IOException on a write failure
     */
    public static byte[] buildErrorStream(Throwable t, Schema schema, String serverId) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        writeErrorStream(buf, schema != null ? schema : new Schema(List.of()), t, serverId);
        return buf.toByteArray();
    }

    /**
     * Return the stream-state continuation token carried in a request or response body.
     *
     * <p>The token (key {@link Metadata#STREAM_STATE}) rides in a record batch's
     * {@code custom_metadata}, not a header — stream continuations recover their
     * state from it, so an intermediary that routes or correlates a stream by it
     * must read the batch metadata. Two body shapes are handled by one walk: an
     * exchange <em>request</em> is a single IPC stream whose first batch carries
     * the token, while a producer init/exchange <em>response</em> may be several
     * concatenated IPC streams (a header stream followed by the producer's data
     * stream), so the token can be in a later stream.
     *
     * <p>Returns the <em>first</em> token found. For a response that rotates the
     * token across several data batches the <em>last</em> one is the continuation
     * the peer will send next; single-token responses (the common case) make them
     * identical.
     *
     * @param data the (decompressed) request or response IPC body bytes
     * @return the state token, or {@code null} when absent or unparseable
     */
    public static String findStateToken(byte[] data) {
        return findBatchMetadata(data, Metadata.STREAM_STATE, /*walkConcatenated=*/true);
    }

    /**
     * Return the application protocol version stamped on a request body.
     *
     * <p>An intermediary that rewrites a request must recover and re-stamp this
     * (see {@link #writeRequest}) so the backend's dispatch-boundary version check
     * still sees the originating client's version.
     *
     * @param data the (decompressed) request IPC body bytes
     * @return the protocol-version string, or {@code null} when absent or unparseable
     */
    public static String findProtocolVersion(byte[] data) {
        return findBatchMetadata(data, Metadata.PROTOCOL_VERSION_KEY, /*walkConcatenated=*/false);
    }

    /** Scan batch custom_metadata for {@code key}, optionally across concatenated IPC streams. */
    private static String findBatchMetadata(byte[] data, String key, boolean walkConcatenated) {
        int offset = 0;
        do {
            CountingInputStream in = new CountingInputStream(new ByteArrayInputStream(data, offset,
                    data.length - offset));
            try (IpcStreamReader reader = new IpcStreamReader(in, Allocators.root())) {
                Map<String, String> meta;
                while ((meta = reader.readNextBatch()) != null) {
                    String value = meta.get(key);
                    if (value != null && !value.isEmpty()) return value;
                }
            } catch (Exception e) {
                return null;
            }
            if (!walkConcatenated || in.count() == 0) return null;
            offset += in.count();
        } while (offset < data.length);
        return null;
    }

    /** Tracks how many bytes an {@link IpcStreamReader} consumed, so a
     *  concatenated-stream walk can advance to the next stream. */
    private static final class CountingInputStream extends java.io.FilterInputStream {
        private int count;

        CountingInputStream(java.io.InputStream in) { super(in); }

        int count() { return count; }

        @Override public int read() throws IOException {
            int b = super.read();
            if (b >= 0) count++;
            return b;
        }

        @Override public int read(byte[] b, int off, int len) throws IOException {
            int n = super.read(b, off, len);
            if (n > 0) count += n;
            return n;
        }

        @Override public long skip(long n) throws IOException {
            long s = super.skip(n);
            count += (int) s;
            return s;
        }
    }

    /**
     * Unwrap a unary-RPC response to its envelope schema and raw result bytes.
     *
     * <p>A unary response is an IPC stream of zero or more leading zero-row log
     * batches (carrying {@link Metadata#LOG_LEVEL}) followed by one data batch
     * whose {@code result} column holds the serialized response object. This
     * returns the envelope schema plus the <em>raw</em> result bytes — no typed
     * decode — for an intermediary that inspects or rewrites the response and
     * re-wraps it via {@link #writeUnaryResult}.
     *
     * <p>Lenient: returns {@code null} for an error, empty, or
     * non-{@code result} stream, so the caller can forward it unchanged.
     *
     * @param data the (resolved, decompressed) response IPC body bytes
     * @return the envelope schema and result bytes, or {@code null}
     */
    public static UnaryResult readUnaryResult(byte[] data) {
        try (IpcStreamReader reader = new IpcStreamReader(
                new ByteArrayInputStream(data), Allocators.root())) {
            Map<String, String> meta;
            while ((meta = reader.readNextBatch()) != null) {
                VectorSchemaRoot root = reader.root();
                if (root.getRowCount() > 0) {
                    if (root.getSchema().findField("result") == null) return null;
                    Object value = Marshalling.decodeRow(root).get("result");
                    return value instanceof byte[] bytes
                            ? new UnaryResult(root.getSchema(), bytes) : null;
                }
                if (!meta.containsKey(Metadata.LOG_LEVEL)) return null;
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Build a unary-RPC response IPC stream wrapping {@code result} — the inverse
     * of {@link #readUnaryResult}.
     *
     * @param envelopeSchema the response envelope schema (a single {@code result} field)
     * @param result the serialized response object
     * @return the response IPC stream bytes
     * @throws IOException on a write failure
     */
    public static byte[] writeUnaryResult(Schema envelopeSchema, byte[] result) throws IOException {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put(envelopeSchema.getFields().get(0).getName(), result);
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try (IpcStreamWriter w = new IpcStreamWriter(buf)) {
            w.writeSchema(envelopeSchema);
            try (VectorSchemaRoot root = Marshalling.encodeRow(envelopeSchema, row, Allocators.root())) {
                w.writeBatch(root, null);
            }
        }
        return buf.toByteArray();
    }
}
