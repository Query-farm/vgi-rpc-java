// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.external;

import farm.query.vgirpc.log.Message;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import farm.query.vgirpc.wire.Metadata;
import farm.query.vgirpc.wire.Wire;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Detect and transparently resolve external-location pointer batches.
 *
 * <p>A pointer batch is a zero-row batch whose custom metadata carries
 * {@code vgi_rpc.location}. On resolution the fetcher downloads the IPC bytes
 * from the URL, opens them as an Arrow stream, discards any log/error batches
 * (log dispatch is the caller's responsibility), and returns the (single) data
 * batch as a fresh {@link VectorSchemaRoot} plus metadata with the location
 * key stripped.</p>
 */
public final class LocationResolver {

    private final ExternalLocationConfig config;
    private final ExternalFetcher fetcher;

    /**
     * @param config configuration backing the underlying {@link ExternalFetcher}
     */
    public LocationResolver(ExternalLocationConfig config) {
        this.config = config;
        this.fetcher = new ExternalFetcher(config);
    }

    /** @return the configuration this resolver was built with. */
    public ExternalLocationConfig config() { return config; }

    /** True iff the metadata describes an external-location pointer. */
    public static boolean isPointer(int rowCount, Map<String, String> meta) {
        return rowCount == 0 && meta != null && meta.get(Metadata.LOCATION) != null
                && meta.get(Metadata.LOG_LEVEL) == null;
    }

    /**
     * Render an external-location URL without userinfo, query credentials, or
     * fragments. Safe for exception messages and logs.
     *
     * @param url untrusted external-location URL text
     * @return scheme/host/port/path only, or a fixed redacted marker
     */
    public static String redactUrl(String url) {
        if (url == null) return "<redacted-url>";
        try {
            return ExternalFetcher.safeUri(URI.create(url));
        } catch (IllegalArgumentException invalid) {
            return "<redacted-url>";
        }
    }

    /**
     * Resolve a pointer batch to its underlying data batch. Ownership of the
     * returned root transfers to the caller (must close). {@code metaOut} is
     * populated with the merged custom metadata minus {@code vgi_rpc.location}
     * and {@code vgi_rpc.location.sha256}; it may be null.
     */
    public Resolved resolve(Map<String, String> pointerMeta) throws Exception {
        return resolve(pointerMeta, null);
    }

    /**
     * Resolve a pointer batch, relaying any log batches the fetched object
     * carries to {@code onLog}.
     *
     * <p>The object is a whole IPC stream, and a peer that externalizes a
     * complete turn puts that turn's log batches in it beside the data batch --
     * the Python reference does exactly this. A resolver with nowhere to send
     * them drops them, and the loss is invisible to every assertion that only
     * looks at data, which is why this overload exists rather than a
     * {@code continue}.
     *
     * @param pointerMeta the pointer batch's custom metadata
     * @param onLog sink for log batches found inside the object, or {@code null}
     *     to discard them
     * @return the fetched batch, its metadata and its dictionaries
     * @throws Exception if the fetch or the decode fails
     */
    public Resolved resolve(Map<String, String> pointerMeta, java.util.function.Consumer<Message> onLog)
            throws Exception {
        String url = pointerMeta.get(Metadata.LOCATION);
        if (url == null) throw new IllegalArgumentException("pointer batch missing vgi_rpc.location");
        String sha = pointerMeta.get(Metadata.LOCATION_SHA256);
        URI location;
        try {
            location = URI.create(url);
        } catch (IllegalArgumentException invalid) {
            throw new java.io.IOException("invalid external location URL: <redacted-url>");
        }
        long startedNanos = System.nanoTime();
        byte[] body = fetcher.fetch(location, sha);
        double elapsedMs = (System.nanoTime() - startedNanos) / 1_000_000.0;

        // Open the fetched stream, advance past any log/error batches, and take
        // the first data batch. We copy it into a caller-owned root because the
        // IpcStreamReader's root is recycled on close.
        try (IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(body), Allocators.root())) {
            while (true) {
                Map<String, String> md = r.readNextBatch();
                if (md == null) throw new java.io.IOException("external stream contained no data batch");
                VectorSchemaRoot root = r.root();
                Wire.BatchKind kind = Wire.classify(root.getRowCount(), md);
                if (kind == Wire.BatchKind.LOG) {
                    if (onLog != null) onLog.accept(Wire.messageFromMetadata(md));
                    continue;
                }
                if (kind == Wire.BatchKind.ERROR) throw Wire.errorFromMetadata(md);
                Copy copy = copyRoot(root, r.dictionaryProvider());
                Map<String, String> merged = new LinkedHashMap<>(pointerMeta);
                merged.remove(Metadata.LOCATION);
                merged.remove(Metadata.LOCATION_SHA256);
                if (md != null) merged.putAll(md);
                // Stamped at resolve time, not copied from the pointer. A reader
                // that only passes through what the writer happened to set
                // reports nothing when the writer set nothing -- and this port's
                // own Externalizer pre-stamps the source, so that gap passes by
                // coincidence against a Java peer and yields empty provenance
                // against every other. WIRE_PROTOCOL.md §12 requires both keys
                // on a resolved batch: how long the fetch took, and where from.
                merged.put(Metadata.LOCATION_FETCH, String.format(java.util.Locale.ROOT, "%.1f", elapsedMs));
                merged.put(Metadata.LOCATION_SOURCE, url);
                return new Resolved(copy.root(), merged, copy.dictionaries(), copy.owned());
            }
        }
    }

    /** A copied batch and the dictionary vectors it refers to, all caller-owned. */
    private record Copy(VectorSchemaRoot root, DictionaryProvider dictionaries, List<FieldVector> owned) {}

    /**
     * Copy a batch out of the reader that owns it, dictionaries included.
     *
     * <p>Two things here are easy to get wrong and were.
     *
     * <p>The copy is made through each source vector's own
     * {@link org.apache.arrow.vector.util.TransferPair}, not by creating a root
     * from the schema and copying cell by cell. A dictionary-encoded column is
     * stored as indices, but its <em>field</em> declares the value type, so
     * {@code VectorSchemaRoot.create} builds a {@code VarCharVector} where the
     * source holds a {@code SmallIntVector} and the copy is nonsense. Asking the
     * source vector for its own transfer pair cannot make that mistake.
     *
     * <p>The dictionaries are copied too. They travel beside a batch and never
     * inside it, so a root handed on without them is not a batch that has lost
     * an optimisation — it is a batch that cannot be written or read at all
     * ("Could not find dictionary with ID 0"), and the failure lands on whoever
     * touches it next, a layer away from the fetch that dropped them. Nothing in
     * the shared suite externalized over a byte-stream transport until
     * {@code TestExternalByteStream} landed, so this half stayed latent while the
     * same defect was being fixed on the HTTP path.
     */
    private static Copy copyRoot(VectorSchemaRoot src, DictionaryProvider source) {
        int rows = src.getRowCount();
        List<FieldVector> owned = new ArrayList<>();
        List<FieldVector> columns = new ArrayList<>(src.getFieldVectors().size());
        for (FieldVector sv : src.getFieldVectors()) {
            TransferPair tp = sv.getTransferPair(Allocators.root());
            tp.splitAndTransfer(0, rows);
            FieldVector copied = (FieldVector) tp.getTo();
            columns.add(copied);
            owned.add(copied);
        }
        VectorSchemaRoot dst = new VectorSchemaRoot(columns);
        dst.setRowCount(rows);

        DictionaryProvider.MapDictionaryProvider dictionaries = null;
        if (source != null) {
            dictionaries = copyDictionaries(src.getSchema().getFields(), source, owned, null);
        }
        return new Copy(dst, dictionaries, owned);
    }

    /**
     * Copy every dictionary the given fields refer to, at any depth.
     *
     * <p>Recursive, and that is the point: a dictionary-encoded column can sit
     * inside a list or a struct — {@code List<item: int16[dictionary: 0]>} is an
     * encoded enum array — and a top-level-only scan finds nothing for it while
     * the schema still declares the encoding. The batch is then written against
     * a provider that has no entry for the id it names, which fails as "Could
     * not find dictionary with ID 0" at the writer rather than at the fetch.
     *
     * @param fields the fields to walk
     * @param source the provider owning the originals
     * @param owned collects every copied vector for the caller to release
     * @param into the provider being built, or {@code null} to create one lazily
     * @return the provider, or {@code null} when no field was encoded
     */
    private static DictionaryProvider.MapDictionaryProvider copyDictionaries(
            List<Field> fields, DictionaryProvider source, List<FieldVector> owned,
            DictionaryProvider.MapDictionaryProvider into) {
        for (Field f : fields) {
            DictionaryEncoding enc = f.getDictionary();
            if (enc != null) {
                Dictionary d = source.lookup(enc.getId());
                if (d != null) {
                    TransferPair tp = d.getVector().getTransferPair(Allocators.root());
                    tp.splitAndTransfer(0, d.getVector().getValueCount());
                    FieldVector copiedDict = (FieldVector) tp.getTo();
                    owned.add(copiedDict);
                    if (into == null) into = new DictionaryProvider.MapDictionaryProvider();
                    into.put(new Dictionary(copiedDict, enc));
                }
            }
            if (!f.getChildren().isEmpty()) {
                into = copyDictionaries(f.getChildren(), source, owned, into);
            }
        }
        return into;
    }

    /**
     * The outcome of resolving an external-location pointer batch: the fetched,
     * materialised batch, the metadata that should accompany it downstream, and
     * the dictionaries its encoded columns refer to.
     *
     * <p>Everything here is caller-owned. {@link #close()} releases the batch
     * <em>and</em> the dictionary copies; closing only {@link #root()} leaks the
     * latter, which is why every call site uses {@code close()}.
     *
     * @param root the fetched batch
     * @param customMetadata the pointer's metadata minus the location keys,
     *     overlaid with the externalized batch's own — what the batch would have
     *     carried had it been sent inline
     * @param dictionaries dictionaries for encoded columns, or {@code null}
     * @param owned every vector this result allocated, closed by {@link #close()}
     */
    public record Resolved(VectorSchemaRoot root, Map<String, String> customMetadata,
                           DictionaryProvider dictionaries, List<FieldVector> owned)
            implements AutoCloseable {

        @Override
        public void close() {
            try {
                root.close();
            } finally {
                if (owned != null) {
                    for (FieldVector v : owned) {
                        try {
                            v.close();
                        } catch (RuntimeException ignore) {
                            // Best-effort: one vector failing to release must not
                            // strand the rest.
                        }
                    }
                }
            }
        }
    }
}
