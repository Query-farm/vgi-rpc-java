// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;

import java.util.Collections;
import java.util.Map;

/**
 * A {@link VectorSchemaRoot} paired with its custom metadata from the Arrow IPC
 * batch envelope. Used as the input/output unit for streaming methods.
 */
public final class AnnotatedBatch implements AutoCloseable {

    private final VectorSchemaRoot root;
    private final Map<String, String> customMetadata;
    private final DictionaryProvider dictionaryProvider;
    private final Runnable releaseFn;
    private boolean closed;

    /**
     * Wrap a batch and its metadata with no extra release action.
     *
     * @param root the batch vectors (closed by {@link #close()})
     * @param customMetadata batch custom metadata; {@code null} becomes empty
     */
    public AnnotatedBatch(VectorSchemaRoot root, Map<String, String> customMetadata) {
        this(root, customMetadata, null, null);
    }

    /**
     * Wrap a batch and its metadata with an extra release action run on close
     * (e.g. to free a backing shared-memory segment before the root is closed).
     *
     * @param root the batch vectors (closed by {@link #close()})
     * @param customMetadata batch custom metadata; {@code null} becomes empty
     * @param releaseFn extra cleanup run before {@code root.close()}, or {@code null}
     */
    public AnnotatedBatch(VectorSchemaRoot root, Map<String, String> customMetadata, Runnable releaseFn) {
        this(root, customMetadata, null, releaseFn);
    }

    /**
     * Wrap a batch together with the dictionaries referenced by its schema.
     * The provider follows the root's lifetime but remains caller-owned; this
     * wrapper does not close dictionary vectors.
     *
     * @param root the batch vectors (closed by {@link #close()})
     * @param customMetadata batch custom metadata; {@code null} becomes empty
     * @param dictionaryProvider dictionaries for encoded fields, or {@code null}
     * @param releaseFn extra cleanup run before {@code root.close()}, or {@code null}
     */
    public AnnotatedBatch(VectorSchemaRoot root, Map<String, String> customMetadata,
                          DictionaryProvider dictionaryProvider, Runnable releaseFn) {
        this.root = root;
        this.customMetadata = customMetadata != null ? customMetadata : Collections.emptyMap();
        this.dictionaryProvider = dictionaryProvider;
        this.releaseFn = releaseFn;
    }

    /**
     * The batch's vectors.
     *
     * @return the wrapped {@link VectorSchemaRoot}; remains owned by this
     *     batch and is released by {@link #close()}
     */
    public VectorSchemaRoot root() { return root; }
    /**
     * The batch's Arrow IPC custom metadata.
     *
     * @return the {@code vgi_rpc.*} metadata from the batch envelope; never
     *     {@code null}, possibly empty
     */
    public Map<String, String> customMetadata() { return customMetadata; }
    /**
     * Dictionaries referenced by the root's encoded fields. For a received
     * stream batch they remain valid only as long as the batch root does.
     *
     * @return the dictionary provider, or {@code null} for plain schemas
     */
    public DictionaryProvider dictionaryProvider() { return dictionaryProvider; }

    /** Run the release function (if any) and close the root. Idempotent. */
    @Override
    public void close() {
        if (closed) return;
        closed = true;
        if (releaseFn != null) releaseFn.run();
        if (root != null) root.close();
    }
}
