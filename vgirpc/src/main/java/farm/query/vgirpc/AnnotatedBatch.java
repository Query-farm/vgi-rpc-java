// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.Collections;
import java.util.Map;

/**
 * A {@link VectorSchemaRoot} paired with its custom metadata from the Arrow IPC
 * batch envelope. Used as the input/output unit for streaming methods.
 */
public final class AnnotatedBatch implements AutoCloseable {

    private final VectorSchemaRoot root;
    private final Map<String, String> customMetadata;
    private final Runnable releaseFn;
    private boolean closed;

    /**
     * Wrap a batch and its metadata with no extra release action.
     *
     * @param root the batch vectors (closed by {@link #close()})
     * @param customMetadata batch custom metadata; {@code null} becomes empty
     */
    public AnnotatedBatch(VectorSchemaRoot root, Map<String, String> customMetadata) {
        this(root, customMetadata, null);
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
        this.root = root;
        this.customMetadata = customMetadata != null ? customMetadata : Collections.emptyMap();
        this.releaseFn = releaseFn;
    }

    /** The batch's vectors. */
    public VectorSchemaRoot root() { return root; }
    /** The batch's Arrow IPC custom metadata (never {@code null}). */
    public Map<String, String> customMetadata() { return customMetadata; }

    /** Run the release function (if any) and close the root. Idempotent. */
    @Override
    public void close() {
        if (closed) return;
        closed = true;
        if (releaseFn != null) releaseFn.run();
        if (root != null) root.close();
    }
}
