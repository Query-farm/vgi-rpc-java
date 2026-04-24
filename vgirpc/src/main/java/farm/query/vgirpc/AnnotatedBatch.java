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

    public AnnotatedBatch(VectorSchemaRoot root, Map<String, String> customMetadata) {
        this(root, customMetadata, null);
    }

    public AnnotatedBatch(VectorSchemaRoot root, Map<String, String> customMetadata, Runnable releaseFn) {
        this.root = root;
        this.customMetadata = customMetadata != null ? customMetadata : Collections.emptyMap();
        this.releaseFn = releaseFn;
    }

    public VectorSchemaRoot root() { return root; }
    public Map<String, String> customMetadata() { return customMetadata; }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        if (releaseFn != null) releaseFn.run();
        if (root != null) root.close();
    }
}
