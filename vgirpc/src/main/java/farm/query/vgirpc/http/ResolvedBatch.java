// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.wire.IpcStreamReader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;

import java.util.Map;

/**
 * An externalized batch, fetched and opened: the real batch a
 * {@code vgi_rpc.location} pointer stood in for, plus everything its validity
 * depends on.
 *
 * <p>The dictionaries are the reason this is a record rather than a bare root.
 * A dictionary-encoded column is stored as indices into a dictionary that
 * travels beside the batch, never inside it, so a root handed on without its
 * provider is not a batch that has lost an optimisation — it is a batch that
 * cannot be written or read at all ("Could not find dictionary with ID 0"). The
 * failure lands on whoever touches the batch next, a layer away from the fetch
 * that dropped it.</p>
 *
 * <p>Ownership travels with it: the reader still owns the vectors, so the batch
 * is valid until {@link #close()} and the holder must close it exactly once.
 * Keeping the reader open is deliberate — copying the root out would leave the
 * dictionaries behind, which is the bug above.</p>
 *
 * @param root the fetched batch's vectors
 * @param customMetadata the metadata the batch travels with downstream: the
 *     pointer's own, minus the location keys, overlaid with the externalized
 *     batch's — what it would have carried had it been sent inline
 * @param dictionaries dictionaries for encoded fields, or {@code null}
 * @param owner the reader over the fetched stream, closed by {@link #close()}
 */
record ResolvedBatch(VectorSchemaRoot root, Map<String, String> customMetadata,
                     DictionaryProvider dictionaries, IpcStreamReader owner)
        implements AutoCloseable {

    @Override
    public void close() {
        try {
            owner.close();
        } catch (Exception ignore) {
            // Backed by a byte array; nothing that can fail meaningfully, and a
            // release failure must not mask what the caller was doing.
        }
    }
}
