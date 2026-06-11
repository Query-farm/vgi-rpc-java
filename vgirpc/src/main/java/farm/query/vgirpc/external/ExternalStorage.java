// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.external;

import java.net.URI;

/**
 * Pluggable uploader for externalised Arrow IPC blobs. An implementation
 * writes {@code body} bytes to object storage and returns a URI the peer
 * can fetch (typically a short-lived pre-signed URL). The contents are
 * treated as opaque IPC bytes by the framework.
 *
 * <p>Implementations need not support deletion — the Python reference
 * documents bucket-side lifecycle policies for cleanup.</p>
 */
public interface ExternalStorage {

    /**
     * Upload the given IPC bytes.
     *
     * @param body full IPC stream bytes
     * @param contentEncoding {@code "zstd"} or {@code null}
     * @return URI the peer should GET to retrieve the bytes
     * @throws Exception if the upload fails; the error propagates out of the
     *         externaliser and surfaces as an RPC error on the calling side
     */
    URI upload(byte[] body, String contentEncoding) throws Exception;
}
