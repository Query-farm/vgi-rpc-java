// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.wire;

/**
 * Well-known custom metadata keys for the vgi-rpc wire protocol.
 * Mirrors {@code vgi_rpc/metadata.py}; these keys appear verbatim on the wire.
 */
public final class Metadata {
    private Metadata() {}

    public static final String RPC_METHOD          = "vgi_rpc.method";
    public static final String STREAM_STATE        = "vgi_rpc.stream_state#b64";
    public static final String CANCEL              = "vgi_rpc.cancel";
    public static final String LOG_LEVEL           = "vgi_rpc.log_level";
    public static final String LOG_MESSAGE         = "vgi_rpc.log_message";
    public static final String LOG_EXTRA           = "vgi_rpc.log_extra";
    public static final String REQUEST_VERSION_KEY = "vgi_rpc.request_version";
    public static final String REQUEST_VERSION     = "1";

    public static final String SERVER_ID  = "vgi_rpc.server_id";
    public static final String REQUEST_ID = "vgi_rpc.request_id";

    public static final String LOCATION       = "vgi_rpc.location";
    public static final String LOCATION_SHA256 = "vgi_rpc.location.sha256";
    public static final String LOCATION_FETCH = "vgi_rpc.location.fetch_ms";
    public static final String LOCATION_SOURCE = "vgi_rpc.location.source";

    public static final String SHM_OFFSET        = "vgi_rpc.shm_offset";
    public static final String SHM_LENGTH        = "vgi_rpc.shm_length";
    public static final String SHM_SOURCE        = "vgi_rpc.shm_source";
    public static final String SHM_SEGMENT_NAME  = "vgi_rpc.shm_segment_name";
    public static final String SHM_SEGMENT_SIZE  = "vgi_rpc.shm_segment_size";

    public static final String PROTOCOL_NAME     = "vgi_rpc.protocol_name";
    public static final String DESCRIBE_VERSION_KEY = "vgi_rpc.describe_version";
    public static final String PROTOCOL_HASH_KEY = "vgi_rpc.protocol_hash";
    public static final String PROTOCOL_VERSION_KEY = "vgi_rpc.protocol_version";

    public static final String TRACEPARENT = "traceparent";
    public static final String TRACESTATE  = "tracestate";

    /** Top-level batch metadata key carrying a stable error category
     *  (e.g. "session_lost", "server_draining", "method_not_implemented")
     *  so clients can pattern-match without substring-searching message text. */
    public static final String ERROR_KIND = "vgi_rpc.error_kind";
}
