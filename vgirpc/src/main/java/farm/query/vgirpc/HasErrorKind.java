// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

/**
 * Marker interface for exception types that carry a stable error
 * category surfaced via the {@code vgi_rpc.error_kind} batch metadata
 * key. Clients pattern-match on this instead of substring-searching the
 * message text.
 */
public interface HasErrorKind {
    /**
     * Stable, machine-readable error category for this exception. The
     * server emits it under the {@code vgi_rpc.error_kind} metadata key on
     * the error batch; the client hoists it back into the reconstructed
     * {@link RpcError#errorKind()} so callers can branch on it.
     *
     * @return the stable error category (e.g. {@code "session_lost"}), or
     *         {@code null} when no category applies
     */
    String errorKind();
}
