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
    String errorKind();
}
