// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.errors;

import java.util.List;
import java.util.Map;

/**
 * An exception that carries typed details ({@code vgi_rpc.error_details}).
 *
 * <p>Each element is the JSON object form of one detail -- usually built with
 * {@link ErrorDetail#toJson()}, or by hand for a protocol-defined type named under the protocol's
 * own name. The server applies the catalog rules and the 4 KiB cap when it writes the batch; an
 * array that breaks them is omitted whole, never trimmed.
 */
public interface HasErrorDetails {
    /**
     * The detail objects, in the order they should appear on the wire.
     *
     * @return the details; empty when there are none, never {@code null}
     */
    List<Map<String, Object>> errorDetails();
}
