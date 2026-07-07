// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

/**
 * Produces the JSON documents for a VGI worker's standardized HTTP landing
 * surface (see {@code ~/Development/vgi/docs/http-landing-contract.md}).
 *
 * <p>{@link HttpServer} owns the generic routing and the shared static
 * {@code landing.html}; the actual describe contents depend on the worker's
 * catalog model, which lives above the RPC layer. A worker (e.g. {@code
 * farm.query.vgi.Worker}) injects an implementation via
 * {@link HttpServer.Config.Builder#describeProvider} so {@code HttpServer} can
 * answer {@code GET {prefix}/describe.json} and {@code GET
 * {prefix}/describe/{catalog}/{schema}/{table}.json} without depending on the
 * VGI worker library.</p>
 *
 * <p>Both methods return a fully-serialized JSON string (UTF-8 written verbatim
 * with {@code Content-Type: application/json}).</p>
 */
public interface DescribeProvider {

    /**
     * Build the versioned {@code describe.json} contract document.
     *
     * @param serverId    the opaque per-process server id (volatile), surfaced as
     *                    {@code server_id}
     * @param oauthActive whether OAuth/PKCE (or another interactive auth) is active
     *                    on this worker, surfaced as {@code oauth}
     * @return the describe document as a JSON string
     */
    String describeJson(String serverId, boolean oauthActive);

    /**
     * Build the lazy per-object column payload for one table or view.
     *
     * @param catalog the catalog name
     * @param schema  the schema name
     * @param table   the table or view name
     * @return {@code {"columns": [...]}} as a JSON string, or {@code null} when the
     *         object cannot be found (the caller responds HTTP 404)
     */
    String columnsJson(String catalog, String schema, String table);
}
