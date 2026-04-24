// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;

/**
 * Minimal OIDC discovery result — fetches
 * {@code {issuer}/.well-known/openid-configuration} and extracts the endpoint
 * URLs the PKCE flow needs. Immutable; callers should cache.
 */
public record OidcMetadata(
        String issuer,
        URI authorizationEndpoint,
        URI tokenEndpoint,
        URI jwksUri) {

    private static final ObjectMapper JSON = new ObjectMapper();

    /** Fetch and parse the OIDC discovery document. Throws on HTTP / parse errors. */
    public static OidcMetadata discover(String issuer) throws IOException, InterruptedException {
        Objects.requireNonNull(issuer, "issuer");
        String url = issuer.replaceAll("/+$", "") + "/.well-known/openid-configuration";
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        HttpResponse<String> resp = client.send(
                HttpRequest.newBuilder(URI.create(url)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() / 100 != 2) {
            throw new IOException("OIDC discovery failed: HTTP " + resp.statusCode() + " from " + url);
        }
        JsonNode root = JSON.readTree(resp.body());
        URI authz = required(root, "authorization_endpoint", url);
        URI token = required(root, "token_endpoint", url);
        URI jwks = required(root, "jwks_uri", url);
        return new OidcMetadata(issuer, authz, token, jwks);
    }

    private static URI required(JsonNode root, String field, String docUrl) throws IOException {
        JsonNode v = root.get(field);
        if (v == null || !v.isTextual()) {
            throw new IOException("OIDC discovery at " + docUrl + " is missing '" + field + "'");
        }
        return URI.create(v.asText());
    }
}
