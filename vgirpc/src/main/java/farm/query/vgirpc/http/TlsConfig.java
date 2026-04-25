// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import java.nio.file.Path;
import java.util.Objects;

/**
 * Optional TLS settings for {@link HttpServer}. Wires up Jetty's
 * {@code SslContextFactory.Server} when present on the {@link HttpServer.Config}.
 *
 * <p>Production deployments either set a {@link TlsConfig} on the server or
 * stand up a TLS-terminating reverse proxy in front of plaintext HTTP. The
 * server itself never accepts plaintext on a non-loopback address without
 * one of those two being arranged.</p>
 *
 * @param keystorePath        path to a JKS / PKCS#12 keystore.
 * @param keystorePassword    password for the keystore.
 * @param keyManagerPassword  optional password for the private key entry; when
 *                            {@code null} Jetty falls back to the keystore password.
 */
public record TlsConfig(
        Path keystorePath,
        String keystorePassword,
        String keyManagerPassword) {

    public TlsConfig {
        Objects.requireNonNull(keystorePath, "keystorePath");
        Objects.requireNonNull(keystorePassword, "keystorePassword");
    }

    public TlsConfig(Path keystorePath, String keystorePassword) {
        this(keystorePath, keystorePassword, null);
    }
}
