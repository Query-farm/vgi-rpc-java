// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.transport;

import java.time.Duration;
import java.util.List;

/** Native-Iroh client options shared by binding providers. */
public record IrohTransportOptions(byte[] secretKey, List<String> relayUrls, boolean noRelay,
                                   Duration connectTimeout, Duration ioTimeout,
                                   String remoteRelayUrl, List<String> directAddresses) {
    public IrohTransportOptions {
        secretKey = secretKey == null ? null : secretKey.clone();
        relayUrls = relayUrls == null ? List.of() : List.copyOf(relayUrls);
        directAddresses = directAddresses == null ? List.of() : List.copyOf(directAddresses);
        if (secretKey != null && secretKey.length != 32) {
            throw new IrohConfigurationException("Iroh secret key must contain exactly 32 bytes");
        }
        if (noRelay && !relayUrls.isEmpty()) {
            throw new IrohConfigurationException("noRelay and relayUrls are mutually exclusive");
        }
        if (connectTimeout == null || connectTimeout.isNegative() || connectTimeout.isZero()) {
            throw new IrohConfigurationException("connectTimeout must be positive");
        }
        if (ioTimeout == null || ioTimeout.isNegative() || ioTimeout.isZero()) {
            throw new IrohConfigurationException("ioTimeout must be positive");
        }
    }

    /** Source-compatible constructor from before remote address hints were exposed. */
    public IrohTransportOptions(byte[] secretKey, List<String> relayUrls, boolean noRelay,
                                Duration connectTimeout, Duration ioTimeout) {
        this(secretKey, relayUrls, noRelay, connectTimeout, ioTimeout, null, List.of());
    }

    @Override public byte[] secretKey() { return secretKey == null ? null : secretKey.clone(); }

    public static IrohTransportOptions defaults() {
        return new IrohTransportOptions(null, List.of(), false,
                Duration.ofSeconds(30), Duration.ofMinutes(5), null, List.of());
    }
}
