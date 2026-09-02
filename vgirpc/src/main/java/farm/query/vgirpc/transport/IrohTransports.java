// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.transport;

import java.io.IOException;
import java.util.ServiceLoader;

/**
 * Entry point for optional native-Iroh raw Arrow-mux transport providers.
 * {@code httpi://} is parsed for the shared contract but is explicitly
 * unsupported until this SDK ships an {@code iroh-http/2} codec.
 */
public final class IrohTransports {
    private IrohTransports() {}

    public static RpcTransport connect(String rawEndpoint, IrohTransportOptions options)
            throws IOException {
        IrohTransportProvider provider = ServiceLoader.load(IrohTransportProvider.class)
                .findFirst().orElseThrow(() -> new IrohTransportException(
                        "iroh:// requires the optional official Kotlin/JVM Iroh transport provider",
                        IrohErrorStage.BIND, IrohErrorCategory.UNSUPPORTED,
                        IrohDispatchCertainty.NOT_SENT));
        return connect(rawEndpoint, options, provider);
    }

    public static RpcTransport connect(String rawEndpoint, IrohTransportOptions options,
                                       IrohTransportProvider provider) throws IOException {
        IrohEndpoint endpoint = IrohEndpoint.parse(rawEndpoint);
        if (endpoint.scheme() != IrohEndpoint.Scheme.IROH) {
            throw new IrohTransportException(
                    "raw RpcTransport requires iroh://; httpi:// requires an iroh-http/2 client",
                    IrohErrorStage.BIND, IrohErrorCategory.UNSUPPORTED,
                    IrohDispatchCertainty.NOT_SENT);
        }
        if (provider == null) throw new NullPointerException("provider");
        return provider.openArrowMux(endpoint,
                options == null ? IrohTransportOptions.defaults() : options);
    }
}
