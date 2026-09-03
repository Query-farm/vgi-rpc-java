// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.transport;

import java.io.IOException;

/**
 * Optional native binding provider. The official Kotlin/JVM Iroh adapter
 * implements this SPI; core stays usable on platforms where it is not bundled.
 */
public interface IrohTransportProvider {
    /** Open one {@code vgi-rpc/arrow-mux/1} bidirectional stream. */
    RpcTransport openArrowMux(IrohEndpoint endpoint, IrohTransportOptions options) throws IOException;

    /** Open a reusable {@code iroh-http/2} request transport. */
    default IrohHttpTransport openHttp(IrohEndpoint endpoint, IrohTransportOptions options)
            throws IOException {
        throw new IrohTransportException(
                "this Iroh provider does not include the iroh-http/2 codec",
                IrohErrorStage.BIND, IrohErrorCategory.UNSUPPORTED,
                IrohDispatchCertainty.NOT_SENT);
    }

    /** Whether this provider includes the {@code iroh-http/2} codec. */
    default boolean supportsHttp() { return false; }
}
