// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.iroh;

import static org.junit.jupiter.api.Assertions.assertTrue;

import farm.query.vgirpc.transport.IrohTransportProvider;
import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;

final class OfficialIrohProviderPackagingTest {
    @Test void serviceLoaderFindsOfficialProvider() {
        assertTrue(ServiceLoader.load(IrohTransportProvider.class).stream()
                .anyMatch(provider -> provider.type().equals(OfficialIrohTransportProvider.class)));
    }
}
