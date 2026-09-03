// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.iroh;

/** Typed Java contract implemented by the hosted Python Iroh integration worker. */
interface IrohHttpIntegrationService {
    String echo(String value);
    String identity();
}
