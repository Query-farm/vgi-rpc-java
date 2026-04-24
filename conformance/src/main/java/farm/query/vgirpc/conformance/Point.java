// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.conformance;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

public record Point(double x, double y) implements ArrowSerializableRecord {}
