// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.conformance;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

public record BoundingBox(Point top_left, Point bottom_right, String label)
        implements ArrowSerializableRecord {}
