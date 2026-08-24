// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.conformance;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Recursive enum/dataclass container fixture for conformance protocol 2.0. */
public record NestedContainers(
        List<Status> statuses,
        List<Point> points,
        Map<String, Status> status_by_name,
        List<Status> frozen_statuses,
        Optional<Status> tagged_status,
        Optional<Point> tagged_point,
        Optional<byte[]> tagged_batch
) implements ArrowSerializableRecord {}
