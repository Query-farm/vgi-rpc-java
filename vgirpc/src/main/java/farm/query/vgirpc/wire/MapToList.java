// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.wire;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

final class MapToList {
    private MapToList() {}

    static List<Map.Entry<String, String>> toList(Map<String, String> meta) {
        if (meta == null || meta.isEmpty()) return List.of();
        List<Map.Entry<String, String>> out = new ArrayList<>(meta.size());
        for (Map.Entry<String, String> e : meta.entrySet()) {
            out.add(new AbstractMap.SimpleImmutableEntry<>(e.getKey(), e.getValue()));
        }
        return out;
    }
}
