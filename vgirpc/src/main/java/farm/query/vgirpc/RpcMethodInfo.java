// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import org.apache.arrow.vector.types.pojo.Schema;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * Metadata for a single RPC method, derived from a service interface via reflection.
 *
 * <p>{@link #streamKind()} is meaningful only when {@link #methodType()} is
 * {@link MethodType#STREAM}; for unary methods it is always {@link StreamKind#UNKNOWN}.
 * {@link #headerType()} is {@code null} unless the stream declares a {@code @StreamHeader}.
 */
public record RpcMethodInfo(
        String name,
        Method reflectMethod,
        Schema paramsSchema,
        Schema resultSchema,
        Type resultType,
        MethodType methodType,
        boolean hasReturn,
        String doc,
        Map<String, Type> paramTypes,
        boolean wantsCallContext,
        StreamKind streamKind,
        Class<?> headerType) {

    public RpcMethodInfo {
        paramTypes = paramTypes != null ? Map.copyOf(paramTypes) : Map.of();
        if (streamKind == null) streamKind = StreamKind.UNKNOWN;
    }
}
