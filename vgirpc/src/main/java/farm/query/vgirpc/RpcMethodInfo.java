// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import org.apache.arrow.vector.types.pojo.Schema;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.Map;

/** Metadata for a single RPC method, derived from a service interface via reflection. */
public final class RpcMethodInfo {

    private final String name;
    private final Method reflectMethod;
    private final Schema paramsSchema;
    private final Schema resultSchema;
    private final Type resultType;
    private final MethodType methodType;
    private final boolean hasReturn;
    private final String doc;
    private final Map<String, Type> paramTypes;
    private final Map<String, Object> paramDefaults;
    private final boolean wantsCallContext;

    /** Stream sub-type: TRUE if exchange (inputs come from client), FALSE if producer, null if unknown. */
    private final Boolean isExchange;

    /** Header type for streams (null if no header). */
    private final Class<?> headerType;

    public RpcMethodInfo(String name, Method reflectMethod,
                         Schema paramsSchema, Schema resultSchema, Type resultType,
                         MethodType methodType, boolean hasReturn, String doc,
                         Map<String, Type> paramTypes, Map<String, Object> paramDefaults,
                         boolean wantsCallContext,
                         Boolean isExchange, Class<?> headerType) {
        this.name = name;
        this.reflectMethod = reflectMethod;
        this.paramsSchema = paramsSchema;
        this.resultSchema = resultSchema;
        this.resultType = resultType;
        this.methodType = methodType;
        this.hasReturn = hasReturn;
        this.doc = doc;
        this.paramTypes = paramTypes != null ? paramTypes : new LinkedHashMap<>();
        this.paramDefaults = paramDefaults != null ? paramDefaults : new LinkedHashMap<>();
        this.wantsCallContext = wantsCallContext;
        this.isExchange = isExchange;
        this.headerType = headerType;
    }

    public String name() { return name; }
    public Method reflectMethod() { return reflectMethod; }
    public Schema paramsSchema() { return paramsSchema; }
    public Schema resultSchema() { return resultSchema; }
    public Type resultType() { return resultType; }
    public MethodType methodType() { return methodType; }
    public boolean hasReturn() { return hasReturn; }
    public String doc() { return doc; }
    public Map<String, Type> paramTypes() { return paramTypes; }
    public Map<String, Object> paramDefaults() { return paramDefaults; }
    public boolean wantsCallContext() { return wantsCallContext; }
    public Boolean isExchange() { return isExchange; }
    public Class<?> headerType() { return headerType; }
}
