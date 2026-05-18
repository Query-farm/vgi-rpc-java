// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper;
import farm.query.vgirpc.PortableStreamState;
import farm.query.vgirpc.StreamState;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Reflection-based CBOR serialisation for {@link StreamState} subclasses.
 *
 * <p>State is serialised as a CBOR object containing every non-static,
 * non-transient instance field. Subclasses must expose a public no-arg
 * constructor (either directly or on their enclosing class for anonymous
 * inner classes). The state blob is opaque to clients — only this JVM
 * reads its own blobs back out — and CBOR (RFC 8949) buys us native
 * {@code byte[]} encoding (no base64 inflation for IPC schemas, filter
 * bytes, join keys) and ~30–50% smaller tokens than JSON, while keeping
 * Jackson's familiar tree/databind API and the same data model.
 *
 * <p>State classes whose fields are not naturally Jackson-friendly — Arrow
 * batches, complex binary structures — should implement
 * {@link PortableStreamState} to take over their own encoding.</p>
 */
public final class StateSerializer {

    private static final ObjectMapper CBOR = new CBORMapper();

    private StateSerializer() {}

    public static byte[] serialize(StreamState state) {
        if (state instanceof PortableStreamState pss) {
            try {
                return pss.encode();
            } catch (Exception e) {
                throw new RuntimeException("state encode failed: " + state.getClass().getName(), e);
            }
        }
        try {
            ObjectNode root = CBOR.createObjectNode();
            for (Field f : declaredFields(state.getClass())) {
                if (!f.canAccess(state)) f.setAccessible(true);
                Object value = f.get(state);
                root.set(f.getName(), CBOR.valueToTree(value));
            }
            return CBOR.writeValueAsBytes(root);
        } catch (Exception e) {
            throw new RuntimeException("state serialize failed: " + state.getClass().getName(), e);
        }
    }

    public static <S extends StreamState> S deserialize(byte[] data, Class<S> cls) {
        if (PortableStreamState.class.isAssignableFrom(cls)) {
            try {
                S instance = newInstance(cls);
                ((PortableStreamState) instance).decode(data);
                return instance;
            } catch (Exception e) {
                throw new RuntimeException("state decode failed: " + cls.getName(), e);
            }
        }
        try {
            JsonNode root = CBOR.readTree(data);
            S instance = newInstance(cls);
            for (Field f : declaredFields(cls)) {
                if (!f.canAccess(instance)) f.setAccessible(true);
                JsonNode node = root.get(f.getName());
                if (node == null || node.isNull()) continue;
                Class<?> t = f.getType();
                if      (t == int.class    || t == Integer.class) f.setInt(instance, node.asInt());
                else if (t == long.class   || t == Long.class)    f.setLong(instance, node.asLong());
                else if (t == double.class || t == Double.class)  f.setDouble(instance, node.asDouble());
                else if (t == float.class  || t == Float.class)   f.setFloat(instance, (float) node.asDouble());
                else if (t == boolean.class|| t == Boolean.class) f.setBoolean(instance, node.asBoolean());
                else if (t == byte.class   || t == Byte.class)    f.setByte(instance, (byte) node.asInt());
                else if (t == short.class  || t == Short.class)   f.setShort(instance, (short) node.asInt());
                else if (t == String.class)                       f.set(instance, node.asText());
                // Generic types (List, Map, custom records, byte[]) round-trip via Jackson's
                // CBOR databind. Use the field's generic type (List<Long>, Map<String,Double>, ...)
                // so collection values come back with their declared component types, not raw nodes.
                else f.set(instance, CBOR.convertValue(node,
                        CBOR.getTypeFactory().constructType(f.getGenericType())));
            }
            return instance;
        } catch (Exception e) {
            throw new RuntimeException("state deserialize failed: " + cls.getName(), e);
        }
    }

    private static List<Field> declaredFields(Class<?> cls) {
        List<Field> result = new ArrayList<>();
        for (Class<?> c = cls; c != null && c != Object.class; c = c.getSuperclass()) {
            for (Field f : c.getDeclaredFields()) {
                int m = f.getModifiers();
                if (Modifier.isStatic(m) || Modifier.isTransient(m)) continue;
                if (f.isSynthetic()) continue;  // inner-class synthetic $enclosing refs
                result.add(f);
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static <S extends StreamState> S newInstance(Class<S> cls) throws Exception {
        Constructor<?> ctor;
        try {
            ctor = cls.getDeclaredConstructor();
        } catch (NoSuchMethodException e) {
            // Try any constructor — must fill in defaults via reflection afterwards.
            Constructor<?>[] all = cls.getDeclaredConstructors();
            if (all.length == 0) throw e;
            ctor = all[0];
            Object[] defaults = new Object[ctor.getParameterCount()];
            Class<?>[] ptypes = ctor.getParameterTypes();
            for (int i = 0; i < ptypes.length; i++) defaults[i] = defaultValue(ptypes[i]);
            if (!ctor.canAccess(null)) ctor.setAccessible(true);
            return (S) ctor.newInstance(defaults);
        }
        if (!ctor.canAccess(null)) ctor.setAccessible(true);
        return (S) ctor.newInstance();
    }

    private static Object defaultValue(Class<?> t) {
        if (!t.isPrimitive()) return null;
        if (t == boolean.class) return false;
        if (t == byte.class) return (byte) 0;
        if (t == short.class) return (short) 0;
        if (t == int.class) return 0;
        if (t == long.class) return 0L;
        if (t == float.class) return 0f;
        if (t == double.class) return 0d;
        if (t == char.class) return (char) 0;
        return null;
    }

    // Convenience: base64 helpers so callers can round-trip bytes in textual metadata.
    public static String base64Encode(byte[] bytes) { return Base64.getEncoder().encodeToString(bytes); }
    public static byte[] base64Decode(String s) { return Base64.getDecoder().decode(s); }
}
