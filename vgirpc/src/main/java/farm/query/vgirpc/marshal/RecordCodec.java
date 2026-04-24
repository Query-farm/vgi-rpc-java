// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.marshal;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.SchemaDerivation;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import farm.query.vgirpc.wire.IpcStreamWriter;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Convert {@link ArrowSerializableRecord}s ↔ row dicts and IPC bytes. */
public final class RecordCodec {

    private RecordCodec() {}

    /** Recursively convert a record into a Map suitable for Arrow struct insertion. */
    public static Map<String, Object> toRowMap(ArrowSerializableRecord record) {
        Map<String, Object> row = new LinkedHashMap<>();
        Class<?> cls = record.getClass();
        for (RecordComponent rc : cls.getRecordComponents()) {
            Object value;
            try {
                value = rc.getAccessor().invoke(record);
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException("failed to read record component " + rc.getName(), e);
            }
            row.put(rc.getName(), convertForArrow(value));
        }
        return row;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object convertForArrow(Object v) {
        if (v == null) return null;
        if (v instanceof Optional<?> o) return convertForArrow(o.orElse(null));
        if (v instanceof Enum<?> e) return e.name();
        if (v instanceof ArrowSerializableRecord r) return toRowMap(r);
        if (v instanceof List<?> list) {
            List<Object> out = new ArrayList<>(list.size());
            for (Object x : list) out.add(convertForArrow(x));
            return out;
        }
        if (v instanceof Map<?, ?> map) {
            Map<Object, Object> out = new LinkedHashMap<>(map.size());
            for (Map.Entry e : map.entrySet()) {
                out.put(convertForArrow(e.getKey()), convertForArrow(e.getValue()));
            }
            return out;
        }
        return v;
    }

    /** Serialize a record to an Arrow IPC bytes blob (used for {@code @ArrowField(BINARY)} fields). */
    public static byte[] serializeToBytes(ArrowSerializableRecord record) {
        Schema schema = SchemaDerivation.schemaForRecord(record.getClass());
        Map<String, Object> rowDict = toRowMap(record);
        try (VectorSchemaRoot root = Marshalling.encodeRow(schema, rowDict, Allocators.root());
             ByteArrayOutputStream bos = new ByteArrayOutputStream();
             IpcStreamWriter w = new IpcStreamWriter(bos)) {
            w.writeBatch(root, null);
            w.writeEos();
            return bos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("record serialize failed", e);
        }
    }

    /** Deserialize a record from IPC bytes. */
    public static <R extends ArrowSerializableRecord> R deserializeFromBytes(byte[] data, Class<R> cls) {
        try (IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(data), Allocators.root())) {
            r.readNextBatch();
            VectorSchemaRoot root = r.root();
            Map<String, Object> row = Marshalling.decodeRow(root);
            return fromRowMap(cls, row);
        } catch (Exception e) {
            throw new RuntimeException("record deserialize failed", e);
        }
    }

    /** Reconstruct a record from a value map (handles enums, nested records, lists, maps, optionals). */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <R extends ArrowSerializableRecord> R fromRowMap(Class<R> cls, Map<String, Object> row) {
        RecordComponent[] comps = cls.getRecordComponents();
        Object[] args = new Object[comps.length];
        Class<?>[] paramTypes = new Class<?>[comps.length];
        for (int i = 0; i < comps.length; i++) {
            RecordComponent rc = comps[i];
            Class<?> ptype = rc.getType();
            paramTypes[i] = ptype;
            Object raw = row.get(rc.getName());
            args[i] = convertForJava(raw, ptype, rc);
        }
        try {
            Constructor<R> ctor = cls.getDeclaredConstructor(paramTypes);
            ctor.setAccessible(true);
            return ctor.newInstance(args);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("record construction failed for " + cls.getName(), e);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object convertForJava(Object raw, Class<?> ptype, RecordComponent rc) {
        if (raw == null) {
            if (ptype == Optional.class) return Optional.empty();
            return null;
        }
        if (ptype == Optional.class) {
            return Optional.ofNullable(convertElement(raw, ptype, rc.getGenericType()));
        }
        if (ptype.isEnum()) {
            return Enum.valueOf((Class<Enum>) ptype.asSubclass(Enum.class), (String) raw);
        }
        if (ArrowSerializableRecord.class.isAssignableFrom(ptype) && raw instanceof Map<?, ?> m) {
            return fromRowMap((Class<? extends ArrowSerializableRecord>) ptype, (Map<String, Object>) m);
        }
        if (ArrowSerializableRecord.class.isAssignableFrom(ptype) && raw instanceof byte[] bytes) {
            return deserializeFromBytes(bytes, (Class<? extends ArrowSerializableRecord>) ptype);
        }
        if (ptype == byte[].class) return raw;
        if (ptype == int.class || ptype == Integer.class) return (int) ((Number) raw).longValue();
        if (ptype == long.class || ptype == Long.class) return ((Number) raw).longValue();
        if (ptype == double.class || ptype == Double.class) return ((Number) raw).doubleValue();
        if (ptype == float.class || ptype == Float.class) return ((Number) raw).floatValue();
        if (ptype == boolean.class || ptype == Boolean.class) return raw;
        if (ptype == String.class) return raw;
        if (List.class.isAssignableFrom(ptype) && raw instanceof List<?> list) {
            // Convert nested elements based on generic component type
            java.lang.reflect.ParameterizedType pt = (java.lang.reflect.ParameterizedType) rc.getGenericType();
            java.lang.reflect.Type elemType = pt.getActualTypeArguments()[0];
            List<Object> out = new ArrayList<>(list.size());
            for (Object x : list) out.add(convertElement(x, rawClass(elemType), elemType));
            return out;
        }
        if (Map.class.isAssignableFrom(ptype) && raw instanceof Map<?, ?> map) {
            java.lang.reflect.ParameterizedType pt = (java.lang.reflect.ParameterizedType) rc.getGenericType();
            java.lang.reflect.Type kT = pt.getActualTypeArguments()[0];
            java.lang.reflect.Type vT = pt.getActualTypeArguments()[1];
            Map<Object, Object> out = new LinkedHashMap<>(map.size());
            for (Map.Entry<?, ?> e : map.entrySet()) {
                out.put(convertElement(e.getKey(), rawClass(kT), kT),
                        convertElement(e.getValue(), rawClass(vT), vT));
            }
            return out;
        }
        return raw;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object convertElement(Object raw, Class<?> ptype, java.lang.reflect.Type genericType) {
        if (raw == null) return null;
        if (ptype == null) return raw;
        if (ptype.isEnum()) return Enum.valueOf((Class<Enum>) ptype.asSubclass(Enum.class), (String) raw);
        if (ArrowSerializableRecord.class.isAssignableFrom(ptype) && raw instanceof Map<?, ?> m) {
            return fromRowMap((Class<? extends ArrowSerializableRecord>) ptype, (Map<String, Object>) m);
        }
        if (ptype == int.class || ptype == Integer.class) return (int) ((Number) raw).longValue();
        if (ptype == long.class || ptype == Long.class) return ((Number) raw).longValue();
        if (ptype == double.class || ptype == Double.class) return ((Number) raw).doubleValue();
        if (ptype == float.class || ptype == Float.class) return ((Number) raw).floatValue();
        if (List.class.isAssignableFrom(ptype) && raw instanceof List<?> list && genericType instanceof java.lang.reflect.ParameterizedType pt) {
            java.lang.reflect.Type elemType = pt.getActualTypeArguments()[0];
            List<Object> out = new ArrayList<>(list.size());
            for (Object x : list) out.add(convertElement(x, rawClass(elemType), elemType));
            return out;
        }
        return raw;
    }

    private static Class<?> rawClass(java.lang.reflect.Type t) {
        if (t instanceof Class<?> c) return c;
        if (t instanceof java.lang.reflect.ParameterizedType pt) return (Class<?>) pt.getRawType();
        return null;
    }
}
