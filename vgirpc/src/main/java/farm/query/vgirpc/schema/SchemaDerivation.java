// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.schema;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Derives Arrow schemas from Java types: records, primitives, generic
 * containers ({@code List}, {@code Map}), {@code Optional}, enums, and nested
 * {@link ArrowSerializableRecord}s.
 *
 * <p>This mirrors {@code _infer_arrow_type} in the Python reference. Mapping:
 * <ul>
 *   <li>{@code String} → utf8</li>
 *   <li>{@code byte[]} → binary</li>
 *   <li>{@code long}, {@code Long}, {@code int}, {@code Integer} → int64</li>
 *   <li>{@code double}, {@code Double}, {@code float}, {@code Float} → float64</li>
 *   <li>{@code boolean}, {@code Boolean} → bool</li>
 *   <li>{@code List<T>} → list&lt;T&gt;</li>
 *   <li>{@code Map<K,V>} → map&lt;K,V&gt;</li>
 *   <li>{@code Optional<T>} or {@code @Nullable T} → nullable {@code T}</li>
 *   <li>{@code Enum} → dictionary&lt;int16, utf8&gt;</li>
 *   <li>{@code ArrowSerializableRecord} as record component → struct</li>
 *   <li>{@code @ArrowField(INT32)} → explicit override (e.g. int32)</li>
 * </ul>
 */
public final class SchemaDerivation {

    private SchemaDerivation() {}

    private static final Map<Class<?>, Schema> RECORD_SCHEMA_CACHE = new ConcurrentHashMap<>();

    /**
     * Derive an Arrow schema for an {@link ArrowSerializableRecord} class by walking
     * its record components. Cached. Uses {@code putIfAbsent} rather than
     * {@code computeIfAbsent} so recursive derivations (a record with a nested
     * record component) don't trip {@link java.util.concurrent.ConcurrentHashMap}'s
     * recursive-update detector.
     */
    public static Schema schemaForRecord(Class<? extends ArrowSerializableRecord> cls) {
        Schema cached = RECORD_SCHEMA_CACHE.get(cls);
        if (cached != null) return cached;
        Schema built = buildRecordSchema(cls);
        Schema existing = RECORD_SCHEMA_CACHE.putIfAbsent(cls, built);
        return existing != null ? existing : built;
    }

    private static Schema buildRecordSchema(Class<?> cls) {
        if (!cls.isRecord()) {
            throw new IllegalArgumentException(cls + " is not a record");
        }
        List<Field> fields = new ArrayList<>();
        for (RecordComponent rc : cls.getRecordComponents()) {
            fields.add(buildField(rc.getName(), rc.getGenericType(), rc));
        }
        return new Schema(fields);
    }

    /** Build a Field for a parameter (uses parameter name from -parameters compile flag). */
    public static Field buildFieldForParameter(Parameter param) {
        return buildTopLevelField(param.getName(), param.getParameterizedType(), param);
    }

    /** Build a Field for the (single) result column of a unary method, named "result". */
    public static Field buildResultField(Method m) {
        Type returnType = m.getGenericReturnType();
        return buildTopLevelField("result", returnType, m);
    }

    /**
     * Build a Field for a top-level (parameter or unary-result) Java type. At the top
     * level, an {@link ArrowSerializableRecord} is sent as IPC binary bytes — the
     * record is serialised whole. Inside a struct/list/etc. it remains a struct.
     */
    private static Field buildTopLevelField(String name, Type type, AnnotatedElement annSource) {
        boolean nullable = isOptionalType(type) || annSource.isAnnotationPresent(Nullable.class);
        Type unwrapped = unwrapOptional(type);
        ArrowField override = annSource.getAnnotation(ArrowField.class);
        if (override != null) {
            return new Field(name, new FieldType(nullable, override.value().arrowType(), null),
                    Collections.emptyList());
        }
        Class<?> raw = rawClass(unwrapped);
        if (raw.isRecord() && ArrowSerializableRecord.class.isAssignableFrom(raw)) {
            return new Field(name, new FieldType(nullable, new ArrowType.Binary(), null),
                    Collections.emptyList());
        }
        return buildField(name, type, annSource);
    }

    /**
     * Build an Arrow {@link Field} for the named field of the given Java type.
     * The annotation source is the location where the type was declared (param
     * or record component), used to discover {@code @ArrowField} / {@code @Nullable}.
     */
    public static Field buildField(String name, Type type, AnnotatedElement annSource) {
        boolean nullable = isOptionalType(type) || annSource.isAnnotationPresent(Nullable.class);
        Type unwrapped = unwrapOptional(type);

        ArrowField override = annSource.getAnnotation(ArrowField.class);
        ArrowType arrowType;
        List<Field> children;
        if (override != null) {
            arrowType = override.value().arrowType();
            children = Collections.emptyList();
        } else {
            // Java declares enums as plain utf8 on the wire. The reader is tolerant
            // of clients that send dict-encoded enum columns and resolves them.
            arrowType = inferArrowType(unwrapped);
            // For records that are sent as IPC binary blobs (e.g. unary params/results,
            // not nested-in-struct), drop child fields — Binary has no children.
            if (arrowType instanceof ArrowType.Binary || arrowType instanceof ArrowType.Utf8
                    || arrowType instanceof ArrowType.Bool || arrowType instanceof ArrowType.Int
                    || arrowType instanceof ArrowType.FloatingPoint) {
                children = Collections.emptyList();
            } else {
                children = childFields(unwrapped);
            }
        }
        FieldType ft = new FieldType(nullable, arrowType, /*dictionary=*/null);
        return new Field(name, ft, children);
    }

    /** Unwrap {@code Optional<T>} to {@code T}. */
    static Type unwrapOptional(Type type) {
        if (type instanceof ParameterizedType pt && pt.getRawType() == Optional.class) {
            return pt.getActualTypeArguments()[0];
        }
        return type;
    }

    static boolean isOptionalType(Type type) {
        return type instanceof ParameterizedType pt && pt.getRawType() == Optional.class;
    }

    static Class<?> rawClass(Type t) {
        if (t instanceof Class<?> c) return c;
        if (t instanceof ParameterizedType pt) return (Class<?>) pt.getRawType();
        throw new IllegalArgumentException("Cannot derive raw class from: " + t);
    }

    public static ArrowType inferArrowType(Type type) {
        Class<?> raw = rawClass(type);

        // Records that implement ArrowSerializableRecord → struct
        if (raw.isRecord() && ArrowSerializableRecord.class.isAssignableFrom(raw)) {
            return new ArrowType.Struct();
        }

        // Enums → dictionary<int16, utf8>
        if (raw.isEnum()) {
            return new ArrowType.Utf8(); // dictionary semantics handled at vector level; on the wire we use utf8
        }

        // Generic container support
        if (type instanceof ParameterizedType pt) {
            Class<?> rawC = (Class<?>) pt.getRawType();
            if (List.class.isAssignableFrom(rawC) || Set.class.isAssignableFrom(rawC)) {
                return new ArrowType.List();
            }
            if (Map.class.isAssignableFrom(rawC)) {
                return new ArrowType.Map(/*keysSorted=*/false);
            }
        }

        if (raw == String.class) return new ArrowType.Utf8();
        if (raw == byte[].class) return new ArrowType.Binary();
        if (raw == long.class || raw == Long.class) return new ArrowType.Int(64, true);
        if (raw == int.class || raw == Integer.class) return new ArrowType.Int(64, true);
        if (raw == short.class || raw == Short.class) return new ArrowType.Int(64, true);
        if (raw == byte.class || raw == Byte.class) return new ArrowType.Int(64, true);
        if (raw == double.class || raw == Double.class) return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        if (raw == float.class || raw == Float.class) return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        if (raw == boolean.class || raw == Boolean.class) return new ArrowType.Bool();
        if (raw == void.class || raw == Void.class) return new ArrowType.Null();

        // Nested record dataclass passed by reference: serialise as binary IPC bytes
        if (ArrowSerializableRecord.class.isAssignableFrom(raw)) {
            return new ArrowType.Binary();
        }

        throw new IllegalArgumentException("Cannot infer Arrow type for: " + type);
    }

    /** Child fields for container types (list element, map key/value, struct fields). */
    static List<Field> childFields(Type type) {
        Class<?> raw = rawClass(type);

        // Struct (nested ArrowSerializableRecord directly nested in another record)
        if (raw.isRecord() && ArrowSerializableRecord.class.isAssignableFrom(raw)) {
            Schema nested = schemaForRecord(raw.asSubclass(ArrowSerializableRecord.class));
            return new ArrayList<>(nested.getFields());
        }
        if (type instanceof ParameterizedType pt) {
            Class<?> rawC = (Class<?>) pt.getRawType();
            Type[] args = pt.getActualTypeArguments();
            if (List.class.isAssignableFrom(rawC) || Set.class.isAssignableFrom(rawC)) {
                Field elem = buildField("item", args[0], NO_ANNS);
                // Lists in Arrow conventionally name the child "item" with nullable=true.
                Field nullable = new Field("item",
                        new FieldType(true, elem.getType(), null),
                        elem.getChildren());
                return List.of(nullable);
            }
            if (Map.class.isAssignableFrom(rawC)) {
                Field key = buildField("key", args[0], NO_ANNS);
                Field val = buildField("value", args[1], NO_ANNS);
                Field keyNonNull = new Field("key",
                        new FieldType(false, key.getType(), null), key.getChildren());
                Field valNullable = new Field("value",
                        new FieldType(true, val.getType(), null), val.getChildren());
                Field entries = new Field("entries",
                        new FieldType(false, new ArrowType.Struct(), null),
                        List.of(keyNonNull, valNullable));
                return List.of(entries);
            }
        }
        return Collections.emptyList();
    }

    private static final AnnotatedElement NO_ANNS = new AnnotatedElement() {
        @Override public <T extends Annotation> T getAnnotation(Class<T> a) { return null; }
        @Override public Annotation[] getAnnotations() { return new Annotation[0]; }
        @Override public Annotation[] getDeclaredAnnotations() { return new Annotation[0]; }
    };
}
