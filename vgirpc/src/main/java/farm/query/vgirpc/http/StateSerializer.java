// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.nio.ByteBuffer;
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

    // Serialise by FIELD, not getters: nested state POJOs (e.g. BatchState)
    // expose record-style accessors (total()/index(), not getTotal()) and
    // private fields, so getter-based bean serialisation finds no properties
    // and throws. Field visibility matches the top-level reflection below and
    // round-trips any plain-POJO state component through valueToTree/treeToValue.
    private static final ObjectMapper CBOR = newMapper();

    private static ObjectMapper newMapper() {
        // Arrow Schema isn't a Jackson bean (no default ctor on Field; the
        // Field tree is recursive). Round-trip it through Arrow's own IPC
        // message encoding so state objects can hold a Schema directly.
        SimpleModule arrow = new SimpleModule();
        arrow.addSerializer(Schema.class, new JsonSerializer<Schema>() {
            @Override public void serialize(Schema s, JsonGenerator g, SerializerProvider p) throws IOException {
                g.writeBinary(s.serializeAsMessage());
            }
        });
        arrow.addDeserializer(Schema.class, new JsonDeserializer<Schema>() {
            @Override public Schema deserialize(JsonParser p, DeserializationContext c) throws IOException {
                return Schema.deserializeMessage(ByteBuffer.wrap(p.getBinaryValue()));
            }
        });
        return new CBORMapper()
                .setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE)
                .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
                .registerModule(arrow)
                // java.time types (e.g. a LocalDateTime in a state field) aren't
                // Jackson beans by default.
                .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
    }

    private StateSerializer() {}

    /**
     * Serialize stream state to CBOR. {@link PortableStreamState} instances
     * encode themselves; all others are reflected field-by-field.
     *
     * @param state the stream state to serialize
     * @return the CBOR (or {@code PortableStreamState.encode()}) bytes
     * @throws RuntimeException if encoding fails
     */
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
                root.set(fieldKey(f), CBOR.valueToTree(value));
            }
            return CBOR.writeValueAsBytes(root);
        } catch (Exception e) {
            throw new RuntimeException("state serialize failed: " + state.getClass().getName(), e);
        }
    }

    /**
     * Reconstruct stream state of type {@code cls} from bytes produced by
     * {@link #serialize(StreamState)}.
     *
     * @param data the serialized bytes
     * @param cls the concrete state type to instantiate
     * @param <S> the state type
     * @return the reconstructed state
     * @throws RuntimeException if decoding or instantiation fails
     */
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
                JsonNode node = root.get(fieldKey(f));
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

    /**
     * Unique CBOR key for a field, qualified by its declaring class. A subclass
     * may legally shadow a superclass field of the same name (e.g. a table
     * producer state that redeclares {@code outputSchema}); keying by the bare
     * field name would let the two collide in the flat CBOR object — the
     * superclass's (often null) value clobbering the subclass's — and the value
     * would round-trip back as null. Qualifying by declaring class keeps both
     * fields independent.
     */
    private static String fieldKey(Field f) {
        return f.getDeclaringClass().getName() + '#' + f.getName();
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
    /** Standard base64-encode bytes for embedding in textual metadata. */
    public static String base64Encode(byte[] bytes) { return Base64.getEncoder().encodeToString(bytes); }
    /** Standard base64-decode a string produced by {@link #base64Encode(byte[])}. */
    public static byte[] base64Decode(String s) { return Base64.getDecoder().decode(s); }
}
