// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.wire;


import farm.query.vgirpc.RpcMethodInfo;
import farm.query.vgirpc.ServiceIntrospector;
import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.schema.ArrowField;
import farm.query.vgirpc.schema.ArrowFieldType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A parameter annotated {@link ArrowFieldType#DICT_INT16_UTF8} must travel as a
 * real dictionary-encoded column.
 *
 * <p>{@code DICT_INT16_UTF8} maps to a bare {@code Utf8} arrow type, so for a
 * long time the annotation was only a marker: the derived schema said plain
 * utf8, the batch carried plain utf8, and every reader in the fleet resolved
 * either — so nothing complained. A <em>writer</em> is a different matter. A
 * peer that validates its declared parameter contract sees the protocol say
 * {@code dictionary<values=string, indices=int16>} and the batch say
 * {@code string}, and rejects the call before the handler runs. That is what a
 * Python worker did to every Java-client {@code catalog_schema_contents_*}
 * call, and no Java-only test could see it: the same flattened schema described
 * both ends of a Java-to-Java conversation.
 *
 * <p>So these assertions are about the DECLARATION and the BYTES, not about a
 * round trip. A round trip through this SDK passes either way.
 */
final class ParameterDictionaryEncodingTest {

    /** A service whose parameter carries the annotation, and one that does not. */
    interface DictService {
        default void listThings(byte[] handle,
                                @ArrowField(ArrowFieldType.DICT_INT16_UTF8) String type,
                                String plain) {}
    }

    private static RpcMethodInfo info() {
        Map<String, RpcMethodInfo> methods = ServiceIntrospector.describe(DictService.class);
        RpcMethodInfo m = methods.get("listThings");
        assertNotNull(m, "listThings was not introspected");
        return m;
    }

    @Test
    void theDeclaredParameterSchemaSaysDictionary() {
        Schema params = info().paramsSchema();

        Field type = params.getFields().get(1);
        assertEquals("type", type.getName());
        DictionaryEncoding enc = type.getDictionary();
        assertNotNull(enc, "an @ArrowField(DICT_INT16_UTF8) parameter must declare a dictionary "
                + "encoding — without it the annotation is inert and the column ships as plain utf8");
        assertInstanceOf(ArrowType.Int.class, enc.getIndexType());
        assertEquals(16, ((ArrowType.Int) enc.getIndexType()).getBitWidth());
        assertInstanceOf(ArrowType.Utf8.class, type.getType(),
                "the field's own type stays the VALUE type; the index type lives on the encoding");

        // The annotation is opt-in: an unannotated String parameter must not
        // suddenly start dictionary-encoding, which would break peers that
        // declare it plain.
        Field plain = params.getFields().get(2);
        assertEquals("plain", plain.getName());
        assertNull(plain.getDictionary());
    }

    @Test
    void dictionaryIdsAreDistinctWithinASchema() {
        // Two dict parameters in one schema must not share an id: the dictionary
        // batch is keyed by it, so a collision makes one column resolve against
        // the other's values.
        Map<String, RpcMethodInfo> methods = ServiceIntrospector.describe(TwoDictService.class);
        Schema params = methods.get("twoOfThem").paramsSchema();
        long a = params.getFields().get(0).getDictionary().getId();
        long b = params.getFields().get(1).getDictionary().getId();
        assertTrue(a != b, "dictionary ids collided: both are " + a);
    }

    interface TwoDictService {
        default void twoOfThem(@ArrowField(ArrowFieldType.DICT_INT16_UTF8) String a,
                               @ArrowField(ArrowFieldType.DICT_INT16_UTF8) String b) {}
    }

    @Test
    void theEncodedBatchCarriesIndicesAndADictionary() {
        BufferAllocator alloc = Allocators.root();
        Schema params = info().paramsSchema();
        Map<String, Object> values = Map.of(
                "handle", new byte[] {1, 2},
                "type", "SCALAR",
                "plain", "left alone");

        try (Marshalling.EncodedRow enc = Marshalling.encodeRowForWire(params, values, alloc)) {
            assertNotNull(enc.provider(),
                    "a schema with a dictionary column must come back with the dictionaries it "
                            + "references — a batch of indices resolving against nothing is worse "
                            + "than plain utf8");

            // The written schema is the declared one, dictionary encoding included.
            assertEquals(params, enc.root().getSchema());

            // The column holds an INDEX, not the string.
            assertInstanceOf(org.apache.arrow.vector.SmallIntVector.class, enc.root().getVector(1),
                    "a dict-encoded column must materialise as its index vector");
            int idx = ((org.apache.arrow.vector.SmallIntVector) enc.root().getVector(1)).get(0);

            // And the dictionary beside it resolves that index to the value.
            DictionaryEncoding de = params.getFields().get(1).getDictionary();
            Dictionary d = enc.provider().lookup(de.getId());
            assertNotNull(d, "no dictionary was published under the id the field declares");
            String resolved = new String(((VarCharVector) d.getVector()).get(idx),
                    StandardCharsets.UTF_8);
            assertEquals("SCALAR", resolved);

            // The unannotated neighbour is untouched.
            assertInstanceOf(VarCharVector.class, enc.root().getVector(2));
        }
    }

    @Test
    void aSchemaWithNoDictionariesPublishesNone() {
        // The fast path has to stay a fast path: publishing an empty provider
        // would make every ordinary call write a dictionary batch it does not need.
        BufferAllocator alloc = Allocators.root();
        Schema plain = new Schema(List.of(
                Field.nullable("a", new ArrowType.Utf8())));
        try (Marshalling.EncodedRow enc =
                     Marshalling.encodeRowForWire(plain, Map.of("a", "x"), alloc)) {
            assertNull(enc.provider());
            assertEquals(plain, enc.root().getSchema());
        }
    }

    @Test
    void aNullDictionaryValueStaysNull() {
        // The dictionary holds only the values actually present, so a null cell
        // must not fabricate an index into an empty dictionary.
        BufferAllocator alloc = Allocators.root();
        Schema params = info().paramsSchema();
        java.util.Map<String, Object> values = new java.util.HashMap<>();
        values.put("handle", new byte[] {1});
        values.put("type", null);
        values.put("plain", "p");

        try (Marshalling.EncodedRow enc = Marshalling.encodeRowForWire(params, values, alloc)) {
            assertTrue(enc.root().getVector(1).isNull(0), "a null dict cell must stay null");
            Dictionary d = enc.provider().lookup(params.getFields().get(1).getDictionary().getId());
            assertEquals(0, d.getVector().getValueCount(),
                    "nothing was written, so the dictionary should be empty");
        }
    }
}
