// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class HttpServerSchemaCompatibilityTest {

    private static final DictionaryEncoding DICTIONARY =
            new DictionaryEncoding(0, false, new ArrowType.Int(16, true));

    @Test
    void acceptsDictionaryEncodedUtf8ForDeclaredPlainUtf8() {
        Schema expected = schema("value", false, new ArrowType.Utf8(), null);
        Schema actual = schema("value", false, new ArrowType.Utf8(), DICTIONARY);

        assertTrue(HttpServer.schemasCompatible(actual, expected));
    }

    @Test
    void dictionaryEncodingDoesNotRelaxOtherFieldProperties() {
        Schema expected = schema("value", false, new ArrowType.Utf8(), null);

        assertFalse(HttpServer.schemasCompatible(
                schema("other", false, new ArrowType.Utf8(), DICTIONARY), expected));
        assertFalse(HttpServer.schemasCompatible(
                schema("value", true, new ArrowType.Utf8(), DICTIONARY), expected));
        assertFalse(HttpServer.schemasCompatible(
                schema("value", false, new ArrowType.Int(32, true), DICTIONARY), expected));
    }

    private static Schema schema(String name, boolean nullable, ArrowType type,
                                 DictionaryEncoding dictionary) {
        return new Schema(List.of(new Field(
                name, new FieldType(nullable, type, dictionary), List.of())));
    }
}
