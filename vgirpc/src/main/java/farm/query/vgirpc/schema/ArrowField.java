// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.schema;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation that overrides the default inferred Arrow type for a record component
 * or method parameter. Use {@link ArrowFieldType} to specify the explicit Arrow
 * type when the default inference (e.g. {@code long → int64}) is wrong.
 *
 * <pre>{@code
 * record AllTypes(@ArrowField(ArrowFieldType.INT32) int annotatedInt32) implements ArrowSerializableRecord {}
 * int echoInt32(@ArrowField(ArrowFieldType.INT32) int value);
 * }</pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.PARAMETER, ElementType.RECORD_COMPONENT, ElementType.METHOD, ElementType.FIELD})
public @interface ArrowField {
    ArrowFieldType value();
}
