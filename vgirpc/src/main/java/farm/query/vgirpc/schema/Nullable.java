// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.schema;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a parameter, method return, or record component as nullable. Equivalent
 * to Python's {@code T | None}. Use this on parameters where {@code Optional<T>}
 * is undesirable (e.g. boxed {@code Long}).
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.PARAMETER, ElementType.RECORD_COMPONENT, ElementType.METHOD, ElementType.FIELD})
public @interface Nullable {
}
