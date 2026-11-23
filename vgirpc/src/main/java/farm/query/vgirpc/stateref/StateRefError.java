// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.stateref;

import farm.query.vgirpc.HasErrorKind;
import farm.query.vgirpc.errors.Code;
import farm.query.vgirpc.errors.HasErrorCode;

/**
 * Base of every {@link StateRef}, argument-reference and turn-budget failure
 * (STATE_REF_V1.md §6). Each subclass carries its canonical code and
 * {@code error_kind}, which reach the wire unchanged.
 *
 * <p>Messages never contain URLs, tokens, object contents or digests of client data.</p>
 */
public abstract class StateRefError extends RuntimeException implements HasErrorKind, HasErrorCode {
    private static final long serialVersionUID = 1L;

    private final String kind;
    private final transient Code code;

    StateRefError(String kind, Code code, String message) {
        super(message);
        this.kind = kind;
        this.code = code;
    }

    @Override
    public String errorKind() {
        return kind;
    }

    @Override
    public Code errorCode() {
        return code;
    }
}
