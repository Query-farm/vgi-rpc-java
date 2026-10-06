// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.conformance;

import farm.query.vgirpc.errors.Code;
import farm.query.vgirpc.errors.ErrorDetail;
import farm.query.vgirpc.errors.StatusError;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Reference implementation of {@link Secondary}, matching the Python reference exactly. */
public final class SecondaryImpl implements Secondary {

    /** Kind {@code fail} answers with when asked for a code outside the closed set. */
    public static final String INVALID_CODE_KIND = "invalid_code";
    /** Kind {@code fail_oversized} raises. */
    public static final String OVERSIZED_KIND = "details_oversized";
    /** Size of the padding {@code fail_oversized} carries -- over the cap on its own. */
    public static final int OVERSIZED_PADDING_BYTES = 5000;

    @Override
    public String echo_string(String value) {
        return ECHO_PREFIX + value;
    }

    @Override
    public void fail(String code, String kind, double retry_delay_seconds) {
        if (!Code.isCanonical(code)) {
            throw new StatusError("'" + code + "' is not a canonical error code",
                    Code.INVALID_ARGUMENT, INVALID_CODE_KIND,
                    List.of(new ErrorDetail.BadRequest(List.of(new ErrorDetail.FieldViolation(
                            "code", "must be a canonical code name")))));
        }
        List<Map<String, Object>> details = new ArrayList<>();
        details.add(new ErrorDetail.ErrorInfo(Map.of("fixture", PROTOCOL_NAME)).toJson());
        if (retry_delay_seconds > 0) {
            details.add(new ErrorDetail.RetryInfo(retry_delay_seconds).toJson());
        }
        Map<String, Object> probe = new LinkedHashMap<>();
        probe.put("@type", PROTOCOL_NAME + ".Probe");
        probe.put("note", "clients ignore detail types they do not know");
        details.add(probe);
        throw new StatusError(Code.valueOf(code),
                ("conformance.Secondary.v1 fail: " + code + " " + kind).stripTrailing(),
                kind, details);
    }

    @Override
    public void fail_oversized() {
        // RetryInfo first, and small: a server that drops only the element that does not fit --
        // rather than the whole array -- keeps it, and the error then reads as retryable.
        throw new StatusError("conformance.Secondary.v1 fail_oversized: details exceed 4 KiB",
                Code.RESOURCE_EXHAUSTED, OVERSIZED_KIND, List.of(
                        new ErrorDetail.RetryInfo(1),
                        new ErrorDetail.ErrorInfo(Map.of("padding", "x".repeat(OVERSIZED_PADDING_BYTES)))));
    }
}
