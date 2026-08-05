// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import java.lang.reflect.Method;

/**
 * Supplies the W3C trace and span ids of the span a call ran under, so an
 * access-log record can be joined to the surrounding distributed trace.
 *
 * <p>{@code request_id} only correlates records within one service; without a
 * trace id a log line and the span describing the same call cannot be matched.
 * Implementations read from whatever span is <em>current</em> rather than from
 * anything the framework threads through, so a record correlates with an
 * application-opened span as readily as a framework-opened one.
 */
@FunctionalInterface
public interface TraceCorrelator {

    /**
     * The trace context of the span currently in scope.
     *
     * @return a two-element array {@code {traceId, spanId}} as lowercase hex (32
     *         and 16 characters), or {@code null} when no valid span is current.
     *         Must not throw: an observability failure must not surface as a
     *         request failure.
     */
    String[] current();

    /**
     * A correlator that never reports a trace.
     *
     * @return a correlator returning {@code null}
     */
    static TraceCorrelator none() {
        return () -> null;
    }

    /**
     * Read the current OpenTelemetry span, reflectively.
     *
     * <p>OpenTelemetry is not a dependency of this module — the same split that
     * keeps {@code nimbus-jose-jwt} in {@code vgirpc-oauth} — so the API is
     * resolved by reflection once at class-load and cached as absent when it
     * isn't on the classpath. A per-call lookup would be the most expensive
     * thing on the common path, where OTel is not installed.
     *
     * @return a correlator reading {@code io.opentelemetry.api.trace.Span.current()},
     *         degrading to {@link #none()} behaviour when OpenTelemetry is absent
     */
    static TraceCorrelator openTelemetry() {
        return Otel.INSTANCE;
    }

    /** Reflective OpenTelemetry accessor, resolved once. */
    final class Otel implements TraceCorrelator {

        static final Otel INSTANCE = new Otel();

        private static final Method SPAN_CURRENT;
        private static final Method GET_SPAN_CONTEXT;
        private static final Method IS_VALID;
        private static final Method GET_TRACE_ID;
        private static final Method GET_SPAN_ID;

        static {
            Method current = null;
            Method spanContext = null;
            Method valid = null;
            Method traceId = null;
            Method spanId = null;
            try {
                Class<?> span = Class.forName("io.opentelemetry.api.trace.Span");
                Class<?> ctx = Class.forName("io.opentelemetry.api.trace.SpanContext");
                current = span.getMethod("current");
                spanContext = span.getMethod("getSpanContext");
                valid = ctx.getMethod("isValid");
                traceId = ctx.getMethod("getTraceId");
                spanId = ctx.getMethod("getSpanId");
            } catch (ReflectiveOperationException | RuntimeException e) {
                current = null;
            }
            SPAN_CURRENT = current;
            GET_SPAN_CONTEXT = spanContext;
            IS_VALID = valid;
            GET_TRACE_ID = traceId;
            GET_SPAN_ID = spanId;
        }

        private Otel() {}

        /** {@inheritDoc} */
        @Override
        public String[] current() {
            if (SPAN_CURRENT == null) return null;
            try {
                Object ctx = GET_SPAN_CONTEXT.invoke(SPAN_CURRENT.invoke(null));
                if (ctx == null || !Boolean.TRUE.equals(IS_VALID.invoke(ctx))) return null;
                return new String[] {(String) GET_TRACE_ID.invoke(ctx), (String) GET_SPAN_ID.invoke(ctx)};
            } catch (ReflectiveOperationException | RuntimeException e) {
                // Defensive: OTel API shape drift must not fail a request.
                return null;
            }
        }
    }
}
