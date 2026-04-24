// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.conformance;

import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.ExchangeState;
import farm.query.vgirpc.ProducerState;
import farm.query.vgirpc.Stream;
import farm.query.vgirpc.log.Level;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class ConformanceServiceImpl implements ConformanceService {

    @Override public String echo_string(String value) { return value; }
    @Override public byte[] echo_bytes(byte[] data) { return data; }
    @Override public long echo_int(long value) { return value; }
    @Override public double echo_float(double value) { return value; }
    @Override public boolean echo_bool(boolean value) { return value; }

    @Override public void void_noop() {}
    @Override public void void_with_param(long value) {}

    @Override public Status echo_enum(Status status) { return status; }
    @Override public List<String> echo_list(List<String> values) { return values; }
    @Override public Map<String, Long> echo_dict(Map<String, Long> mapping) { return mapping; }
    @Override public List<List<Long>> echo_nested_list(List<List<Long>> matrix) { return matrix; }

    @Override public Optional<String> echo_optional_string(Optional<String> value) { return value; }
    @Override public Optional<Long> echo_optional_int(Optional<Long> value) { return value; }

    @Override public Point echo_point(Point point) { return point; }
    @Override public BoundingBox echo_bounding_box(BoundingBox box) { return box; }
    @Override public AllTypes echo_all_types(AllTypes data) { return data; }

    @Override public String inspect_point(Point point) {
        return "Point(x=" + point.x() + ", y=" + point.y() + ")";
    }

    @Override public int echo_int32(int value) { return value; }
    @Override public float echo_float32(float value) { return value; }

    @Override public double add_floats(double a, double b) { return a + b; }

    @Override public String concatenate(String prefix, String suffix, String separator) {
        return prefix + (separator != null ? separator : "-") + suffix;
    }

    @Override public String with_defaults(long required, String optional_str, long optional_int) {
        return "required=" + required
                + ", optional_str=" + (optional_str != null ? optional_str : "default")
                + ", optional_int=" + optional_int;
    }

    @Override public String raise_value_error(String message) {
        throw new IllegalArgumentException(message);
    }
    @Override public String raise_runtime_error(String message) {
        throw new RuntimeException(message);
    }
    @Override public String raise_type_error(String message) {
        throw new ClassCastException(message);
    }

    // --- Client-directed logging -------------------------------------------

    @Override public String echo_with_info_log(String value, CallContext ctx) {
        ctx.clientLog(Level.INFO, "info log for " + value);
        return value;
    }
    @Override public String echo_with_multi_logs(String value, CallContext ctx) {
        ctx.clientLog(Level.DEBUG, "debug log");
        ctx.clientLog(Level.INFO, "info log");
        ctx.clientLog(Level.WARN, "warn log");
        return value;
    }
    @Override public String echo_with_log_extras(String value, CallContext ctx) {
        Map<String, Object> extra = new LinkedHashMap<>();
        extra.put("key1", "value1");
        extra.put("key2", 42);
        ctx.clientLog(Level.INFO, "log with extras", extra);
        return value;
    }

    // --- Producer streams ---------------------------------------------------

    @Override public Stream<? extends ProducerState> produce_n(long count) {
        return Stream.producer(StreamStates.COUNTER_SCHEMA, new StreamStates.Counter(count));
    }
    @Override public Stream<? extends ProducerState> produce_empty() {
        return Stream.producer(StreamStates.COUNTER_SCHEMA, new StreamStates.Empty());
    }
    @Override public Stream<? extends ProducerState> produce_single() {
        return Stream.producer(StreamStates.COUNTER_SCHEMA, new StreamStates.Single());
    }
    @Override public Stream<? extends ProducerState> produce_large_batches(long rpb, long bc) {
        return Stream.producer(StreamStates.COUNTER_SCHEMA, new StreamStates.Large(rpb, bc));
    }
    @Override public Stream<? extends ProducerState> produce_with_logs(long count) {
        return Stream.producer(StreamStates.COUNTER_SCHEMA, new StreamStates.LoggingProducer(count));
    }
    @Override public Stream<? extends ProducerState> produce_error_mid_stream(long n) {
        return Stream.producer(StreamStates.COUNTER_SCHEMA, new StreamStates.ErrorAfterN(n));
    }
    @Override public Stream<? extends ProducerState> produce_error_on_init() {
        throw new RuntimeException("intentional error during stream initialization");
    }

    @Override public Stream<? extends ProducerState> produce_with_header(long count) {
        return Stream.producer(StreamStates.COUNTER_SCHEMA, new StreamStates.Counter(count),
                new ConformanceHeader(count, "producer with header"));
    }
    @Override public Stream<? extends ProducerState> produce_with_header_and_logs(long count) {
        return Stream.producer(StreamStates.COUNTER_SCHEMA, new StreamStates.LoggingProducer(count),
                new ConformanceHeader(count, "producer with header and logs"));
    }

    // --- Exchange streams ---------------------------------------------------

    @Override public Stream<? extends ExchangeState> exchange_scale(double factor) {
        return Stream.exchange(StreamStates.SCALE_SCHEMA, StreamStates.SCALE_SCHEMA,
                new StreamStates.Scale(factor));
    }
    @Override public Stream<? extends ExchangeState> exchange_accumulate() {
        return Stream.exchange(StreamStates.SCALE_SCHEMA, StreamStates.ACCUM_SCHEMA,
                new StreamStates.Accumulate());
    }
    @Override public Stream<? extends ExchangeState> exchange_with_logs() {
        return Stream.exchange(StreamStates.SCALE_SCHEMA, StreamStates.SCALE_SCHEMA,
                new StreamStates.LoggingExchange());
    }
    @Override public Stream<? extends ExchangeState> exchange_error_on_nth(long failOn) {
        return Stream.exchange(StreamStates.SCALE_SCHEMA, StreamStates.SCALE_SCHEMA,
                new StreamStates.FailOnNth(failOn));
    }
    @Override public Stream<? extends ExchangeState> exchange_zero_columns() {
        return Stream.exchange(StreamStates.EMPTY_SCHEMA, StreamStates.EMPTY_SCHEMA,
                new StreamStates.ZeroColumns());
    }
    @Override public Stream<? extends ExchangeState> exchange_error_on_init() {
        throw new RuntimeException("intentional error during exchange stream initialization");
    }
    @Override public Stream<? extends ExchangeState> exchange_with_header(double factor) {
        return Stream.exchange(StreamStates.SCALE_SCHEMA, StreamStates.SCALE_SCHEMA,
                new StreamStates.Scale(factor),
                new ConformanceHeader(0, "exchange with header"));
    }

    // --- Cancellation -------------------------------------------------------

    @Override public Stream<? extends ProducerState> cancellable_producer() {
        return Stream.producer(StreamStates.COUNTER_SCHEMA, new StreamStates.CancellableProducer());
    }
    @Override public Stream<? extends ExchangeState> cancellable_exchange() {
        return Stream.exchange(StreamStates.COUNTER_SCHEMA, StreamStates.COUNTER_SCHEMA,
                new StreamStates.CancellableExchange());
    }
    @Override public List<Long> cancel_probe_counters() {
        return List.of(StreamStates.CancelProbe.produceCalls,
                StreamStates.CancelProbe.exchangeCalls,
                StreamStates.CancelProbe.onCancelCalls);
    }
    @Override public void reset_cancel_probe() { StreamStates.CancelProbe.reset(); }

    // --- Rich-header producers / dynamic schema ----------------------------

    @Override public Stream<? extends ProducerState> produce_with_rich_header(long seed, long count) {
        return Stream.producer(StreamStates.COUNTER_SCHEMA, new StreamStates.Counter(count),
                RichHeader.build(seed));
    }
    @Override public Stream<? extends ProducerState> produce_dynamic_schema(long seed, long count,
                                                                              boolean include_strings,
                                                                              boolean include_floats) {
        org.apache.arrow.vector.types.pojo.Schema schema =
                StreamStates.dynamicSchema(include_strings, include_floats);
        return Stream.producer(schema,
                new StreamStates.DynamicProducer(count, include_strings, include_floats),
                RichHeader.build(seed));
    }
    @Override public Stream<? extends ExchangeState> exchange_cast_compatible() {
        return Stream.exchange(StreamStates.SCALE_SCHEMA, StreamStates.SCALE_SCHEMA,
                new StreamStates.Scale(1.0));
    }
    @Override public Stream<? extends ExchangeState> exchange_with_rich_header(long seed, double factor) {
        return Stream.exchange(StreamStates.SCALE_SCHEMA, StreamStates.SCALE_SCHEMA,
                new StreamStates.Scale(factor),
                RichHeader.build(seed));
    }
}
