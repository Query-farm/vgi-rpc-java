// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.conformance;

import farm.query.vgirpc.schema.ArrowField;
import farm.query.vgirpc.schema.ArrowFieldType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The reference cross-language conformance service. Each method exercises one or
 * more wire-protocol behaviours.
 *
 * <p>This Java surface targets the same wire shape as
 * {@code vgi_rpc.conformance._protocol.ConformanceService} in the Python reference.</p>
 */
public interface ConformanceService {

    // --- Scalar echo --------------------------------------------------------

    String echo_string(String value);
    byte[] echo_bytes(byte[] data);
    long echo_int(long value);
    double echo_float(double value);
    boolean echo_bool(boolean value);

    // --- Void ---------------------------------------------------------------

    void void_noop();
    void void_with_param(long value);

    // --- Complex types ------------------------------------------------------

    Status echo_enum(Status status);
    List<String> echo_list(List<String> values);
    Map<String, Long> echo_dict(Map<String, Long> mapping);
    List<List<Long>> echo_nested_list(List<List<Long>> matrix);

    // --- Optionals ----------------------------------------------------------

    Optional<String> echo_optional_string(Optional<String> value);
    Optional<Long> echo_optional_int(Optional<Long> value);

    // --- Dataclass round-trip -----------------------------------------------

    Point echo_point(Point point);
    BoundingBox echo_bounding_box(BoundingBox box);
    AllTypes echo_all_types(AllTypes data);

    // --- Dataclass as parameter --------------------------------------------

    String inspect_point(Point point);

    // --- Annotated types ---------------------------------------------------

    @ArrowField(ArrowFieldType.INT32)
    int echo_int32(@ArrowField(ArrowFieldType.INT32) int value);

    @ArrowField(ArrowFieldType.FLOAT32)
    float echo_float32(@ArrowField(ArrowFieldType.FLOAT32) float value);

    // --- Multi-param + defaults --------------------------------------------

    double add_floats(double a, double b);
    String concatenate(String prefix, String suffix, String separator);
    String with_defaults(long required, String optional_str, long optional_int);

    // --- Errors ------------------------------------------------------------

    String raise_value_error(String message);
    String raise_runtime_error(String message);
    String raise_type_error(String message);

    // --- Client-directed logging -------------------------------------------

    String echo_with_info_log(String value, farm.query.vgirpc.CallContext ctx);
    String echo_with_multi_logs(String value, farm.query.vgirpc.CallContext ctx);
    String echo_with_log_extras(String value, farm.query.vgirpc.CallContext ctx);

    // --- Producer streams ---------------------------------------------------

    farm.query.vgirpc.Stream<? extends farm.query.vgirpc.ProducerState> produce_n(long count);
    farm.query.vgirpc.Stream<? extends farm.query.vgirpc.ProducerState> produce_empty();
    farm.query.vgirpc.Stream<? extends farm.query.vgirpc.ProducerState> produce_single();
    farm.query.vgirpc.Stream<? extends farm.query.vgirpc.ProducerState> produce_large_batches(
            long rows_per_batch, long batch_count);
    farm.query.vgirpc.Stream<? extends farm.query.vgirpc.ProducerState> produce_with_logs(long count);
    farm.query.vgirpc.Stream<? extends farm.query.vgirpc.ProducerState> produce_error_mid_stream(
            long emit_before_error);
    farm.query.vgirpc.Stream<? extends farm.query.vgirpc.ProducerState> produce_error_on_init();

    // --- Producer streams with headers --------------------------------------

    @farm.query.vgirpc.schema.StreamHeader(ConformanceHeader.class)
    farm.query.vgirpc.Stream<? extends farm.query.vgirpc.ProducerState> produce_with_header(long count);

    @farm.query.vgirpc.schema.StreamHeader(ConformanceHeader.class)
    farm.query.vgirpc.Stream<? extends farm.query.vgirpc.ProducerState> produce_with_header_and_logs(long count);

    // --- Exchange streams ---------------------------------------------------

    farm.query.vgirpc.Stream<? extends farm.query.vgirpc.ExchangeState> exchange_scale(double factor);
    farm.query.vgirpc.Stream<? extends farm.query.vgirpc.ExchangeState> exchange_accumulate();
    farm.query.vgirpc.Stream<? extends farm.query.vgirpc.ExchangeState> exchange_with_logs();
    farm.query.vgirpc.Stream<? extends farm.query.vgirpc.ExchangeState> exchange_error_on_nth(long fail_on);
    farm.query.vgirpc.Stream<? extends farm.query.vgirpc.ExchangeState> exchange_zero_columns();
    farm.query.vgirpc.Stream<? extends farm.query.vgirpc.ExchangeState> exchange_error_on_init();

    // --- Exchange with header ----------------------------------------------

    @farm.query.vgirpc.schema.StreamHeader(ConformanceHeader.class)
    farm.query.vgirpc.Stream<? extends farm.query.vgirpc.ExchangeState> exchange_with_header(double factor);

    // --- Cancellation ------------------------------------------------------

    farm.query.vgirpc.Stream<? extends farm.query.vgirpc.ProducerState> cancellable_producer();
    farm.query.vgirpc.Stream<? extends farm.query.vgirpc.ExchangeState> cancellable_exchange();
    java.util.List<Long> cancel_probe_counters();
    void reset_cancel_probe();

    // --- Dynamic streams with rich headers ---------------------------------

    @farm.query.vgirpc.schema.StreamHeader(RichHeader.class)
    farm.query.vgirpc.Stream<? extends farm.query.vgirpc.ProducerState> produce_with_rich_header(long seed, long count);

    @farm.query.vgirpc.schema.StreamHeader(RichHeader.class)
    farm.query.vgirpc.Stream<? extends farm.query.vgirpc.ProducerState> produce_dynamic_schema(
            long seed, long count, boolean include_strings, boolean include_floats);

    farm.query.vgirpc.Stream<? extends farm.query.vgirpc.ExchangeState> exchange_cast_compatible();

    @farm.query.vgirpc.schema.StreamHeader(RichHeader.class)
    farm.query.vgirpc.Stream<? extends farm.query.vgirpc.ExchangeState> exchange_with_rich_header(long seed, double factor);
}
