// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.conformance;

import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.ExchangeState;
import farm.query.vgirpc.ProducerState;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.schema.ArrowField;
import farm.query.vgirpc.schema.ArrowFieldType;
import farm.query.vgirpc.schema.StreamHeader;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The reference cross-language conformance service. Each method exercises one or
 * more wire-protocol behaviours.
 *
 * <p>This Java surface targets the same wire shape as
 * {@code vgi_rpc.conformance._protocol.ConformanceService} in the Python reference.
 * Method and field names intentionally use {@code snake_case} to match the Python
 * wire (the framework binds kwargs by parameter name), so do not normalise them
 * to Java {@code camelCase}.</p>
 */
public interface ConformanceService {

    // --- Scalar echo --------------------------------------------------------

    String echo_string(String value);
    byte[] echo_bytes(byte[] data);

    @ArrowField(ArrowFieldType.LARGE_UTF8)
    String echo_large_string(@ArrowField(ArrowFieldType.LARGE_UTF8) String value);

    @ArrowField(ArrowFieldType.LARGE_BINARY)
    byte[] echo_large_binary(@ArrowField(ArrowFieldType.LARGE_BINARY) byte[] value);
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

    // --- Wide integer widths ----------------------------------------------

    @ArrowField(ArrowFieldType.INT8)
    long echo_int8(@ArrowField(ArrowFieldType.INT8) long value);

    @ArrowField(ArrowFieldType.INT16)
    long echo_int16(@ArrowField(ArrowFieldType.INT16) long value);

    @ArrowField(ArrowFieldType.UINT8)
    long echo_uint8(@ArrowField(ArrowFieldType.UINT8) long value);

    @ArrowField(ArrowFieldType.UINT16)
    long echo_uint16(@ArrowField(ArrowFieldType.UINT16) long value);

    @ArrowField(ArrowFieldType.UINT32)
    long echo_uint32(@ArrowField(ArrowFieldType.UINT32) long value);

    @ArrowField(ArrowFieldType.UINT64)
    long echo_uint64(@ArrowField(ArrowFieldType.UINT64) long value);

    // --- Temporal + decimal + fixed-binary --------------------------------

    @ArrowField(ArrowFieldType.DATE32)
    int echo_date(@ArrowField(ArrowFieldType.DATE32) int value);

    @ArrowField(ArrowFieldType.TIME64_US)
    long echo_time(@ArrowField(ArrowFieldType.TIME64_US) long value);

    @ArrowField(ArrowFieldType.TIMESTAMP_US)
    long echo_timestamp(@ArrowField(ArrowFieldType.TIMESTAMP_US) long value);

    @ArrowField(ArrowFieldType.TIMESTAMP_US_UTC)
    long echo_timestamp_utc(@ArrowField(ArrowFieldType.TIMESTAMP_US_UTC) long value);

    @ArrowField(ArrowFieldType.DURATION_US)
    long echo_duration(@ArrowField(ArrowFieldType.DURATION_US) long value);

    @ArrowField(ArrowFieldType.DECIMAL128_20_4)
    byte[] echo_decimal(@ArrowField(ArrowFieldType.DECIMAL128_20_4) byte[] value);

    @ArrowField(ArrowFieldType.BINARY_8)
    byte[] echo_fixed_binary(@ArrowField(ArrowFieldType.BINARY_8) byte[] value);

    WideTypes echo_wide_types(WideTypes data);
    ContainerWideTypes echo_container_wide_types(ContainerWideTypes data);

    // --- Multi-param + defaults --------------------------------------------

    double add_floats(double a, double b);
    String concatenate(String prefix, String suffix, String separator);
    String with_defaults(long required, String optional_str, long optional_int);

    // --- Errors ------------------------------------------------------------

    String raise_value_error(String message);
    String raise_runtime_error(String message);
    String raise_type_error(String message);

    // --- Client-directed logging -------------------------------------------

    String echo_with_info_log(String value, CallContext ctx);
    String echo_with_multi_logs(String value, CallContext ctx);
    String echo_with_log_extras(String value, CallContext ctx);
    String echo_with_all_log_levels(String value, CallContext ctx);

    // --- Producer streams ---------------------------------------------------

    RpcStream<? extends ProducerState> produce_n(long count);
    RpcStream<? extends ProducerState> produce_empty();
    RpcStream<? extends ProducerState> produce_single();
    RpcStream<? extends ProducerState> produce_large_batches(long rows_per_batch, long batch_count);
    RpcStream<? extends ProducerState> produce_with_logs(long count);
    RpcStream<? extends ProducerState> produce_error_mid_stream(long emit_before_error);
    RpcStream<? extends ProducerState> produce_error_on_init();

    // --- Producer streams with headers --------------------------------------

    @StreamHeader(ConformanceHeader.class)
    RpcStream<? extends ProducerState> produce_with_header(long count);

    @StreamHeader(ConformanceHeader.class)
    RpcStream<? extends ProducerState> produce_with_header_and_logs(long count, CallContext ctx);

    // --- Exchange streams ---------------------------------------------------

    RpcStream<? extends ExchangeState> exchange_scale(double factor);
    RpcStream<? extends ExchangeState> exchange_accumulate();
    RpcStream<? extends ExchangeState> exchange_with_logs();
    RpcStream<? extends ExchangeState> exchange_error_on_nth(long fail_on);
    RpcStream<? extends ExchangeState> exchange_zero_columns();
    RpcStream<? extends ExchangeState> exchange_error_on_init();

    // --- Exchange with header ----------------------------------------------

    @StreamHeader(ConformanceHeader.class)
    RpcStream<? extends ExchangeState> exchange_with_header(double factor);

    // --- Cancellation ------------------------------------------------------

    RpcStream<? extends ProducerState> cancellable_producer();
    RpcStream<? extends ExchangeState> cancellable_exchange();
    List<Long> cancel_probe_counters();
    void reset_cancel_probe();

    // --- Dynamic streams with rich headers ---------------------------------

    @StreamHeader(RichHeader.class)
    RpcStream<? extends ProducerState> produce_with_rich_header(long seed, long count);

    @StreamHeader(RichHeader.class)
    RpcStream<? extends ProducerState> produce_dynamic_schema(
            long seed, long count, boolean include_strings, boolean include_floats);

    RpcStream<? extends ExchangeState> exchange_cast_compatible();

    @StreamHeader(RichHeader.class)
    RpcStream<? extends ExchangeState> exchange_with_rich_header(long seed, double factor);
}
