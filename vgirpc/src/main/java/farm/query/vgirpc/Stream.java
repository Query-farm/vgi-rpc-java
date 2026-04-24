// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collections;

/**
 * Return type for streaming RPC methods. Bundles the output schema, the state
 * object whose {@code process()} is called per input batch, the input schema,
 * and an optional one-time header record sent before the data stream begins.
 *
 * <p>For producer streams, leave {@code inputSchema} as {@link #EMPTY_SCHEMA}.
 * For exchange streams, set {@code inputSchema} to the real input schema.</p>
 */
public final class Stream<S extends StreamState> {

    public static final Schema EMPTY_SCHEMA = new Schema(Collections.emptyList());

    private final Schema outputSchema;
    private final S state;
    private final Schema inputSchema;
    private final ArrowSerializableRecord header;

    private Stream(Schema outputSchema, S state, Schema inputSchema, ArrowSerializableRecord header) {
        this.outputSchema = outputSchema;
        this.state = state;
        this.inputSchema = inputSchema != null ? inputSchema : EMPTY_SCHEMA;
        this.header = header;
    }

    public static <S extends StreamState> Stream<S> producer(Schema outputSchema, S state) {
        return new Stream<>(outputSchema, state, EMPTY_SCHEMA, null);
    }

    public static <S extends StreamState> Stream<S> producer(Schema outputSchema, S state,
                                                              ArrowSerializableRecord header) {
        return new Stream<>(outputSchema, state, EMPTY_SCHEMA, header);
    }

    public static <S extends StreamState> Stream<S> exchange(Schema inputSchema, Schema outputSchema, S state) {
        return new Stream<>(outputSchema, state, inputSchema, null);
    }

    public static <S extends StreamState> Stream<S> exchange(Schema inputSchema, Schema outputSchema, S state,
                                                              ArrowSerializableRecord header) {
        return new Stream<>(outputSchema, state, inputSchema, header);
    }

    public Schema outputSchema() { return outputSchema; }
    public S state() { return state; }
    public Schema inputSchema() { return inputSchema; }
    public ArrowSerializableRecord header() { return header; }

    public boolean isProducer() { return inputSchema.getFields().isEmpty(); }
}
