// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Return type for streaming RPC methods. Named {@code RpcStream} to avoid
 * shadowing {@link java.util.stream.Stream} in user code.
 *
 * <p>On the server side, methods construct an {@link RpcStream} via the static
 * factories {@link #producer(Schema, StreamState)} /
 * {@link #exchange(Schema, Schema, StreamState)} to describe the output schema
 * (and optional header) and hand the framework a {@link StreamState} instance
 * whose {@code process()} is invoked per input batch.</p>
 *
 * <p>On the client side, the dynamic proxy returns a {@code ClientStreamSession}
 * subclass. Client callers use {@link #tick()} (producer), {@link #exchange(AnnotatedBatch)}
 * (exchange), {@link #batches()} for iteration, and {@link #close()} /
 * {@link #cancel()} to end the stream.</p>
 */
public abstract class RpcStream<S extends StreamState> implements AutoCloseable {

    /** The empty Arrow schema (zero fields), used for ticks and producer inputs. */
    public static final Schema EMPTY_SCHEMA = new Schema(Collections.emptyList());

    /** Schema of output (server→client) batches. */
    public abstract Schema outputSchema();
    /** Schema of input (client→server) batches; {@link #EMPTY_SCHEMA} for producer streams. */
    public abstract Schema inputSchema();
    /** The server-side {@link StreamState}; throws on client sessions. */
    public abstract S state();
    /** The optional stream header record, or {@code null} if the method declares none. */
    public abstract ArrowSerializableRecord header();

    /** @return {@code true} if this is a producer stream (no input schema). */
    public boolean isProducer() { return inputSchema().getFields().isEmpty(); }

    // --- Client-only operations (server-built streams throw) ---------------

    /**
     * Advance a producer stream one tick and return the next batch.
     *
     * @return the next output batch
     * @throws UnsupportedOperationException on server-built streams
     */
    public AnnotatedBatch tick() {
        throw new UnsupportedOperationException("tick() only valid on client-side stream sessions");
    }
    /**
     * Send an input batch and return the next output batch (exchange streams).
     *
     * @param input the input batch to send
     * @return the next output batch
     * @throws UnsupportedOperationException on server-built streams
     */
    public AnnotatedBatch exchange(AnnotatedBatch input) {
        throw new UnsupportedOperationException("exchange() only valid on client-side stream sessions");
    }
    /** Abort the stream early (client sessions only); no-op by default. */
    public void cancel() {}
    /** End the stream and release resources; no-op by default. */
    @Override public void close() {}

    /** Iterate over data batches (client-side producer streams). */
    public Iterable<AnnotatedBatch> batches() {
        return () -> new Iterator<AnnotatedBatch>() {
            AnnotatedBatch pending;
            boolean done;
            @Override public boolean hasNext() {
                if (done) return false;
                if (pending != null) return true;
                try {
                    pending = tick();
                    return true;
                } catch (NoSuchElementException e) {
                    done = true;
                    return false;
                }
            }
            @Override public AnnotatedBatch next() {
                if (!hasNext()) throw new NoSuchElementException();
                AnnotatedBatch ab = pending;
                pending = null;
                return ab;
            }
        };
    }

    // --- Server factories --------------------------------------------------

    /**
     * Build a producer stream (server pushes output batches per tick, no input).
     *
     * @param outputSchema schema of the batches the state will emit
     * @param state per-stream state whose {@code process} runs once per tick
     * @param <S> the state type
     * @return a server-side stream
     */
    public static <S extends StreamState> RpcStream<S> producer(Schema outputSchema, S state) {
        return new ServerStream<>(outputSchema, EMPTY_SCHEMA, state, null);
    }
    /**
     * Build a producer stream that emits a header record before the body.
     *
     * @param outputSchema schema of the batches the state will emit
     * @param state per-stream state whose {@code process} runs once per tick
     * @param header header record sent on the separate header IPC stream
     * @param <S> the state type
     * @return a server-side stream
     */
    public static <S extends StreamState> RpcStream<S> producer(Schema outputSchema, S state,
                                                                 ArrowSerializableRecord header) {
        return new ServerStream<>(outputSchema, EMPTY_SCHEMA, state, header);
    }
    /**
     * Build an exchange stream (client sends an input batch each tick, server replies with one output batch).
     *
     * @param inputSchema schema of client input batches
     * @param outputSchema schema of server output batches
     * @param state per-stream state whose {@code process} runs once per input
     * @param <S> the state type
     * @return a server-side stream
     */
    public static <S extends StreamState> RpcStream<S> exchange(Schema inputSchema, Schema outputSchema, S state) {
        return new ServerStream<>(outputSchema, inputSchema, state, null);
    }
    /**
     * Build an exchange stream that emits a header record before the body.
     *
     * @param inputSchema schema of client input batches
     * @param outputSchema schema of server output batches
     * @param state per-stream state whose {@code process} runs once per input
     * @param header header record sent on the separate header IPC stream
     * @param <S> the state type
     * @return a server-side stream
     */
    public static <S extends StreamState> RpcStream<S> exchange(Schema inputSchema, Schema outputSchema, S state,
                                                                 ArrowSerializableRecord header) {
        return new ServerStream<>(outputSchema, inputSchema, state, header);
    }

    /** Concrete container used by the server. */
    static final class ServerStream<S extends StreamState> extends RpcStream<S> {
        private final Schema outputSchema;
        private final Schema inputSchema;
        private final S state;
        private final ArrowSerializableRecord header;
        ServerStream(Schema outputSchema, Schema inputSchema, S state, ArrowSerializableRecord header) {
            this.outputSchema = outputSchema;
            this.inputSchema = inputSchema;
            this.state = state;
            this.header = header;
        }
        @Override public Schema outputSchema() { return outputSchema; }
        @Override public Schema inputSchema() { return inputSchema; }
        @Override public S state() { return state; }
        @Override public ArrowSerializableRecord header() { return header; }
    }
}
