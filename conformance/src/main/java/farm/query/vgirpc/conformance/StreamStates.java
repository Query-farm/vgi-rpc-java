// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.conformance;

import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.ExchangeState;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.ProducerState;
import farm.query.vgirpc.log.Level;
import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/** RpcStream-state implementations used by the conformance service. */
final class StreamStates {

    private StreamStates() {}

    /** Not-null {@code int64} field. */
    private static Field i64(String name) {
        return new Field(name, FieldType.notNullable(new ArrowType.Int(64, true)), null);
    }
    /** Not-null {@code float64} field. */
    private static Field f64(String name) {
        return new Field(name, FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
    }

    static final Schema COUNTER_SCHEMA = new Schema(List.of(i64("index"), i64("value")));
    static final Schema SCALE_SCHEMA   = new Schema(List.of(f64("value")));
    static final Schema ACCUM_SCHEMA   = new Schema(List.of(f64("running_sum"), i64("exchange_count")));

    static final Schema EMPTY_SCHEMA = new Schema(List.of());

    static VectorSchemaRoot counterRow(long index, long value) {
        VectorSchemaRoot root = VectorSchemaRoot.create(COUNTER_SCHEMA, Allocators.root());
        root.allocateNew();
        ((BigIntVector) root.getVector(0)).setSafe(0, index);
        ((BigIntVector) root.getVector(1)).setSafe(0, value);
        root.setRowCount(1);
        return root;
    }

    static VectorSchemaRoot counterRows(long offset, long rowsPerBatch) {
        VectorSchemaRoot root = VectorSchemaRoot.create(COUNTER_SCHEMA, Allocators.root());
        root.allocateNew();
        BigIntVector idx = (BigIntVector) root.getVector(0);
        BigIntVector val = (BigIntVector) root.getVector(1);
        for (int i = 0; i < rowsPerBatch; i++) {
            idx.setSafe(i, offset + i);
            val.setSafe(i, (offset + i) * 10);
        }
        root.setRowCount((int) rowsPerBatch);
        return root;
    }

    /** Simple counter producer: emits {index, value=index*10} for [0, count). */
    static final class Counter extends ProducerState {
        final long count;
        long current;
        Counter(long count) { this.count = count; }
        @Override public void produce(OutputCollector out, CallContext ctx) {
            if (current >= count) { out.finish(); return; }
            out.emit(counterRow(current, current * 10));
            current++;
        }
    }

    static final class Empty extends ProducerState {
        @Override public void produce(OutputCollector out, CallContext ctx) { out.finish(); }
    }

    static final class Single extends ProducerState {
        boolean emitted;
        @Override public void produce(OutputCollector out, CallContext ctx) {
            if (emitted) { out.finish(); return; }
            emitted = true;
            out.emit(counterRow(0, 0));
        }
    }

    static final class Large extends ProducerState {
        final long rowsPerBatch;
        final long batchCount;
        long current;
        Large(long rowsPerBatch, long batchCount) { this.rowsPerBatch = rowsPerBatch; this.batchCount = batchCount; }
        @Override public void produce(OutputCollector out, CallContext ctx) {
            if (current >= batchCount) { out.finish(); return; }
            out.emit(counterRows(current * rowsPerBatch, rowsPerBatch));
            current++;
        }
    }

    static final class LoggingProducer extends ProducerState {
        final long count;
        long current;
        LoggingProducer(long count) { this.count = count; }
        @Override public void produce(OutputCollector out, CallContext ctx) {
            if (current >= count) { out.finish(); return; }
            out.clientLog(Level.INFO, "producing batch " + current);
            out.emit(counterRow(current, current * 10));
            current++;
        }
    }

    /** Emits one batch of {@code rowsPerBatch} int64 rows then finishes.  Used
     *  by the HTTP-only conformance tests to deliberately overshoot the
     *  configured response cap in a single producer iteration. */
    static final class OversizedBatch extends ProducerState {
        final long rowsPerBatch;
        boolean emitted;
        OversizedBatch(long rowsPerBatch) { this.rowsPerBatch = rowsPerBatch; }
        @Override public void produce(OutputCollector out, CallContext ctx) {
            if (emitted) { out.finish(); return; }
            out.emit(counterRows(0, rowsPerBatch));
            emitted = true;
        }
    }

    /** Companion to {@link OversizedBatch} for the lockstep exchange path —
     *  emits a fixed-size oversized output for any input. */
    static final class OversizedExchange extends ExchangeState {
        final long rowsPerBatch;
        OversizedExchange(long rowsPerBatch) { this.rowsPerBatch = rowsPerBatch; }
        @Override public void exchange(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            out.emit(counterRows(0, rowsPerBatch));
        }
    }

    static final class ErrorAfterN extends ProducerState {
        final long emitBeforeError;
        long current;
        ErrorAfterN(long n) { this.emitBeforeError = n; }
        @Override public void produce(OutputCollector out, CallContext ctx) {
            if (current >= emitBeforeError) {
                throw new RuntimeException("intentional error after " + emitBeforeError + " batches");
            }
            out.emit(counterRow(current, current * 10));
            current++;
        }
    }

    // --- Exchange states ---------------------------------------------------

    static final class Scale extends ExchangeState {
        final double factor;
        Scale(double factor) { this.factor = factor; }
        @Override public void exchange(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            int rows = input.root().getRowCount();
            Float8Vector inVec = (Float8Vector) input.root().getVector("value");
            VectorSchemaRoot root = VectorSchemaRoot.create(SCALE_SCHEMA, Allocators.root());
            root.allocateNew();
            Float8Vector outVec = (Float8Vector) root.getVector(0);
            for (int i = 0; i < rows; i++) outVec.setSafe(i, inVec.get(i) * factor);
            root.setRowCount(rows);
            out.emit(root);
        }
    }

    static final class Accumulate extends ExchangeState {
        double runningSum;
        long exchangeCount;
        @Override public void exchange(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            int rows = input.root().getRowCount();
            Float8Vector inVec = (Float8Vector) input.root().getVector("value");
            for (int i = 0; i < rows; i++) runningSum += inVec.get(i);
            exchangeCount++;
            VectorSchemaRoot root = VectorSchemaRoot.create(ACCUM_SCHEMA, Allocators.root());
            root.allocateNew();
            ((Float8Vector) root.getVector(0)).setSafe(0, runningSum);
            ((BigIntVector) root.getVector(1)).setSafe(0, exchangeCount);
            root.setRowCount(1);
            out.emit(root);
        }
    }

    static final class LoggingExchange extends ExchangeState {
        @Override public void exchange(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            out.clientLog(Level.INFO, "exchange processing");
            out.clientLog(Level.DEBUG, "exchange debug");
            out.emit(echoBatch(input.root()));
        }
    }

    /** Row-by-row, column-by-column copy of {@code in} into a freshly-allocated root of the same schema. */
    private static VectorSchemaRoot echoBatch(VectorSchemaRoot in) {
        int rows = in.getRowCount();
        VectorSchemaRoot root = VectorSchemaRoot.create(in.getSchema(), Allocators.root());
        root.allocateNew();
        for (int c = 0; c < root.getSchema().getFields().size(); c++) {
            for (int i = 0; i < rows; i++) root.getVector(c).copyFromSafe(i, i, in.getVector(c));
        }
        root.setRowCount(rows);
        return root;
    }

    static final class ZeroColumns extends ExchangeState {
        @Override public void exchange(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            VectorSchemaRoot root = VectorSchemaRoot.create(EMPTY_SCHEMA, Allocators.root());
            root.setRowCount(input.root().getRowCount());
            out.emit(root);
        }
    }

    static final class FailOnNth extends ExchangeState {
        final long failOn;
        long count;
        FailOnNth(long failOn) { this.failOn = failOn; }
        @Override public void exchange(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            count++;
            if (count >= failOn) throw new RuntimeException("intentional error on exchange " + count);
            out.emit(echoBatch(input.root()));
        }
    }

    // --- Dynamic schema producer -------------------------------------------

    static Schema dynamicSchema(boolean includeStrings, boolean includeFloats) {
        java.util.List<Field> fields = new java.util.ArrayList<>();
        fields.add(new Field("index", FieldType.nullable(new ArrowType.Int(64, true)), null));
        if (includeStrings) {
            fields.add(new Field("label", FieldType.nullable(new ArrowType.Utf8()), null));
        }
        if (includeFloats) {
            fields.add(new Field("score", FieldType.nullable(new ArrowType.FloatingPoint(
                    org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)), null));
        }
        return new Schema(fields);
    }

    static final class DynamicProducer extends ProducerState {
        final long count;
        final boolean includeStrings;
        final boolean includeFloats;
        long current;
        DynamicProducer(long count, boolean s, boolean f) {
            this.count = count; this.includeStrings = s; this.includeFloats = f;
        }
        @Override public void produce(OutputCollector out, CallContext ctx) {
            if (current >= count) { out.finish(); return; }
            Schema schema = dynamicSchema(includeStrings, includeFloats);
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root());
            root.allocateNew();
            ((BigIntVector) root.getVector("index")).setSafe(0, current);
            if (includeStrings) {
                byte[] b = ("row-" + current).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                ((org.apache.arrow.vector.VarCharVector) root.getVector("label")).setSafe(0, b);
            }
            if (includeFloats) {
                ((Float8Vector) root.getVector("score")).setSafe(0, current * 1.5);
            }
            root.setRowCount(1);
            out.emit(root);
            current++;
        }
    }

    // --- Cancellation probes -----------------------------------------------

    static final class CancelProbe {
        static long produceCalls;
        static long exchangeCalls;
        static long onCancelCalls;
        static void reset() { produceCalls = 0; exchangeCalls = 0; onCancelCalls = 0; }
    }

    static final class CancellableProducer extends ProducerState {
        long current;
        @Override public void produce(OutputCollector out, CallContext ctx) {
            CancelProbe.produceCalls++;
            out.emit(counterRow(current, current * 10));
            current++;
        }
        @Override public void onCancel(CallContext ctx) { CancelProbe.onCancelCalls++; }
    }

    static final class CancellableExchange extends ExchangeState {
        @Override public void exchange(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            CancelProbe.exchangeCalls++;
            out.emit(echoBatch(input.root()));
        }
        @Override public void onCancel(CallContext ctx) { CancelProbe.onCancelCalls++; }
    }
}
