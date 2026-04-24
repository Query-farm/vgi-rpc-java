// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.benchmark;

import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.ExchangeState;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.ProducerState;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public final class BenchmarkServiceImpl implements BenchmarkService {

    private static Field i64(String name) {
        return new Field(name, FieldType.notNullable(new ArrowType.Int(64, true)), null);
    }
    private static Field f64(String name) {
        return new Field(name, FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
    }

    static final Schema GENERATE_SCHEMA  = new Schema(List.of(i64("i"), i64("value")));
    static final Schema TRANSFORM_SCHEMA = new Schema(List.of(f64("value")));

    @Override public void noop() {}

    @Override public double add(double a, double b) { return a + b; }

    @Override public String greet(String name) { return "Hello, " + name + "!"; }

    @Override
    public String roundtrip_types(Color color, Map<String, Long> mapping, List<Long> tags) {
        // Matches the Go/Python format: "{COLOR}:true:{sorted dict}:{sorted list}"
        String map = new TreeMap<>(mapping).entrySet().stream()
                .map(e -> "'" + e.getKey() + "': " + e.getValue())
                .collect(Collectors.joining(", ", "{", "}"));
        String list = tags.stream().sorted().map(String::valueOf)
                .collect(Collectors.joining(", ", "[", "]"));
        return color.name() + ":true:" + map + ":" + list;
    }

    @Override
    public RpcStream<? extends ProducerState> generate(long count) {
        return RpcStream.producer(GENERATE_SCHEMA, new GenerateState(count));
    }

    @Override
    public RpcStream<? extends ExchangeState> transform(double factor) {
        return RpcStream.exchange(TRANSFORM_SCHEMA, TRANSFORM_SCHEMA, new TransformState(factor));
    }

    // --- RpcStream states --------------------------------------------------

    static final class GenerateState extends ProducerState {
        long count;
        long current;

        // reflection-friendly no-arg ctor for HTTP state serialisation
        GenerateState() {}
        GenerateState(long count) { this.count = count; }

        @Override
        public void produce(OutputCollector out, CallContext ctx) {
            if (current >= count) { out.finish(); return; }
            VectorSchemaRoot root = VectorSchemaRoot.create(GENERATE_SCHEMA, Allocators.root());
            root.allocateNew();
            ((BigIntVector) root.getVector(0)).setSafe(0, current);
            ((BigIntVector) root.getVector(1)).setSafe(0, current * 2);
            root.setRowCount(1);
            out.emit(root);
            current++;
        }
    }

    static final class TransformState extends ExchangeState {
        double factor;

        TransformState() {}
        TransformState(double factor) { this.factor = factor; }

        @Override
        public void exchange(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            int rows = input.root().getRowCount();
            Float8Vector inVec = (Float8Vector) input.root().getVector("value");
            VectorSchemaRoot root = VectorSchemaRoot.create(TRANSFORM_SCHEMA, Allocators.root());
            root.allocateNew();
            Float8Vector outVec = (Float8Vector) root.getVector(0);
            for (int i = 0; i < rows; i++) outVec.setSafe(i, inVec.get(i) * factor);
            root.setRowCount(rows);
            out.emit(root);
        }
    }
}
