// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.benchmark;

import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.ExchangeState;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.ProducerState;
import farm.query.vgirpc.Stream;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public final class BenchmarkServiceImpl implements BenchmarkService {

    static final Schema GENERATE_SCHEMA = new Schema(List.of(
            new Field("i", FieldType.notNullable(new ArrowType.Int(64, true)), null),
            new Field("value", FieldType.notNullable(new ArrowType.Int(64, true)), null)));

    static final Schema TRANSFORM_SCHEMA = new Schema(List.of(
            new Field("value", FieldType.notNullable(
                    new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)));

    @Override public void noop() {}

    @Override public double add(double a, double b) { return a + b; }

    @Override public String greet(String name) { return "Hello, " + name + "!"; }

    @Override
    public String roundtrip_types(Color color, Map<String, Long> mapping, List<Long> tags) {
        // Matches the Go/Python format: "{COLOR}:true:{sorted dict}:{sorted list}"
        Map<String, Long> sortedMap = new TreeMap<>(mapping);
        StringBuilder map = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Long> e : sortedMap.entrySet()) {
            if (!first) map.append(", "); first = false;
            map.append("'").append(e.getKey()).append("': ").append(e.getValue());
        }
        map.append("}");

        List<Long> sortedTags = new ArrayList<>(tags);
        Collections.sort(sortedTags);
        StringBuilder list = new StringBuilder("[");
        for (int i = 0; i < sortedTags.size(); i++) {
            if (i > 0) list.append(", ");
            list.append(sortedTags.get(i));
        }
        list.append("]");

        return color.name() + ":true:" + map + ":" + list;
    }

    @Override
    public Stream<? extends ProducerState> generate(long count) {
        return Stream.producer(GENERATE_SCHEMA, new GenerateState(count));
    }

    @Override
    public Stream<? extends ExchangeState> transform(double factor) {
        return Stream.exchange(TRANSFORM_SCHEMA, TRANSFORM_SCHEMA, new TransformState(factor));
    }

    // --- Stream states --------------------------------------------------

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
