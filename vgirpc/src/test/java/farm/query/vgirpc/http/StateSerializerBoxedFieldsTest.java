// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgirpc.http;

import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.StreamState;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * A stream state may declare a BOXED numeric field, and it must survive the
 * round trip.
 *
 * <p>It did not. The restorer dispatched on {@code t == int.class ||
 * t == Integer.class} and then called {@link java.lang.reflect.Field#setInt},
 * whose contract accepts only a primitive field — on the boxed half of the pair
 * it throws {@code "Can not set java.lang.Integer field X to (int)"}. The same
 * mistake sat on all seven wrapper branches.</p>
 *
 * <p>Nothing caught it because it needs three things at once: a boxed field, a
 * non-null value in it, and the HTTP transport. {@code StateSerializer} exists
 * solely to carry stream state across the stateless HTTP request boundary;
 * subprocess keeps that state in memory and never serialises it. So the first
 * boxed field to be populated — a split function's cache TTL — broke every
 * split scan over HTTP on this SDK while the subprocess suite stayed green.</p>
 *
 * <p>Each wrapper is asserted separately rather than through one representative:
 * they were seven independent copies of the same line, so one passing says
 * nothing about the other six.</p>
 */
class StateSerializerBoxedFieldsTest {

    /** Every boxed type the restorer special-cases, plus a null and a primitive control. */
    public static final class BoxedState extends StreamState {
        public Integer boxedInt;
        public Long boxedLong;
        public Double boxedDouble;
        public Float boxedFloat;
        public Boolean boxedBoolean;
        public Byte boxedByte;
        public Short boxedShort;
        public Integer absent;
        public int primitiveInt;

        /** Never invoked — this state exists only to be serialised. */
        @Override
        public void process(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            throw new UnsupportedOperationException("serialization fixture");
        }
    }

    @Test
    void everyBoxedNumericFieldSurvivesTheRoundTrip() {
        BoxedState before = new BoxedState();
        before.boxedInt = 300;          // the value that actually broke: SplitState.cacheTtl
        before.boxedLong = 9_000_000_000L;   // beyond int, so a narrowing bug shows up
        before.boxedDouble = 2.5d;
        before.boxedFloat = 1.5f;
        before.boxedBoolean = Boolean.TRUE;
        before.boxedByte = (byte) 7;
        before.boxedShort = (short) 1234;
        before.absent = null;
        before.primitiveInt = 42;

        BoxedState after = StateSerializer.deserialize(
                StateSerializer.serialize(before), BoxedState.class);

        assertEquals(300, after.boxedInt);
        assertEquals(9_000_000_000L, after.boxedLong);
        assertEquals(2.5d, after.boxedDouble);
        assertEquals(1.5f, after.boxedFloat);
        assertEquals(Boolean.TRUE, after.boxedBoolean);
        assertEquals((byte) 7, after.boxedByte);
        assertEquals((short) 1234, after.boxedShort);
        assertNull(after.absent, "a null boxed field must stay null, not become 0");
        assertEquals(42, after.primitiveInt, "the primitive path must keep working");
    }
}
