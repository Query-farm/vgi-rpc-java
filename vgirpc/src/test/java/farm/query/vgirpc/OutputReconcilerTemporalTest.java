// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * String to and from timestamp / date / time, checked case by case against
 * what the reference's pyarrow does.
 *
 * <p>The cases live in {@code pyarrow_temporal_casts.json}, recorded by
 * {@code scripts/gen_temporal_cast_cases.py} from pyarrow 25.0.1: each is one
 * {@code Array.cast} and the value it produced or the error it raised,
 * verbatim. Regenerate the file rather than editing it.
 *
 * <ul>
 *   <li>{@code ok}: Java must produce the same value;</li>
 *   <li>{@code error}: Java must refuse, with pyarrow's message;</li>
 *   <li>{@code unsupported}: pyarrow has no such cast (string to time32 /
 *       time64, binary to anything temporal); Java must refuse;</li>
 *   <li>{@code diverge}: a timezone-aware timestamp outside years
 *       -32767..32767, which pyarrow formats as wrapped garbage
 *       ({@code 17525-05-06 309237645313:46:40Z}); Java refuses, the one
 *       deliberate departure.</li>
 * </ul>
 */
final class OutputReconcilerTemporalTest {

    private static final Pattern TS = Pattern.compile("timestamp\\[(s|ms|us|ns)(?:,(.+))?]");

    static ArrowType type(String name) {
        Matcher m = TS.matcher(name);
        if (m.matches()) {
            TimeUnit u = switch (m.group(1)) {
                case "s" -> TimeUnit.SECOND;
                case "ms" -> TimeUnit.MILLISECOND;
                case "us" -> TimeUnit.MICROSECOND;
                default -> TimeUnit.NANOSECOND;
            };
            return new ArrowType.Timestamp(u, m.group(2));
        }
        return switch (name) {
            case "string" -> ArrowType.Utf8.INSTANCE;
            case "large_string" -> ArrowType.LargeUtf8.INSTANCE;
            case "binary" -> ArrowType.Binary.INSTANCE;
            case "date32" -> new ArrowType.Date(DateUnit.DAY);
            case "date64" -> new ArrowType.Date(DateUnit.MILLISECOND);
            case "time32[s]" -> new ArrowType.Time(TimeUnit.SECOND, 32);
            case "time32[ms]" -> new ArrowType.Time(TimeUnit.MILLISECOND, 32);
            case "time64[us]" -> new ArrowType.Time(TimeUnit.MICROSECOND, 64);
            case "time64[ns]" -> new ArrowType.Time(TimeUnit.NANOSECOND, 64);
            default -> throw new IllegalArgumentException(name);
        };
    }

    static void put(FieldVector v, JsonNode input) {
        if (v instanceof VarCharVector x) x.setSafe(0, input.asText().getBytes(StandardCharsets.UTF_8));
        else if (v instanceof LargeVarCharVector x) x.setSafe(0, input.asText().getBytes(StandardCharsets.UTF_8));
        else if (v instanceof VarBinaryVector x) x.setSafe(0, input.asText().getBytes(StandardCharsets.UTF_8));
        else if (v instanceof TimeStampVector x) x.setSafe(0, input.asLong());
        else if (v instanceof DateDayVector x) x.setSafe(0, input.asInt());
        else if (v instanceof DateMilliVector x) x.setSafe(0, input.asLong());
        else if (v instanceof TimeSecVector x) x.setSafe(0, input.asInt());
        else if (v instanceof TimeMilliVector x) x.setSafe(0, input.asInt());
        else if (v instanceof TimeMicroVector x) x.setSafe(0, input.asLong());
        else if (v instanceof TimeNanoVector x) x.setSafe(0, input.asLong());
        else throw new IllegalStateException(v.getClass().getName());
    }

    static Object get(FieldVector v) {
        if (v instanceof VarCharVector x) return new String(x.get(0), StandardCharsets.UTF_8);
        if (v instanceof LargeVarCharVector x) return new String(x.get(0), StandardCharsets.UTF_8);
        if (v instanceof TimeStampVector x) return x.get(0);
        if (v instanceof DateDayVector x) return (long) x.get(0);
        if (v instanceof DateMilliVector x) return x.get(0);
        throw new IllegalStateException(v.getClass().getName());
    }

    /** Emit the case's one value; return what came out, or the refusal's message prefixed "!". */
    static Object run(JsonNode c) {
        Field source = new Field("v", FieldType.nullable(type(c.get("source").asText())), null);
        Field target = new Field("v", FieldType.nullable(type(c.get("target").asText())), null);
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(source)), Allocators.root());
        root.allocateNew();
        put(root.getVector(0), c.get("input"));
        root.getVector(0).setValueCount(1);
        root.setRowCount(1);
        OutputCollector out = new OutputCollector(new Schema(List.of(target)), "test", true);
        try {
            out.emit(root);
        } catch (IllegalArgumentException e) {
            return "!" + e.getMessage();
        }
        try (VectorSchemaRoot r = out.dataEntry().root()) {
            return get(r.getVector(0));
        }
    }

    @Test
    void everyProbedCastMatchesTheReference() throws Exception {
        JsonNode doc;
        try (InputStream in = getClass().getResourceAsStream("pyarrow_temporal_casts.json")) {
            doc = new ObjectMapper().readTree(in);
        }
        assertEquals("25.0.1", doc.get("pyarrow").asText());
        List<String> mismatches = new ArrayList<>();
        int checked = 0;
        for (JsonNode c : doc.get("cases")) {
            checked++;
            String what = c.get("source").asText() + " -> " + c.get("target").asText() + " " + c.get("input");
            Object got = run(c);
            String kind = c.get("kind").asText();
            boolean refused = got instanceof String s && s.startsWith("!");
            switch (kind) {
                case "ok" -> {
                    Object want = c.get("expected").isTextual() ? c.get("expected").asText() : c.get("expected").asLong();
                    if (!want.equals(got)) mismatches.add(what + ": pyarrow " + want + ", java " + got);
                }
                case "error" -> {
                    String want = c.get("expected").asText();
                    if (!refused || !((String) got).contains(want)) {
                        mismatches.add(what + ": pyarrow raised [" + want + "], java " + got);
                    }
                }
                case "unsupported" -> {
                    if (!refused) mismatches.add(what + ": pyarrow has no such cast, java " + got);
                }
                case "diverge" -> {
                    if (!refused || !((String) got).contains("outside the years")) {
                        mismatches.add(what + ": expected a refusal, java " + got);
                    }
                }
                default -> fail("unknown kind " + kind);
            }
        }
        assertTrue(checked > 1000, "the oracle is missing cases: " + checked);
        if (!mismatches.isEmpty()) {
            fail(mismatches.size() + " of " + checked + " casts differ from pyarrow:\n  "
                    + String.join("\n  ", mismatches.subList(0, Math.min(60, mismatches.size()))));
        }
    }
}
