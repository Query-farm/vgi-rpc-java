package farm.query.vgirpc.hash;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Canonical text tokens for Arrow types, for the protocol hash preimage.
 *
 * <p>The protocol hash is taken over what Arrow <em>decodes to</em>, not over what an encoder
 * emits: each language's Arrow implementation may legitimately produce different bytes for the
 * same logical schema, so a hash over serialized IPC is not a cross-language contract. The
 * preimage is canonical JSON (RFC 8785) of the decoded description, and these tokens are how a
 * type appears inside it.
 *
 * <p>JSON solves framing, escaping and key ordering. It does not solve spelling -- two ports can
 * agree on every JCS rule and still disagree on whether a microsecond timestamp is
 * {@code timestamp[us]} or {@code timestamp(us)}, which is a silent hash divergence. So the
 * vocabulary is enumerated exhaustively and {@link #typeToken} is total: an unrecognised type
 * throws rather than falling back to {@code ArrowType.toString()}, whose output is an
 * arrow-java implementation detail that differs from every other port.
 *
 * <p><b>Grammar.</b> A token is lowercase ASCII. Parameters go in parentheses, children in angle
 * brackets. A child is {@code name:token} when non-nullable and {@code name?:token} when
 * nullable -- child nullability is part of the type in Arrow, and two schemas differing only
 * there are different schemas. Numeric parameters are folded into the token
 * ({@code decimal128(38,9)}) so the preimage contains no JSON numbers and RFC 8785's hardest
 * rule, number canonicalisation, never applies. Keep it that way.
 *
 * <p><b>What is normalised.</b> Arrow's own type equality ignores the <em>name</em> of a list's
 * child field and of a map's key/value fields -- arrow-java names the list child {@code $data$},
 * other producers name it {@code item} or {@code element}. Those names are normalised, because
 * keeping them would give two ports different hashes for a protocol Arrow itself calls
 * identical. Everything Arrow does treat as part of the type is kept: child nullability, struct
 * field names, union child names and type codes, dictionary index/value types and orderedness,
 * and map {@code keysSorted}.
 */
public final class TypeTokens {

    private TypeTokens() {}

    /**
     * An Arrow type with no canonical token.
     *
     * <p>Thrown rather than falling back to {@code toString()}: a port that silently spelled an
     * unknown type its own way would produce a protocol hash that disagrees with every other
     * port, and the disagreement would surface as an unexplained mismatch at a client rather
     * than as an error here.
     */
    public static final class UnsupportedArrowTypeException extends RuntimeException {
        public UnsupportedArrowTypeException(Object type) {
            super(
                    "Arrow type "
                            + type
                            + " has no canonical token. Add one to TypeTokens.java and to every "
                            + "other port at the same time: a one-sided addition changes only this "
                            + "port's protocol hash.");
        }
    }

    /** One top-level schema field as it appears in the hash preimage. */
    public record FieldToken(String name, boolean nullable, String type) {}

    /** Arrow's own spelling of a time unit. */
    private static String unitToken(org.apache.arrow.vector.types.TimeUnit unit) {
        return switch (unit) {
            case SECOND -> "s";
            case MILLISECOND -> "ms";
            case MICROSECOND -> "us";
            case NANOSECOND -> "ns";
        };
    }

    /**
     * Spell a child whose name Arrow does not consider part of the type.
     *
     * <p>A list's child is named {@code $data$} by arrow-java and {@code item} or {@code element}
     * elsewhere, and Arrow's own type equality ignores all of it. Normalising to a fixed name is
     * what keeps two ports that default differently from hashing the same protocol differently.
     * Nullability <em>is</em> part of the type, so it is kept.
     */
    private static String anonChild(Field field, String name) {
        return name + (field.isNullable() ? "?" : "") + ":" + typeToken(field);
    }

    /** Spell a child field whose name is part of the type. */
    private static String child(Field field) {
        return anonChild(field, field.getName());
    }

    /**
     * Return the canonical token for {@code field}'s type.
     *
     * <p>Dictionary encoding is checked first because arrow-java carries it on the <em>field</em>
     * rather than on the data type -- unlike every other port's binding, where it is a
     * {@code DictionaryType}. Reading only {@code getType()} spells a dictionary-encoded string
     * as plain {@code utf8}, which is a silent divergence that survives an IPC round trip (the
     * encoding is on the wire either way) and so is invisible to any comparison of decoded
     * schemas. Only the hash catches it.
     */
    public static String typeToken(Field field) {
        var encoding = field.getDictionary();
        if (encoding != null) {
            var indexType = encoding.getIndexType();
            String index =
                    (indexType.getIsSigned() ? "int" : "uint") + indexType.getBitWidth();
            String value = typeTokenOfType(field);
            String token = "dictionary<index:" + index + ",value:" + value + ">";
            return encoding.isOrdered() ? token + ",ordered" : token;
        }
        return typeTokenOfType(field);
    }

    /** The token for a field's declared type, ignoring any dictionary encoding. */
    private static String typeTokenOfType(Field field) {
        ArrowType t = field.getType();
        List<Field> children = field.getChildren();
        return switch (t.getTypeID()) {
            case Null -> "null";
            case Bool -> "bool";
            case Int -> {
                ArrowType.Int i = (ArrowType.Int) t;
                yield (i.getIsSigned() ? "int" : "uint") + i.getBitWidth();
            }
            case FloatingPoint -> switch (((ArrowType.FloatingPoint) t).getPrecision()) {
                case HALF -> "float16";
                case SINGLE -> "float32";
                case DOUBLE -> "float64";
            };
            case Utf8 -> "utf8";
            case LargeUtf8 -> "large_utf8";
            case Utf8View -> "utf8_view";
            case Binary -> "binary";
            case LargeBinary -> "large_binary";
            case BinaryView -> "binary_view";
            case FixedSizeBinary ->
                    "fixed_size_binary(" + ((ArrowType.FixedSizeBinary) t).getByteWidth() + ")";
            case Date ->
                    ((ArrowType.Date) t).getUnit() == org.apache.arrow.vector.types.DateUnit.DAY
                            ? "date32"
                            : "date64";
            case Time -> {
                ArrowType.Time time = (ArrowType.Time) t;
                yield "time" + time.getBitWidth() + "(" + unitToken(time.getUnit()) + ")";
            }
            case Timestamp -> {
                // The zone is carried verbatim: "UTC" and "+00:00" are distinct
                // Arrow types and must not collapse to one token.
                ArrowType.Timestamp ts = (ArrowType.Timestamp) t;
                String unit = unitToken(ts.getUnit());
                yield ts.getTimezone() == null
                        ? "timestamp(" + unit + ")"
                        : "timestamp(" + unit + ",tz=" + ts.getTimezone() + ")";
            }
            case Duration -> "duration(" + unitToken(((ArrowType.Duration) t).getUnit()) + ")";
            case Interval -> switch (((ArrowType.Interval) t).getUnit()) {
                case YEAR_MONTH -> "interval_months";
                case DAY_TIME -> "interval_day_time";
                case MONTH_DAY_NANO -> "interval_month_day_nano";
            };
            case Decimal -> {
                ArrowType.Decimal d = (ArrowType.Decimal) t;
                yield "decimal" + d.getBitWidth() + "(" + d.getPrecision() + "," + d.getScale() + ")";
            }
            case List -> "list<" + anonChild(only(children, t), "item") + ">";
            case LargeList -> "large_list<" + anonChild(only(children, t), "item") + ">";
            case ListView -> "list_view<" + anonChild(only(children, t), "item") + ">";
            case LargeListView -> "large_list_view<" + anonChild(only(children, t), "item") + ">";
            case FixedSizeList ->
                    "fixed_size_list("
                            + ((ArrowType.FixedSizeList) t).getListSize()
                            + ")<"
                            + anonChild(only(children, t), "item")
                            + ">";
            case Struct -> {
                StringBuilder sb = new StringBuilder("struct<");
                for (int i = 0; i < children.size(); i++) {
                    if (i > 0) sb.append(',');
                    sb.append(child(children.get(i)));
                }
                yield sb.append('>').toString();
            }
            case Map -> {
                // A map's child is a struct of the key and value fields.
                Field entries = only(children, t);
                List<Field> kv = entries.getChildren();
                if (kv.size() != 2) throw new UnsupportedArrowTypeException(t);
                String token =
                        "map<" + anonChild(kv.get(0), "key") + "," + anonChild(kv.get(1), "value") + ">";
                // keysSorted is part of the type in Arrow, so it is part of the token.
                yield ((ArrowType.Map) t).getKeysSorted() ? token + ",keys_sorted" : token;
            }
            case Union -> {
                ArrowType.Union u = (ArrowType.Union) t;
                int[] codes = u.getTypeIds();
                StringBuilder sb =
                        new StringBuilder(
                                u.getMode() == org.apache.arrow.vector.types.UnionMode.Sparse
                                        ? "sparse_union<"
                                        : "dense_union<");
                for (int i = 0; i < children.size(); i++) {
                    if (i > 0) sb.append(',');
                    // Type codes need not be 0..n-1, so they are spelled rather
                    // than implied by position.
                    sb.append(codes != null && i < codes.length ? codes[i] : i)
                            .append('=')
                            .append(child(children.get(i)));
                }
                yield sb.append('>').toString();
            }
            default -> throw new UnsupportedArrowTypeException(t);
        };
    }

    private static Field only(List<Field> children, ArrowType t) {
        if (children.size() != 1) throw new UnsupportedArrowTypeException(t);
        return children.get(0);
    }

    /** Describe a schema's fields in declaration order, which is significant. */
    public static List<FieldToken> schemaTokens(Schema schema) {
        List<FieldToken> out = new ArrayList<>();
        if (schema == null) return out;
        for (Field f : schema.getFields()) {
            out.add(new FieldToken(f.getName(), f.isNullable(), typeToken(f)));
        }
        return out;
    }

    /** UTF-8 bytes, for callers that hash the tokens directly. */
    static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
