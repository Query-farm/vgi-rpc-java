package farm.query.vgirpc.hash;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import farm.query.vgirpc.hash.ProtocolHash.HashMethod;
import java.util.List;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/**
 * The cross-port contract.
 *
 * <p>These values are produced by the Python reference ({@code vgi_rpc/rpc/_protocol_hash.py});
 * a mismatch means this port and that one would disagree about whether they speak the same
 * protocol. A failure is a JSON diff, not a guess: {@link ProtocolHash#canonicalDescription}
 * returns the exact preimage, so print it and compare.
 */
class ProtocolHashTest {

    private static Schema utf8Schema(String name) {
        return new Schema(
                List.of(new Field(name, FieldType.notNullable(new ArrowType.Utf8()), null)));
    }

    @Test
    void matchesThePythonReferenceDigest() {
        List<HashMethod> methods =
                List.of(
                        new HashMethod(
                                "echo",
                                "unary",
                                true,
                                false,
                                utf8Schema("value"),
                                utf8Schema("result"),
                                null));
        assertEquals(
                "a4b8ae57bf777c906081ff3610d435836b77dbc1f17a381c6a2febf1a2adb115",
                ProtocolHash.computeProtocolHash("demo.Hash.v1", methods),
                () -> "preimage: " + ProtocolHash.canonicalDescription("demo.Hash.v1", methods));
    }

    /** Absent and empty are different and must not hash alike. */
    @Test
    void omitsResultForAMethodReturningNothing() {
        List<HashMethod> methods =
                List.of(new HashMethod("fire", "unary", false, false, utf8Schema("v"), null, null));
        assertEquals(
                "{\"methods\":[{\"has_header\":false,\"has_return\":false,\"name\":\"fire\","
                        + "\"params\":[{\"name\":\"v\",\"nullable\":false,\"type\":\"utf8\"}],"
                        + "\"type\":\"unary\"}],\"protocol\":\"demo.Void.v1\"}",
                ProtocolHash.canonicalDescription("demo.Void.v1", methods));
    }

    /** A port iterating a hash map must still produce this order. */
    @Test
    void sortsMethodsByName() {
        HashMethod a = new HashMethod("a", "unary", false, false, new Schema(List.of()), null, null);
        HashMethod b = new HashMethod("b", "unary", false, false, new Schema(List.of()), null, null);
        assertEquals(
                ProtocolHash.computeProtocolHash("p", List.of(a, b)),
                ProtocolHash.computeProtocolHash("p", List.of(b, a)));
    }

    /**
     * Arrow ignores a list child's <em>name</em>, so the token must too -- otherwise two ports
     * that default differently hash the same protocol differently. Nullability it does not
     * ignore, so the token keeps that.
     */
    @Test
    void listChildNameIsNormalisedButNullabilityIsKept() {
        Field named =
                new Field(
                        "col",
                        FieldType.notNullable(new ArrowType.List()),
                        List.of(new Field("element", FieldType.nullable(new ArrowType.Int(64, true)), null)));
        assertEquals("list<item?:int64>", TypeTokens.typeToken(named));

        Field nonNull =
                new Field(
                        "col",
                        FieldType.notNullable(new ArrowType.List()),
                        List.of(
                                new Field(
                                        "item", FieldType.notNullable(new ArrowType.Int(64, true)), null)));
        assertEquals("list<item:int64>", TypeTokens.typeToken(nonNull));
        assertNotEquals(TypeTokens.typeToken(named), TypeTokens.typeToken(nonNull));
    }
}
