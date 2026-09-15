package farm.query.vgirpc.hash;

import farm.query.vgirpc.hash.TypeTokens.FieldToken;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * The protocol hash: a fingerprint of a protocol's wire surface.
 *
 * <p>A client and a worker agree on a protocol or they do not, and the hash is how either side
 * says which one it has without shipping the whole description. For that to be worth anything
 * the same protocol must hash the same in every port, which the previous definition could not
 * promise: it hashed serialized Arrow IPC bytes, and each language's Arrow implementation may
 * legitimately emit different bytes for the same logical schema. The docs said so, which made
 * the field advisory -- comparable only against itself.
 *
 * <p>So the preimage is canonical JSON of what Arrow <em>decodes to</em>:
 *
 * <pre>sha256("vgi_rpc.protocol_hash.v1|" + canonicalJson(description))</pre>
 *
 * <p>Profile: RFC 8785 (JCS), chosen for its published test vectors. The structure is
 * deliberately restricted to objects, arrays, strings and booleans; every number is folded into
 * a type token ({@code decimal128(38,9)}), so JCS's hardest rule -- number canonicalisation, and
 * the likeliest place for six ports to diverge -- never applies. Keep it that way.
 *
 * <p>Not in the preimage: server identity, docstrings, parameter defaults, language-specific
 * type names, the framework's own request/describe versions, and whether a stream is an
 * exchange. That last is an <em>implementation</em> property, not visible on the protocol
 * definition, so one port can determine it and another cannot -- and a field one port knows and
 * another does not cannot be part of a cross-language contract. It still reaches clients as
 * {@code stream_kind} on the description, where "unknown" is a sayable answer; a hash has no
 * such option.
 */
public final class ProtocolHash {

    private ProtocolHash() {}

    /**
     * Domain separator. Moves only when the hash definition moves, never when a protocol changes
     * -- that is what the hash itself is for.
     */
    public static final String HASH_DOMAIN = "vgi_rpc.protocol_hash.v1|";

    /**
     * One method's input.
     *
     * <p>Takes decoded schemas rather than serialized IPC: the hash is over structure, and
     * accepting bytes would invite a caller to pass whatever its encoder produced.
     */
    public record HashMethod(
            String name,
            // "unary" or "stream".
            String methodType,
            boolean hasReturn,
            boolean hasHeader,
            Schema paramsSchema,
            // Ignored when hasReturn is false.
            Schema resultSchema,
            // Ignored when hasHeader is false.
            Schema headerSchema) {}

    /**
     * Build the canonical preimage for one protocol.
     *
     * <p>Exposed because a hash mismatch between ports is otherwise one bit of information. With
     * the preimage in hand a failing port diffs two JSON documents and sees which method, field
     * or type token it spells differently.
     */
    public static String canonicalDescription(String protocolName, List<HashMethod> methods) {
        // Sorted so two ports iterating differently-ordered maps still agree.
        List<HashMethod> sorted = new ArrayList<>(methods);
        sorted.sort(Comparator.comparing(HashMethod::name));

        StringBuilder out = new StringBuilder("{\"methods\":[");
        for (int i = 0; i < sorted.size(); i++) {
            if (i > 0) out.append(',');
            out.append(methodEntry(sorted.get(i)));
        }
        return out.append("],\"protocol\":").append(jsonString(protocolName)).append('}').toString();
    }

    /**
     * One method entry, with its keys emitted in sorted order.
     *
     * <p>Canonical JSON requires sorted keys, and the order here is load bearing rather than
     * cosmetic: emitting {@code header} after {@code params} would change the digest for every
     * protocol that has a stream header, which is exactly the class of bug the canonical form
     * exists to prevent.
     */
    private static String methodEntry(HashMethod m) {
        StringBuilder sb = new StringBuilder("{");
        sb.append("\"has_header\":").append(m.hasHeader());
        sb.append(",\"has_return\":").append(m.hasReturn());
        // Absent and empty are different: a method returning nothing is not a
        // method returning an empty struct, and they must not hash alike.
        if (m.hasHeader()) {
            sb.append(",\"header\":").append(fieldTokens(m.headerSchema()));
        }
        sb.append(",\"name\":").append(jsonString(m.name()));
        sb.append(",\"params\":").append(fieldTokens(m.paramsSchema()));
        if (m.hasReturn()) {
            sb.append(",\"result\":").append(fieldTokens(m.resultSchema()));
        }
        sb.append(",\"type\":").append(jsonString(m.methodType()));
        return sb.append('}').toString();
    }

    private static String fieldTokens(Schema schema) {
        List<FieldToken> tokens = TypeTokens.schemaTokens(schema);
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < tokens.size(); i++) {
            FieldToken t = tokens.get(i);
            if (i > 0) sb.append(',');
            sb.append("{\"name\":")
                    .append(jsonString(t.name()))
                    .append(",\"nullable\":")
                    .append(t.nullable())
                    .append(",\"type\":")
                    .append(jsonString(t.type()))
                    .append('}');
        }
        return sb.append(']').toString();
    }

    /**
     * Encode a string the way RFC 8785 requires.
     *
     * <p>Short escapes where JCS mandates them, {@code \\uXXXX} only for the remaining control
     * characters, and every other character emitted as itself -- notably <em>not</em>
     * ASCII-escaped, which is where a JSON library's defaults would silently diverge from the
     * other ports.
     */
    private static String jsonString(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }

    /**
     * Return the SHA-256 hex digest of a protocol's canonical description.
     *
     * <p>Identical in every port for the same protocol -- which is a property conformance can
     * assert, and could not before.
     */
    public static String computeProtocolHash(String protocolName, List<HashMethod> methods) {
        byte[] preimage =
                (HASH_DOMAIN + canonicalDescription(protocolName, methods))
                        .getBytes(StandardCharsets.UTF_8);
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by every JRE", e);
        }
        StringBuilder hex = new StringBuilder(64);
        for (byte b : md.digest(preimage)) {
            hex.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
        }
        return hex.toString();
    }
}
