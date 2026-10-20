// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import org.apache.arrow.vector.types.pojo.ArrowType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Scalar conversions that must produce exactly what Arrow C++'s cast kernels
 * produce, because the reference casts with them: number formatting and
 * parsing, and the decimal arithmetic. Each method names the Arrow C++ routine
 * it mirrors; the failure messages are Arrow's own.
 */
final class ArrowCasts {

    private ArrowCasts() {}

    // ------------------------------------------------------------------
    // Number -> string (arrow/util/formatting.cc)
    // ------------------------------------------------------------------

    /**
     * Arrow's {@code FloatToStringFormatter}: double-conversion's shortest
     * round-trip digits, fixed notation for decimal exponents in
     * {@code [-6, 10)}, otherwise {@code d.ddde+X}; no trailing {@code .0};
     * {@code nan}, {@code inf}, {@code -inf}, {@code -0}.
     */
    static String formatDouble(double d) {
        if (Double.isNaN(d)) return "nan";
        if (Double.isInfinite(d)) return d > 0 ? "inf" : "-inf";
        if (d == 0) return (1 / d < 0) ? "-0" : "0";
        double a = Math.abs(d);
        BigDecimal shortest = shortest(new BigDecimal(Double.toString(a)), a, false);
        return (d < 0 ? "-" : "") + layout(shortest);
    }

    /** As {@link #formatDouble}, with single-precision shortest digits ({@code ToShortestSingle}). */
    static String formatFloat(float f) {
        if (Float.isNaN(f)) return "nan";
        if (Float.isInfinite(f)) return f > 0 ? "inf" : "-inf";
        if (f == 0) return (1 / f < 0) ? "-0" : "0";
        float a = Math.abs(f);
        BigDecimal shortest = shortest(new BigDecimal(Float.toString(a)), a, true);
        return (f < 0 ? "-" : "") + layout(shortest);
    }

    /**
     * Java's {@code toString} is shortest-round-trip too (JDK 19+), but never
     * shorter than two digits: {@code Double.MIN_VALUE} prints {@code 4.9E-324}
     * where the one-digit {@code 5e-324} also round-trips. Recover the
     * one-digit form when one exists, choosing the closer candidate.
     */
    private static BigDecimal shortest(BigDecimal javaDigits, double value, boolean single) {
        BigDecimal s = javaDigits.stripTrailingZeros();
        if (s.precision() != 2) return s;
        BigDecimal unit = BigDecimal.ONE.scaleByPowerOfTen(s.precision() - s.scale() - 1);
        BigDecimal lead = new BigDecimal(s.unscaledValue().divide(BigInteger.TEN)).multiply(unit);
        BigDecimal exact = new BigDecimal(value);
        BigDecimal best = null;
        for (BigDecimal c : new BigDecimal[] {lead, lead.add(unit)}) {
            if (c.signum() == 0) continue;
            boolean roundTrips = single
                    ? Float.parseFloat(c.toString()) == (float) value
                    : Double.parseDouble(c.toString()) == value;
            if (!roundTrips) continue;
            if (best == null || c.subtract(exact).abs().compareTo(best.subtract(exact).abs()) < 0) best = c;
        }
        return best != null ? best.stripTrailingZeros() : s;
    }

    private static String layout(BigDecimal v) {
        String digits = v.unscaledValue().toString();
        int n = digits.length();
        int point = n - v.scale();      // digits before the decimal point
        int exponent = point - 1;
        StringBuilder sb = new StringBuilder();
        if (exponent >= -6 && exponent < 10) {
            if (point <= 0) {
                sb.append("0.").append("0".repeat(-point)).append(digits);
            } else if (point >= n) {
                sb.append(digits).append("0".repeat(point - n));
            } else {
                sb.append(digits, 0, point).append('.').append(digits, point, n);
            }
        } else {
            sb.append(digits.charAt(0));
            if (n > 1) sb.append('.').append(digits, 1, n);
            sb.append('e').append(exponent >= 0 ? '+' : '-').append(Math.abs(exponent));
        }
        return sb.toString();
    }

    /** Arrow's integer formatting: plain decimal, unsigned for unsigned types. */
    static String formatInt(long raw, ArrowType.Int t) {
        return !t.getIsSigned() && t.getBitWidth() == 64 ? Long.toUnsignedString(raw) : Long.toString(raw);
    }

    // ------------------------------------------------------------------
    // String -> number (arrow/util/value_parsing.h)
    // ------------------------------------------------------------------

    private static final Pattern DEC_INT = Pattern.compile("-?[0-9]+");
    private static final Pattern HEX_INT = Pattern.compile("0[xX][0-9a-fA-F]+");
    private static final Pattern REAL = Pattern.compile("[+-]?([0-9]+(\\.[0-9]*)?|\\.[0-9]+)([eE][+-]?[0-9]+)?");
    private static final Pattern REAL_SPECIAL = Pattern.compile("[+-]?(nan|inf|infinity)", Pattern.CASE_INSENSITIVE);

    /**
     * Arrow's {@code StringConverter<IntegerType>}: an optional {@code -} then
     * decimal digits (no {@code +}, no whitespace), or an unsigned
     * {@code 0x}/{@code 0X} hex literal whose bits are taken at the target's
     * width ({@code 0xff} is {@code -1} as int8). Returns the value's bits.
     */
    static long parseInt(String s, ArrowType.Int t) {
        int bits = t.getBitWidth();
        BigInteger v;
        if (HEX_INT.matcher(s).matches()) {
            v = new BigInteger(s.substring(2), 16);
            if (v.bitLength() > bits) throw parseFailure(s, intName(t));
            if (t.getIsSigned() && v.testBit(bits - 1)) v = v.subtract(BigInteger.ONE.shiftLeft(bits));
        } else if (DEC_INT.matcher(s).matches()) {
            if (!t.getIsSigned() && s.startsWith("-")) throw parseFailure(s, intName(t));
            v = new BigInteger(s);
            BigInteger min = t.getIsSigned() ? BigInteger.ONE.shiftLeft(bits - 1).negate() : BigInteger.ZERO;
            BigInteger max = t.getIsSigned() ? BigInteger.ONE.shiftLeft(bits - 1).subtract(BigInteger.ONE)
                    : BigInteger.ONE.shiftLeft(bits).subtract(BigInteger.ONE);
            if (v.compareTo(min) < 0 || v.compareTo(max) > 0) throw parseFailure(s, intName(t));
        } else {
            throw parseFailure(s, intName(t));
        }
        return v.longValue();
    }

    /**
     * Arrow's real parsing (fast_float): optional sign, digits with an optional
     * point and exponent, or {@code nan} / {@code inf} / {@code infinity} in any
     * case; no whitespace, hex or suffixes. Correctly rounded; overflow is
     * infinite, underflow zero.
     */
    static double parseDouble(String s, String typeName) {
        if (REAL_SPECIAL.matcher(s).matches()) {
            boolean neg = s.startsWith("-");
            String body = s.replaceFirst("^[+-]", "").toLowerCase(Locale.ROOT);
            if (body.equals("nan")) return Double.NaN;
            return neg ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
        }
        if (!REAL.matcher(s).matches()) throw parseFailure(s, typeName);
        return Double.parseDouble(s);
    }

    /** As {@link #parseDouble}, rounded directly to single precision. */
    static float parseFloat(String s) {
        if (REAL_SPECIAL.matcher(s).matches()) return (float) parseDouble(s, "float");
        if (!REAL.matcher(s).matches()) throw parseFailure(s, "float");
        return Float.parseFloat(s);
    }

    /** Arrow's boolean parsing: {@code true} / {@code false} in any case, {@code 1} / {@code 0}. */
    static boolean parseBool(String s) {
        if (s.equalsIgnoreCase("true") || s.equals("1")) return true;
        if (s.equalsIgnoreCase("false") || s.equals("0")) return false;
        throw new IllegalArgumentException("Failed to parse value: " + s);
    }

    private static IllegalArgumentException parseFailure(String s, String typeName) {
        return new IllegalArgumentException("Failed to parse string: '" + s + "' as a scalar of type " + typeName);
    }

    static String intName(ArrowType.Int t) {
        return (t.getIsSigned() ? "int" : "uint") + t.getBitWidth();
    }

    // ------------------------------------------------------------------
    // Decimals (arrow/compute/kernels/scalar_cast_numeric.cc, arrow/util/decimal.cc)
    // ------------------------------------------------------------------

    static String decimalName(int bitWidth) { return bitWidth == 256 ? "decimal256" : "decimal128"; }

    private static String decimalTypeName(int bitWidth) { return bitWidth == 256 ? "Decimal256" : "Decimal128"; }

    static BigInteger pow10(int n) { return BigInteger.TEN.pow(n); }

    /** {@code Decimal::Rescale}: exact, or "data loss". */
    static BigInteger rescale(BigInteger unscaled, int fromScale, int toScale) {
        if (toScale >= fromScale) return unscaled.multiply(pow10(toScale - fromScale));
        BigInteger[] qr = unscaled.divideAndRemainder(pow10(fromScale - toScale));
        if (qr[1].signum() != 0) throw new IllegalArgumentException("Rescaling Decimal value would cause data loss");
        return qr[0];
    }

    static boolean fitsPrecision(BigInteger unscaled, int precision) {
        return unscaled.abs().compareTo(pow10(precision)) < 0;
    }

    /** Decimal to decimal: rescale exactly, then check the target precision. */
    static BigInteger decimalToDecimal(BigInteger unscaled, int fromScale, int precision, int scale) {
        BigInteger v = rescale(unscaled, fromScale, scale);
        if (!fitsPrecision(v, precision)) {
            throw new IllegalArgumentException("Decimal value does not fit in precision " + precision);
        }
        return v;
    }

    /**
     * Integer to decimal. Arrow checks the TYPE, not the value: the target
     * must hold every value of the source type ({@code precision - scale} at
     * least the type's decimal digits), and a negative scale is refused.
     */
    static BigInteger intToDecimal(long raw, ArrowType.Int t, int precision, int scale) {
        if (scale < 0) throw new IllegalArgumentException("Scale must be non-negative");
        int digits = switch (t.getBitWidth()) {
            case 8 -> 3;
            case 16 -> 5;
            case 32 -> 10;
            default -> t.getIsSigned() ? 19 : 20;
        };
        if (precision - scale < digits) {
            throw new IllegalArgumentException("Precision is not great enough for the result. It should be at least "
                    + (digits + scale));
        }
        BigInteger v = !t.getIsSigned() && t.getBitWidth() == 64 && raw < 0
                ? new BigInteger(Long.toUnsignedString(raw)) : BigInteger.valueOf(raw);
        return v.multiply(pow10(scale));
    }

    /** Decimal to integer: exact at scale 0 ("data loss"), then in range ("out of bounds"). */
    static BigInteger decimalToInt(BigInteger unscaled, int scale, ArrowType.Int t) {
        BigInteger v = rescale(unscaled, scale, 0);
        int bits = t.getBitWidth();
        BigInteger min = t.getIsSigned() ? BigInteger.ONE.shiftLeft(bits - 1).negate() : BigInteger.ZERO;
        BigInteger max = t.getIsSigned() ? BigInteger.ONE.shiftLeft(bits - 1).subtract(BigInteger.ONE)
                : BigInteger.ONE.shiftLeft(bits).subtract(BigInteger.ONE);
        if (v.compareTo(min) < 0 || v.compareTo(max) > 0) throw new IllegalArgumentException("Integer value out of bounds");
        return v;
    }

    private static final Pattern DECIMAL_LITERAL =
            Pattern.compile("[+-]?([0-9]+(\\.[0-9]*)?|\\.[0-9]+)([eE][+-]?[0-9]+)?");

    /** {@code Decimal::FromString}, then the cast's rescale and precision checks. */
    static BigInteger parseDecimal(String s, int precision, int scale, int bitWidth) {
        if (!DECIMAL_LITERAL.matcher(s).matches()) {
            throw new IllegalArgumentException("The string '" + s + "' is not a valid " + decimalName(bitWidth) + " number");
        }
        BigDecimal parsed = new BigDecimal(s);
        return decimalToDecimal(parsed.unscaledValue(), parsed.scale(), precision, scale);
    }

    /** {@code Decimal::ToString(scale)}: plain unless the scale is negative or the value is below 1e-6. */
    static String decimalToString(BigInteger unscaled, int scale) {
        return new BigDecimal(unscaled, scale).toString();
    }

    // --- decimal -> real: DecimalRealConversion::ToReal --------------------

    private static final double TWO_64 = 0x1p64;
    private static final double TWO_128 = 0x1p128;
    private static final double TWO_192 = 0x1p192;

    static double decimalToDouble(BigInteger unscaled, int scale, int bitWidth) {
        if (unscaled.signum() < 0) return -toDoublePositive(unscaled.negate(), scale, bitWidth);
        return toDoublePositive(unscaled, scale, bitWidth);
    }

    static float decimalToFloat(BigInteger unscaled, int scale, int bitWidth) {
        if (unscaled.signum() < 0) return -toFloatPositive(unscaled.negate(), scale, bitWidth);
        return toFloatPositive(unscaled, scale, bitWidth);
    }

    private static boolean precise(BigInteger v, int bitWidth, long maxPrecise) {
        // 128: high == 0 && low <= max; 256: upper three words zero && low < max.
        if (v.bitLength() > 64) return false;
        int cmp = Long.compareUnsigned(v.longValue(), maxPrecise);
        return bitWidth == 256 ? cmp < 0 : cmp <= 0;
    }

    private static double toDoublePositive(BigInteger v, int scale, int bitWidth) {
        if (scale <= 0 || precise(v, bitWidth, (1L << 53) - 1)) return noSplitDouble(v, scale, bitWidth);
        BigInteger[] wf = v.divideAndRemainder(pow10(scale));
        return noSplitDouble(wf[0], 0, bitWidth) + noSplitDouble(wf[1], scale, bitWidth);
    }

    private static float toFloatPositive(BigInteger v, int scale, int bitWidth) {
        if (scale <= 0 || precise(v, bitWidth, (1L << 24) - 1)) return noSplitFloat(v, scale, bitWidth);
        BigInteger[] wf = v.divideAndRemainder(pow10(scale));
        return noSplitFloat(wf[0], 0, bitWidth) + noSplitFloat(wf[1], scale, bitWidth);
    }

    private static long word(BigInteger v, int i) { return v.shiftRight(64 * i).longValue(); }

    private static double noSplitDouble(BigInteger v, int scale, int bitWidth) {
        double x;
        if (bitWidth == 256) {
            x = 0;
            x += u64ToDouble(word(v, 3)) * TWO_192;
            x += u64ToDouble(word(v, 2)) * TWO_128;
            x += u64ToDouble(word(v, 1)) * TWO_64;
            x += u64ToDouble(word(v, 0));
        } else {
            x = (double) word(v, 1) * TWO_64;   // the signed high word; non-negative here
            x += u64ToDouble(word(v, 0));
        }
        return x * pow10Double(-scale);
    }

    private static float noSplitFloat(BigInteger v, int scale, int bitWidth) {
        float x;
        if (bitWidth == 256) {
            x = 0;
            x += word(v, 3) == 0 ? 0f : Float.POSITIVE_INFINITY;   // two_to_192 overflows a float
            x += word(v, 2) == 0 ? 0f : Float.POSITIVE_INFINITY;   // two_to_128 likewise
            x += u64ToFloat(word(v, 1)) * 0x1p64f;
            x += u64ToFloat(word(v, 0));
        } else {
            x = (float) word(v, 1) * 0x1p64f;
            x += u64ToFloat(word(v, 0));
        }
        return x * pow10Float(-scale);
    }

    private static double u64ToDouble(long w) {
        return w >= 0 ? (double) w : new BigInteger(Long.toUnsignedString(w)).doubleValue();
    }

    private static float u64ToFloat(long w) {
        return w >= 0 ? (float) w : Float.parseFloat(Long.toUnsignedString(w));
    }

    /** Arrow's precomputed {@code kDoublePowersOfTen}: the double nearest 10^e, for |e| <= 76. */
    static double pow10Double(int e) {
        return Math.abs(e) <= 76 ? Double.parseDouble("1e" + e) : Math.pow(10, e);
    }

    static float pow10Float(int e) {
        return Math.abs(e) <= 76 ? Float.parseFloat("1e" + e) : (float) Math.pow(10, e);
    }

    // --- real -> decimal: DecimalRealConversion::FromReal ------------------

    static BigInteger doubleToDecimal(double x, int precision, int scale, int bitWidth) {
        return fromReal(x, false, precision, scale, bitWidth);
    }

    static BigInteger floatToDecimal(float x, int precision, int scale, int bitWidth) {
        return fromReal(x, true, precision, scale, bitWidth);
    }

    private static BigInteger fromReal(double x, boolean single, int precision, int scale, int bitWidth) {
        if (Double.isNaN(x) || Double.isInfinite(x)) {
            throw new IllegalArgumentException("Cannot convert " + cFormat(x) + " to " + decimalTypeName(bitWidth));
        }
        if (x == 0) return BigInteger.ZERO;
        if (x < 0) return fromPositiveReal(-x, single, precision, scale, bitWidth).negate();
        return fromPositiveReal(x, single, precision, scale, bitWidth);
    }

    private static BigInteger fromPositiveReal(double real, boolean single, int precision, int scale, int bitWidth) {
        final int mantissaBits = single ? 24 : 53;
        final int mantissaDigits = single ? 8 : 16;
        final int maxPrecision = bitWidth == 256 ? 76 : 38;
        if (scale < 0) return fromPositiveRealApprox(real, precision, scale, bitWidth);

        double limit = single ? pow10Float(precision - scale) : pow10Double(precision - scale);
        if (real > limit) throw overflow(real, precision, scale, bitWidth);

        // frexp: real = m * 2^e with m in [0.5, 1); mant = m * 2^M is an integer.
        double r = real;
        int adjust = 0;
        if (r < Double.MIN_NORMAL) { r *= 0x1p64; adjust = 64; }
        int e = Math.getExponent(r) + 1 - adjust;
        long mant = (long) Math.scalb(real, mantissaBits - e);
        int k = e - mantissaBits;

        BigInteger xv = BigInteger.valueOf(mant);
        if (k < 0) {
            int rightShiftBy = -k;
            int mulByTenTo = scale;
            int safeMulByTenTo = maxPrecision - mantissaDigits;
            if (mulByTenTo <= safeMulByTenTo) {
                xv = roundedRightShift(xv.multiply(pow10(mulByTenTo)), rightShiftBy);
            } else {
                xv = xv.multiply(pow10(safeMulByTenTo));
                mulByTenTo -= safeMulByTenTo;
                int mulStep = Math.max(1, maxPrecision - precision);
                int totalExp = 0;
                int totalShift = 0;
                while (mulByTenTo > 0 && rightShiftBy > 0) {
                    int exp = Math.min(mulByTenTo, mulStep);
                    totalExp += exp;
                    int bits = Math.min(rightShiftBy, ceilLog2PowerOfTen(totalExp) - totalShift);
                    totalShift += bits;
                    xv = roundedRightShift(xv, bits);
                    rightShiftBy -= bits;
                    xv = xv.multiply(pow10(exp));
                    mulByTenTo -= exp;
                }
                if (mulByTenTo > 0) xv = xv.multiply(pow10(mulByTenTo));
                if (rightShiftBy > 0) xv = roundedRightShift(xv, rightShiftBy);
            }
        } else {
            xv = xv.multiply(pow10(scale)).shiftLeft(k);
        }
        if (!fitsPrecision(xv, precision)) throw overflow(real, precision, scale, bitWidth);
        return xv;
    }

    private static BigInteger fromPositiveRealApprox(double real, int precision, int scale, int bitWidth) {
        double x = Math.rint(real * pow10Double(scale));
        double maxAbs = pow10Double(precision);
        if (x <= -maxAbs || x >= maxAbs) throw overflow(real, precision, scale, bitWidth);
        return new BigDecimal(x).toBigInteger();
    }

    /** {@code kCeilLog2PowersOfTen[n]}: ceil(log2(10^n)). */
    private static int ceilLog2PowerOfTen(int n) {
        return n == 0 ? 0 : pow10(n).bitLength();
    }

    /** Right shift of a non-negative value, rounded half to even. */
    private static BigInteger roundedRightShift(BigInteger x, int bits) {
        if (bits == 0) return x;
        BigInteger q = x.shiftRight(bits);
        BigInteger rem = x.subtract(q.shiftLeft(bits));
        int cmp = rem.compareTo(BigInteger.ONE.shiftLeft(bits - 1));
        if (cmp > 0 || (cmp == 0 && q.testBit(0))) q = q.add(BigInteger.ONE);
        return q;
    }

    private static IllegalArgumentException overflow(double real, int precision, int scale, int bitWidth) {
        return new IllegalArgumentException("Cannot convert " + cFormat(real) + " to " + decimalTypeName(bitWidth)
                + "(precision = " + precision + ", scale = " + scale + "): overflow");
    }

    /** How Arrow's message stream prints the real it could not convert ({@code %f}). */
    private static String cFormat(double x) {
        if (Double.isNaN(x)) return "nan";
        if (Double.isInfinite(x)) return x > 0 ? "inf" : "-inf";
        return String.format(Locale.ROOT, "%f", x);
    }
}
