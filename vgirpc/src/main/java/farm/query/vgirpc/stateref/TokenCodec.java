// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.stateref;

import com.github.luben.zstd.Zstd;

/**
 * The standard token codec (STATE_REF_V1.md §7): {@code codec (1 byte) || body}.
 *
 * <p>Every sealed payload — the cursor and call tokens, and the inline and stored
 * {@link StateRef} payloads — is compressed, then sealed. Once sealed, a token is
 * ciphertext and no outer codec can find its redundancy.</p>
 *
 * <table>
 *   <caption>Codec tags</caption>
 *   <tr><th>Tag</th><th>Body</th></tr>
 *   <tr><td>{@code 0x00}</td><td>raw: used whenever compression does not make the payload smaller</td></tr>
 *   <tr><td>{@code 0x01}</td><td>a zstd frame (level 3)</td></tr>
 *   <tr><td>{@code 0x02}</td><td>raw DEFLATE: reserved for runtimes without zstd; this SDK never
 *       writes it, so (per the spec's "a reader accepts every codec its own writer can emit") it
 *       refuses it like any other unknown tag</td></tr>
 * </table>
 */
public final class TokenCodec {

    /** Raw body. */
    public static final byte CODEC_RAW = 0x00;
    /** zstd frame. */
    public static final byte CODEC_ZSTD = 0x01;

    /** Decompression bound for tokens: 64 MiB. */
    public static final long MAX_TOKEN_PLAINTEXT_BYTES = 64L << 20;

    /** Matches the reference: at token sizes level 3 is as fast as level 1 and slightly smaller. */
    private static final int ZSTD_LEVEL = 3;

    private TokenCodec() {}

    /** A payload whose codec tag is unknown or whose body does not decode. */
    public static final class TokenCodecException extends IllegalArgumentException {
        private static final long serialVersionUID = 1L;

        TokenCodecException(String message) {
            super(message);
        }
    }

    /**
     * Compress {@code plaintext} when that makes it smaller, and tag the codec.
     *
     * @param plaintext the bytes to pack
     * @return {@code codec || body}; never longer than {@code plaintext.length + 1}
     */
    public static byte[] pack(byte[] plaintext) {
        byte[] packed = Zstd.compress(plaintext, ZSTD_LEVEL);
        if (packed.length < plaintext.length) return prefixed(CODEC_ZSTD, packed);
        return prefixed(CODEC_RAW, plaintext);
    }

    /**
     * Reverse {@link #pack}, refusing more than {@code maxBytes} of output.
     *
     * <p>A zstd frame must declare its content size, and that size is checked against
     * the bound before anything is allocated.</p>
     *
     * @param data {@code codec || body}
     * @param maxBytes the decompression bound
     * @return the plaintext
     * @throws TokenCodecException on an empty payload, an unknown codec, or a body that
     *     does not decompress within the bound
     */
    public static byte[] unpack(byte[] data, long maxBytes) {
        if (data == null || data.length == 0) throw new TokenCodecException("empty payload");
        byte[] body = new byte[data.length - 1];
        System.arraycopy(data, 1, body, 0, body.length);
        switch (data[0]) {
            case CODEC_RAW:
                if (body.length > maxBytes) throw new TokenCodecException("payload exceeds " + maxBytes + " bytes");
                return body;
            case CODEC_ZSTD: {
                long size = Zstd.getFrameContentSize(body);
                if (size < 0 || size > maxBytes || size > Integer.MAX_VALUE - 8) {
                    throw new TokenCodecException("zstd frame size is unknown or over the bound");
                }
                byte[] out = new byte[(int) size];
                long ret = Zstd.decompress(out, body);
                if (Zstd.isError(ret) || ret != size) throw new TokenCodecException("zstd frame does not decode");
                return out;
            }
            default:
                throw new TokenCodecException(String.format("unknown codec 0x%02x", data[0] & 0xff));
        }
    }

    private static byte[] prefixed(byte tag, byte[] body) {
        byte[] out = new byte[1 + body.length];
        out[0] = tag;
        System.arraycopy(body, 0, out, 1, body.length);
        return out;
    }
}
