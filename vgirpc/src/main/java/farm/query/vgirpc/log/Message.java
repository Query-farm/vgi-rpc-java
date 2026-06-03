// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import farm.query.vgirpc.wire.Metadata;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A structured log/error record carried as zero-row batch metadata
 * ({@code vgi_rpc.log_level} / {@code vgi_rpc.log_message} /
 * {@code vgi_rpc.log_extra}).
 */
public final class Message {

    /** Maximum traceback length retained by {@link #fromException(Throwable)} before truncation. */
    public static final int MAX_TRACEBACK_CHARS = 16_000;

    private static final ObjectMapper JSON = new ObjectMapper();

    private final Level level;
    private final String message;
    private final Map<String, Object> extra;

    /**
     * @param level severity
     * @param message human-readable message text
     * @param extra structured fields serialized to {@code vgi_rpc.log_extra}, or {@code null}
     */
    public Message(Level level, String message, Map<String, Object> extra) {
        this.level = level;
        this.message = message;
        this.extra = extra;
    }

    /**
     * @param level severity
     * @param message human-readable message text
     */
    public Message(Level level, String message) {
        this(level, message, null);
    }

    /** @return the severity level. */
    public Level level() { return level; }
    /** @return the message text. */
    public String message() { return message; }
    /** @return the structured extra fields, or {@code null}. */
    public Map<String, Object> extra() { return extra; }

    /** Build an {@link Level#EXCEPTION}-level message with structured extras. */
    public static Message exception(String msg, Map<String, Object> extra) {
        return new Message(Level.EXCEPTION, msg, extra);
    }
    /** Build an {@link Level#ERROR}-level message. */
    public static Message error(String msg) { return new Message(Level.ERROR, msg, null); }
    /** Build a {@link Level#WARN}-level message. */
    public static Message warn(String msg) { return new Message(Level.WARN, msg, null); }
    /** Build an {@link Level#INFO}-level message. */
    public static Message info(String msg) { return new Message(Level.INFO, msg, null); }
    /** Build a {@link Level#DEBUG}-level message. */
    public static Message debug(String msg) { return new Message(Level.DEBUG, msg, null); }
    /** Build a {@link Level#TRACE}-level message. */
    public static Message trace(String msg) { return new Message(Level.TRACE, msg, null); }

    /**
     * Write this message's level/message/extra onto a metadata map.
     *
     * @param base existing metadata to copy and extend, or {@code null} for a fresh map
     * @return a new map containing {@code base} plus the log metadata keys
     */
    public Map<String, String> addToMetadata(Map<String, String> base) {
        Map<String, String> result = base != null ? new LinkedHashMap<>(base) : new LinkedHashMap<>();
        result.put(Metadata.LOG_LEVEL, level.name());
        result.put(Metadata.LOG_MESSAGE, message);
        if (extra != null && !extra.isEmpty()) {
            try {
                result.put(Metadata.LOG_EXTRA, JSON.writeValueAsString(extra));
            } catch (Exception ignore) {
                result.put(Metadata.LOG_EXTRA, "{}");
            }
        }
        return result;
    }

    /**
     * Build an {@link Level#EXCEPTION} message from a thrown exception: captures
     * the (truncated) stack trace and a Python-compatible {@code exception_type}
     * into the extras so cross-language clients can match by type name.
     *
     * @param t the exception
     * @return the corresponding error message
     */
    public static Message fromException(Throwable t) {
        StringWriter sw = new StringWriter();
        try (PrintWriter pw = new PrintWriter(sw)) {
            t.printStackTrace(pw);
        }
        String tb = sw.toString();
        if (tb.length() > MAX_TRACEBACK_CHARS) {
            tb = tb.substring(0, MAX_TRACEBACK_CHARS) + "\n... <traceback truncated>";
        }
        Map<String, Object> extra = new LinkedHashMap<>();
        String typeName = mapExceptionType(t);
        extra.put("exception_type", typeName);
        extra.put("exception_message", t.getMessage() != null ? t.getMessage() : "");
        extra.put("traceback", tb);
        // Type name lives in the wire extras (and clients read it from there
        // as RpcError.error_type). Keeping it as a prose prefix duplicates it
        // for humans — readers see "VGI Worker Exception: ValueError: <msg>"
        // and the type adds no information they don't already have. Use the
        // type name only as a fallback when there is no message.
        String summary = t.getMessage() != null && !t.getMessage().isEmpty()
                ? t.getMessage() : typeName;
        return new Message(Level.EXCEPTION, summary, extra);
    }

    /**
     * Map a Java exception class to a Python-equivalent error type name so that
     * cross-language conformance tests can match by name.
     */
    private static String mapExceptionType(Throwable t) {
        if (t instanceof IllegalArgumentException) return "ValueError";
        if (t instanceof UnsupportedOperationException) return "NotImplementedError";
        if (t instanceof ClassCastException) return "TypeError";
        if (t instanceof NullPointerException) return "TypeError";
        if (t instanceof java.util.NoSuchElementException) return "KeyError";
        if (t instanceof RuntimeException && t.getClass() == RuntimeException.class) return "RuntimeError";
        // Pass-through for thrown RpcError (use its preserved type)
        return t.getClass().getSimpleName();
    }
}
