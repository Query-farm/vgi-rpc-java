// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.log;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.Map;

public final class Message {

    public static final int MAX_TRACEBACK_CHARS = 16_000;

    private static final ObjectMapper JSON = new ObjectMapper();

    private final Level level;
    private final String message;
    private final Map<String, Object> extra;

    public Message(Level level, String message, Map<String, Object> extra) {
        this.level = level;
        this.message = message;
        this.extra = extra;
    }

    public Message(Level level, String message) {
        this(level, message, null);
    }

    public Level level() { return level; }
    public String message() { return message; }
    public Map<String, Object> extra() { return extra; }

    public static Message exception(String msg, Map<String, Object> extra) {
        return new Message(Level.EXCEPTION, msg, extra);
    }
    public static Message error(String msg) { return new Message(Level.ERROR, msg, null); }
    public static Message warn(String msg) { return new Message(Level.WARN, msg, null); }
    public static Message info(String msg) { return new Message(Level.INFO, msg, null); }
    public static Message debug(String msg) { return new Message(Level.DEBUG, msg, null); }
    public static Message trace(String msg) { return new Message(Level.TRACE, msg, null); }

    public Map<String, String> addToMetadata(Map<String, String> base) {
        Map<String, String> result = base != null ? new LinkedHashMap<>(base) : new LinkedHashMap<>();
        result.put("vgi_rpc.log_level", level.name());
        result.put("vgi_rpc.log_message", message);
        if (extra != null && !extra.isEmpty()) {
            try {
                result.put("vgi_rpc.log_extra", JSON.writeValueAsString(extra));
            } catch (Exception ignore) {
                result.put("vgi_rpc.log_extra", "{}");
            }
        }
        return result;
    }

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
        String summary = typeName + ": " + (t.getMessage() != null ? t.getMessage() : "");
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
