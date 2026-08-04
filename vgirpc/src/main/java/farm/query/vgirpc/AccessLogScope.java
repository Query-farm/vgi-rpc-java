// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Per-request scope that measures what actually crossed the network and defers
 * access-log emission until the final response body exists.
 *
 * <p>A record written by the dispatch hook is written too early to know what the
 * response weighed: response compression runs after the handler returns, so a
 * record emitted at dispatch time can only ever report the uncompressed body —
 * the wrong number for anything that costs money. The transport therefore opens
 * a scope around the request, the hook parks its records in it, and the scope
 * emits once the encoded body has been measured.
 *
 * <p>Three byte figures answer three different questions and must not be
 * conflated: {@code request_bytes}/{@code response_bytes} are what crossed the
 * wire after compression; {@link CallStatistics}' {@code input_bytes}/
 * {@code output_bytes} are logical Arrow buffers, routinely orders of magnitude
 * larger; {@code externalized_bytes} never touch the HTTP body at all and are
 * frequently the largest of the three.
 *
 * <p>Transports that install no scope — pipe, unix socket, raw TCP — keep
 * logging inline, so the immediate-versus-deferred choice is made in exactly one
 * place. The cost of deferring is that a crash between the handler and the
 * response loses that request's records; the alternative is a permanently wrong
 * number.
 *
 * <p>Bound to the dispatching thread. Not safe for use across threads.
 */
public final class AccessLogScope implements AutoCloseable {

    private static final ThreadLocal<AccessLogScope> CURRENT = new ThreadLocal<>();

    private final String requestId;
    private long requestBytes = -1;
    private long responseBytes = -1;
    private long externalizedBytes;
    private int httpStatus;
    private List<Deferred> deferred;

    private AccessLogScope(String requestId) {
        this.requestId = requestId == null ? "" : requestId;
    }

    /** One parked record and the hook that will write it. */
    private record Deferred(AccessLogHook hook, Map<String, Object> record) {}

    /**
     * Open a scope on the current thread, replacing any scope already installed.
     *
     * @param requestId per-request correlation id the records should carry; may be {@code null}
     * @return the scope; close it (ideally via try-with-resources) to emit the parked records
     */
    public static AccessLogScope open(String requestId) {
        AccessLogScope scope = new AccessLogScope(requestId);
        CURRENT.set(scope);
        return scope;
    }

    /**
     * The scope installed on the current thread.
     *
     * @return the current scope, or {@code null} when the transport installs none
     */
    public static AccessLogScope current() {
        return CURRENT.get();
    }

    /**
     * Record the on-wire size of the request body, <em>before</em> decompression —
     * what the peer actually sent. No-op when no scope is installed.
     *
     * @param bytes number of bytes read off the socket
     */
    public static void recordRequestBytes(long bytes) {
        AccessLogScope scope = CURRENT.get();
        if (scope != null) scope.requestBytes = bytes;
    }

    /**
     * Record the on-wire size of the response body, <em>after</em> compression.
     * No-op when no scope is installed.
     *
     * @param bytes number of bytes written to the socket
     */
    public static void recordResponseBytes(long bytes) {
        AccessLogScope scope = CURRENT.get();
        if (scope != null) scope.responseBytes = bytes;
    }

    /**
     * Add bytes uploaded to external storage during this call. Counted at the
     * single upload choke point so a new upload path cannot drift from the
     * total. No-op when no scope is installed.
     *
     * @param bytes size of the payload handed to {@code ExternalStorage.upload}
     */
    public static void countExternalized(long bytes) {
        AccessLogScope scope = CURRENT.get();
        if (scope != null) scope.externalizedBytes += bytes;
    }

    /**
     * Set the response status the records should report.
     *
     * @param status HTTP status code of the response
     */
    public void httpStatus(int status) {
        this.httpStatus = status;
    }

    /** @return the request correlation id, or {@code ""} when the transport minted none */
    String requestId() {
        return requestId;
    }

    /** @return on-wire request size, or a negative value when it was never measured */
    long requestBytes() {
        return requestBytes;
    }

    /** @return bytes uploaded to external storage so far during this call */
    long externalizedBytes() {
        return externalizedBytes;
    }

    /** Park a record until the response has been measured. */
    void defer(AccessLogHook hook, Map<String, Object> record) {
        if (deferred == null) deferred = new ArrayList<>(2);
        deferred.add(new Deferred(hook, record));
    }

    /** Stamp the egress figures known only now, emit, and uninstall the scope. */
    @Override
    public void close() {
        CURRENT.remove();
        if (deferred == null) return;
        for (Deferred d : deferred) {
            if (responseBytes >= 0) d.record().put("response_bytes", responseBytes);
            // The schema constrains this to a real status code, so a response
            // that never got one is better left unreported than guessed at.
            if (httpStatus >= 100 && httpStatus <= 599) d.record().put("http_status", httpStatus);
            d.hook().write(d.record());
        }
        deferred = null;
    }
}
