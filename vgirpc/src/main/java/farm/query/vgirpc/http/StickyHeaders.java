// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

/** Wire constants for the HTTP sticky-sessions feature. */
public final class StickyHeaders {
    private StickyHeaders() {}

    public static final String SESSION         = "VGI-Session";
    public static final String SESSION_ACCEPT  = "VGI-Session-Accept";
    public static final String SESSION_CLOSE   = "VGI-Session-Close";
    public static final String STICKY_ENABLED  = "VGI-Sticky-Enabled";
    public static final String STICKY_TTL      = "VGI-Sticky-Default-TTL";
    public static final String STICKY_ECHO     = "VGI-Sticky-Echo-Headers";
    public static final String ECHO_PREFIX     = "VGI-Echo-";

    /** Session DELETE endpoint path (relative to the servlet base). */
    public static final String SESSION_PATH    = "__session__";
    /** Test-only drain admin endpoint. */
    public static final String TEST_DRAIN_PATH = "__test_drain__";
}
