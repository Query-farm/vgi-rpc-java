// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

/**
 * Worker identity for the standardized VGI landing surface.
 *
 * <p>The shared {@code landing.html} reads catalog metadata by speaking the VGI
 * protocol through the client bundle the worker serves beside it, so nothing
 * about the catalog belongs here. What the protocol has no method for — which
 * worker this is, what it is called, what version it runs — rides on the JSON
 * status document at {@code GET {prefix}/?format=json}.</p>
 *
 * @param name    worker name shown as the page heading
 * @param doc     one-line description shown under the heading
 * @param version worker version string shown in the footer
 */
public record LandingInfo(String name, String doc, String version) {}
