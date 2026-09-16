// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

/**
 * One pre-signed upload slot vended by a worker's {@code __upload_url__}
 * endpoint: where a client PUTs a payload too large to send inline, and the URL
 * it then names in the pointer batch it sends instead.
 *
 * <p>The two URLs are separate because they are not interchangeable — the
 * upload URL usually carries a signature scoped to {@code PUT} and expires
 * quickly, while the download URL is what the <em>worker</em> will fetch. A
 * client that echoes the upload URL back as the location hands the worker a
 * credential it cannot use.</p>
 *
 * <p>The counterpart of {@code farm.query.vgirpc.external.UploadUrlProvider.UploadUrl}
 * on the server side, with the expiry flattened to whole Unix seconds: it
 * crosses the wire as {@code timestamp[us, UTC]}, but sub-second precision on a
 * deadline minutes away is noise, and an integer is what every other port's
 * client surface reports.</p>
 *
 * @param uploadUrl the URL to PUT the payload to
 * @param downloadUrl the URL to name as {@code vgi_rpc.location} afterwards
 * @param expiresAtUnixSeconds when the slot stops being usable, in seconds
 *     since the Unix epoch
 */
public record UploadUrl(String uploadUrl, String downloadUrl, long expiresAtUnixSeconds) {}
