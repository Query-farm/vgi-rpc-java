// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

/**
 * Blob-vendor client: region-pinned, single-object pre-signed URLs.
 *
 * <p>The <em>blob vendor</em> is a small HTTP service that holds the
 * object-storage credentials so workers never do. A worker asks it for a
 * pre-signed URL for one object of a declared size and SHA-256, uploads
 * straight to storage with that URL, and hands a download URL (or an object
 * id) to whoever needs the bytes. The storage enforces the signed
 * {@code Content-Length} and {@code x-amz-checksum-sha256}, so whatever sits at
 * the object's key is exactly the declared bytes.</p>
 *
 * <ul>
 *   <li>{@link farm.query.vgirpc.vendor.VendorStorage}: an
 *       {@link farm.query.vgirpc.external.ExternalStorage} (server-side
 *       externalization of large results, purpose {@code rpc}) and an
 *       {@link farm.query.vgirpc.external.UploadUrlProvider} (upload URLs for
 *       large client requests).</li>
 *   <li>{@link farm.query.vgirpc.vendor.VendorBlobClient}: raw put/get/delete
 *       of byte blobs (purpose {@code fs}), verified against a
 *       {@link farm.query.vgirpc.vendor.BlobRef} on read.</li>
 *   <li>{@link farm.query.vgirpc.vendor.BlobVendor}: URL validators pinned to
 *       the vendor's storage hosts and environment-driven configs.</li>
 * </ul>
 *
 * <p>Wire-compatible with vgi-rpc-python's {@code vgi_rpc.vendor}, which is
 * the reference.</p>
 */
package farm.query.vgirpc.vendor;
