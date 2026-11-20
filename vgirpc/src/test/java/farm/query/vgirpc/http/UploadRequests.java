// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.http;

import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamWriter;
import farm.query.vgirpc.wire.Metadata;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;

/** Raw {@code __upload_url__/init} request bodies for the server-side tests. */
final class UploadRequests {

    private UploadRequests() {}

    /** A valid-looking digest. */
    static final String SHA = "a".repeat(64);

    /** A v2 body with {@code n} entries of {@code size} bytes and {@code sha}. */
    static byte[] v2(int n, long size, String sha) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (VectorSchemaRoot root = VectorSchemaRoot.create(HttpServer.UPLOAD_URL_PARAMS_SCHEMA, Allocators.root());
             IpcStreamWriter writer = new IpcStreamWriter(out)) {
            UnionListWriter lw = ((ListVector) root.getVector("uploads")).getWriter();
            lw.setPosition(0);
            lw.startList();
            for (int i = 0; i < n; i++) {
                BaseWriter.StructWriter sw = lw.struct();
                sw.start();
                sw.bigInt("size").writeBigInt(size);
                sw.varChar("sha256").writeVarChar(new Text(sha));
                sw.end();
            }
            lw.endList();
            root.setRowCount(1);
            writer.writeBatch(root, meta());
        }
        return out.toByteArray();
    }

    /** A v2 body with {@code n} well-formed entries. */
    static byte[] v2(int n) throws Exception {
        return v2(n, 16, SHA);
    }

    /** The retired v1 body: one {@code count} column. */
    static byte[] v1Count(long count) throws Exception {
        Schema schema = new Schema(List.of(new Field("count", FieldType.nullable(new ArrowType.Int(64, true)), null)));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root());
             IpcStreamWriter writer = new IpcStreamWriter(out)) {
            BigIntVector counts = (BigIntVector) root.getVector("count");
            counts.allocateNew(1);
            counts.setSafe(0, count);
            root.setRowCount(1);
            writer.writeBatch(root, meta());
        }
        return out.toByteArray();
    }

    private static Map<String, String> meta() {
        return Map.of(Metadata.RPC_METHOD, HttpServer.UPLOAD_URL_METHOD,
                Metadata.REQUEST_VERSION_KEY, Metadata.REQUEST_VERSION);
    }
}
