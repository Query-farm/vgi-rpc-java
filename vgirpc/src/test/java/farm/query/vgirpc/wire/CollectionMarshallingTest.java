// Copyright 2026 Query Farm LLC - https://query.farm
package farm.query.vgirpc.wire;
import farm.query.vgirpc.marshal.Marshalling;
import org.apache.arrow.vector.types.pojo.*;
import org.junit.jupiter.api.Test;
import java.io.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class CollectionMarshallingTest {
    public record BinaryRecord(
        @farm.query.vgirpc.schema.ArrowField(farm.query.vgirpc.schema.ArrowFieldType.DICT_INT32_BINARY) byte[] bytes,
        @farm.query.vgirpc.schema.Nullable @farm.query.vgirpc.schema.ArrowField(farm.query.vgirpc.schema.ArrowFieldType.DICT_INT32_BINARY) byte[] optional
    ) implements farm.query.vgirpc.schema.ArrowSerializableRecord {}

    @Test void binaryDictionaryRecordsPreserveNullEmptyAndNonUtf8() throws Exception {
        var schema=farm.query.vgirpc.schema.SchemaDerivation.schemaForRecord(BinaryRecord.class);
        assertEquals(32,schema.getFields().getFirst().getDictionary().getIndexType().getBitWidth());
        var rows=new ArrayList<Map<String,Object>>();
        for(byte[] optional:new byte[][] {null,new byte[0],new byte[]{(byte)255,0,(byte)128}}) {
            var row=new HashMap<String,Object>();row.put("bytes",new byte[]{(byte)255,0});row.put("optional",optional);rows.add(row);
        }
        var output=new ByteArrayOutputStream();
        try(var encoded=Marshalling.encodeRowsForWire(schema,rows,Allocators.root());var writer=new IpcStreamWriter(output)) {
            assertEquals(1,encoded.provider().lookup(schema.getFields().getFirst().getDictionary().getId()).getVector().getValueCount());
            writer.writeBatch(encoded.root(),null,encoded.provider());
        }
        try(var reader=new IpcStreamReader(new ByteArrayInputStream(output.toByteArray()),Allocators.root())) {
            reader.readNextBatch();
            for(int i=0;i<rows.size();i++)try(var row=reader.root().slice(i,1)) {
                var decoded=Marshalling.decodeRow(row,reader.dictionaryProvider(),reader.wireSchema());
                assertArrayEquals((byte[])rows.get(i).get("bytes"),(byte[])decoded.get("bytes"));
                assertArrayEquals((byte[])rows.get(i).get("optional"),(byte[])decoded.get("optional"));
            }
        }
        var value=new BinaryRecord(new byte[]{(byte)255,0},new byte[0]);
        var back=farm.query.vgirpc.marshal.RecordCodec.deserializeFromBytes(farm.query.vgirpc.marshal.RecordCodec.serializeToBytes(value),BinaryRecord.class);
        assertArrayEquals(value.bytes(),back.bytes()); assertArrayEquals(value.optional(),back.optional());
    }

    @Test void multipleRowsPreserveUnsignedStructChildrenAndDictionaryValues() throws Exception {
        var uint=new Field("version",FieldType.notNullable(new ArrowType.Int(64,false)),List.of());
        var child=new Field("item",FieldType.notNullable(ArrowType.Struct.INSTANCE),List.of(uint));
        var list=new Field("capabilities",FieldType.notNullable(ArrowType.List.INSTANCE),List.of(child));
        var dict=new Field("kind",new FieldType(false,ArrowType.Utf8.INSTANCE,new DictionaryEncoding(4,false,new ArrowType.Int(16,true))),List.of());
        var schema=new Schema(List.of(dict,list));
        var rows=List.<Map<String,Object>>of(Map.of("kind","first","capabilities",List.of(Map.of("version",-1L))),Map.of("kind","second","capabilities",List.of()),Map.of("kind","first","capabilities",List.of(Map.of("version",7L))));
        var output=new ByteArrayOutputStream();
        try(var encoded=Marshalling.encodeRowsForWire(schema,rows,Allocators.root());var writer=new IpcStreamWriter(output)){writer.writeBatch(encoded.root(),null,encoded.provider());writer.writeEos();writer.writeEos();}
        var input=new ByteArrayInputStream(output.toByteArray());
        try(var reader=new IpcStreamReader(input,Allocators.root())){
            assertEquals(schema,reader.wireSchema());assertNotNull(reader.readNextBatch());assertEquals(3,reader.root().getRowCount());
            for(int i=0;i<rows.size();i++)try(var row=reader.root().slice(i,1)){assertEquals(rows.get(i),Marshalling.decodeRow(row,reader.dictionaryProvider(),reader.wireSchema()));}
            assertNull(reader.readNextBatch());assertEquals(0,input.available(),"explicit EOS and close must emit exactly one marker");
        }
    }
}
