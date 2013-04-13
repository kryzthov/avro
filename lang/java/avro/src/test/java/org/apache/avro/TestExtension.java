package org.apache.avro;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.Test;

public class TestExtension {
  private static <E> List<E> list(E... elements) {
    final List<E> list = new ArrayList<E>();
    Collections.addAll(list, elements);
    return list;
  }

  private static final Schema LONG = Schema.create(Schema.Type.LONG);
  private static final Schema STRING = Schema.create(Schema.Type.STRING);
  static {
    STRING.addProp("avro.java.string", "String");
  }
  private static final Schema RECORD = Schema.createRecord("RecordWithExtension", null, null, false);
  static {
    RECORD.setFields(list(new Field("ext", Schema.createExtension(STRING), null, null)));
  }

  private static final Schema RECORD_EXT = Schema.createRecord("Extension", null, null, false);
  static {
    RECORD_EXT.setFields(list(new Field("long", LONG, null, null)));
  }

  @Test
  public void test() throws Exception {
    final GenericRecord recordExt = new GenericRecordBuilder(RECORD_EXT)
        .set("long", 0x1122334455667788L)
        .build();

    final GenericRecord rec = new GenericRecordBuilder(RECORD)
        .set("ext", recordExt)
        .build();

    System.out.println(rec);

    final ByteArrayOutputStream ostream = new ByteArrayOutputStream();
    final Encoder out = EncoderFactory.get().binaryEncoder(ostream, null);
    final GenericDatumWriter<Object> writer = new GenericDatumWriter<Object>(RECORD)
        .setSchemaResolver(new JsonSchemaResolver());
    writer.write(rec, out);
    out.flush();
    for (byte b : ostream.toByteArray()) {
      System.out.printf("%02x:", b);
    }
    System.out.println();
    System.out.println(new String(ostream.toByteArray()));

    final Decoder in = DecoderFactory.get().binaryDecoder(ostream.toByteArray(), null);
    final GenericDatumReader<Object> reader = new GenericDatumReader<Object>(RECORD)
        .setSchemaResolver(new JsonSchemaResolver());
    final Object decoded = reader.read(null, in);
    System.out.println(decoded);
  }

}
