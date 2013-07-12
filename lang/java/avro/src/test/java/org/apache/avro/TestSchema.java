/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSchema {
  private static final Logger LOG = LoggerFactory.getLogger(TestSchema.class);

  @Test
  public void testSplitSchemaBuild() {
    Schema s = SchemaBuilder
       .recordType("HandshakeRequest")
       .namespace("org.apache.avro.ipc")
       .unionType("clientProtocol", SchemaBuilder.unionType(
           SchemaBuilder.NULL,
           SchemaBuilder.STRING)
           .build())
       .unionType("meta", SchemaBuilder.unionType(
           SchemaBuilder.NULL,
           SchemaBuilder.mapType(SchemaBuilder.BYTES)
             .build())
           .build())
       .build();

    String schemaString = s.toString();
    final int mid = schemaString.length() / 2;

    Schema parsedStringSchema = new org.apache.avro.Schema.Parser().parse(s.toString());
    Schema parsedArrayOfStringSchema =
      new org.apache.avro.Schema.Parser().parse
      (schemaString.substring(0, mid), schemaString.substring(mid));
    assertNotNull(parsedStringSchema);
    assertNotNull(parsedArrayOfStringSchema);
    assertEquals(parsedStringSchema.toString(), parsedArrayOfStringSchema.toString());
  }

  /** Tests parsing a union schema with properties. */
  @Test
  public void testUnionSchemaWithProperties() {
    final List<Schema> types = new ArrayList<Schema>();
    types.add(Schema.create(Schema.Type.NULL));
    types.add(Schema.create(Schema.Type.INT));
    final Schema union = Schema.createUnion(types);
    union.addProp("the_property", "the_value");

    final String json = union.toString();
    LOG.info("JSON serialized: {}", json);
    final Schema parsed = new Schema.Parser().parse(json);
    assertEquals("the_value", parsed.getProp("the_property"));
    assertEquals(json, parsed.toString());

    assertEquals(union, parsed);
  }

}
