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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSchemaJsonParser {
  private static final Logger LOG = LoggerFactory.getLogger(TestSchemaJsonParser.class);

  @Test
  public void testParseSimple() {
    final SchemaJsonParser parser = new SchemaJsonParser();
    LOG.info("{}", parser.parse("\"int\""));
    LOG.info("{}", parser.parse("\"string\""));
    LOG.info("{}", parser.parse("[\"string\"]"));
  }

  @Test
  public void testParse() throws Exception {
    final SchemaJsonParser parser = new SchemaJsonParser();
    LOG.info("{}", parser.parse(getResource("test-recursive-record.avsc")));
  }

  private static String getResource(String name) throws IOException {
    final InputStream istream =
        TestSchemaJsonParser.class.getClassLoader().getResourceAsStream(name);
    final LineNumberReader reader = new LineNumberReader(new InputStreamReader(istream));
    final StringBuilder sb = new StringBuilder();
    while (true) {
      final String line = reader.readLine();
      if (line == null) {
        break;
      }
      sb.append(line);
    }
    reader.close();
    return sb.toString();
  }
}
