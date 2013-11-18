package org.apache.avro.parser.idl

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.seqAsJavaListConverter

import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericData
import org.junit.Assert
import org.slf4j.LoggerFactory

import junit.framework.TestCase

class TestAvroValueParser
    extends TestCase {

  final val Log = LoggerFactory.getLogger(classOf[TestAvroValueParser])

  private final val StringSchema = Schema.create(Type.STRING)
  private val schemaParser = new AvroSchemaParser()

  def testPrimitives(): Unit = {
    Assert.assertEquals(null, AvroValueParser.parse("null", schemaParser.parse("null")))
    Assert.assertEquals(true, AvroValueParser.parse("true", Schema.create(Type.BOOLEAN)))
    Assert.assertEquals(false, AvroValueParser.parse("false", Schema.create(Type.BOOLEAN)))
    Assert.assertEquals(1, AvroValueParser.parse("1", Schema.create(Type.INT)))
    Assert.assertEquals(-1L, AvroValueParser.parse("-1", Schema.create(Type.LONG)))
    Assert.assertEquals(-1L, AvroValueParser.parse("-1L", Schema.create(Type.LONG)))
    Assert.assertEquals(3.14f, AvroValueParser.parse("3.14", Schema.create(Type.FLOAT)))
    Assert.assertEquals(3.14f, AvroValueParser.parse("3.14f", Schema.create(Type.FLOAT)))
    Assert.assertEquals(3.14d, AvroValueParser.parse("3.14", Schema.create(Type.DOUBLE)))
    Assert.assertEquals(3.14d, AvroValueParser.parse("3.14d", Schema.create(Type.DOUBLE)))
  }

  def testArray(): Unit = {
    val intArraySchema = schemaParser.parse("array<int>")
    val list = List(1, 2, 3).asJava
    val strs = List(
        """[1, 2, 3]""",
        """[1, 2, 3,]""",
        """[1 2 3]""",
        """[1; 2; 3]""",
        """[1; 2; 3;]"""
    )
    for (str <- strs) {
      Log.info("Parsing '{}'", str)
      val parsed = AvroValueParser.parse(str, intArraySchema)
      Log.info("Parsed '{}' as {}", str, parsed)
      Assert.assertEquals(list, parsed)
      Log.info(AvroValue.toString(parsed, intArraySchema))
    }
  }

  def testMap(): Unit = {
    val intMapSchema = schemaParser.parse("map<int>")
    val map = Map("a" -> 1, "b" -> 2).asJava
    val strs = List(
        """{"a": 1, "b": 2}""",
        """{"a": 1; "b": 2;}""",
        """{"a": 1 "b": 2}"""
    )
    for (str <- strs) {
      Log.info("Parsing '{}'", str)
      val parsed = AvroValueParser.parse(str, intMapSchema)
      Log.info("Parsed '{}' into {}", str, parsed)
      Assert.assertEquals(map, parsed)
      Log.info(AvroValue.toString(parsed, intMapSchema))
    }
  }

  def testRecords(): Unit = {
    val schema = schemaParser.parse("record ns.X { int x; string y }")
    val record =
        AvroValueParser.parse(""" ns.X { x=-314159; y="hello" } """, schema)
        .asInstanceOf[GenericData.Record]
    Assert.assertEquals(schema, record.getSchema)
    Assert.assertEquals(-314159, record.get("x"))
    Assert.assertEquals("hello", record.get("y"))
  }

  def testUnion(): Unit = {
    val schema = schemaParser.parse("union { null, int }")
    Assert.assertEquals(null, AvroValueParser.parse("null", schema))
    Assert.assertEquals(15, AvroValueParser.parse("15", schema))

    Assert.assertEquals("null", AvroValue.toString(null, schema))
    Assert.assertEquals("15", AvroValue.toString(15, schema))
  }

  def testString(): Unit = {
    Assert.assertEquals("", AvroValueParser.parse(""" "" """, StringSchema))
    Assert.assertEquals("", AvroValueParser.parse(""" '' """, StringSchema))
    Assert.assertEquals("", AvroValueParser.parse(" \"\"\"\"\"\" ", StringSchema))

    Assert.assertEquals("hello", AvroValueParser.parse(""" "hello" """, StringSchema))
    Assert.assertEquals("hello", AvroValueParser.parse(""" 'hello' """, StringSchema))
    Assert.assertEquals("hello", AvroValueParser.parse(" \"\"\"hello\"\"\" ", StringSchema))

    Assert.assertEquals("hel'lo", AvroValueParser.parse(""" "hel'lo" """, StringSchema))
    Assert.assertEquals("hel'lo", AvroValueParser.parse(""" 'hel\'lo' """, StringSchema))
    Assert.assertEquals("hel'lo", AvroValueParser.parse(" \"\"\"hel\'lo\"\"\" ", StringSchema))

    Assert.assertEquals("hel\"lo", AvroValueParser.parse(""" "hel\"lo" """, StringSchema))
    Assert.assertEquals("hel\"lo", AvroValueParser.parse(""" 'hel"lo' """, StringSchema))
    Assert.assertEquals("hel\"lo", AvroValueParser.parse(" \"\"\"hel\"lo\"\"\" ", StringSchema))

    Assert.assertEquals("hel\nlo", AvroValueParser.parse(" \"\"\"hel\nlo\"\"\" ", StringSchema))
  }

  def testDoubleQuoteString(): Unit = {
    val str: String =
        AvroValueParser.parse(""" "Here is a \"double quoted\" word." """, StringSchema)
        .asInstanceOf[String]

    Log.info("str=[%s]".format(str))
    Assert.assertEquals("""Here is a "double quoted" word.""", str)
    val reserialized = AvroValue.toString(str, StringSchema)
    Log.info("reserialized=[%s]".format(reserialized))
    Assert.assertEquals(""""Here is a \"double quoted\" word."""", reserialized)
    val str2 = AvroValueParser.parse(reserialized, StringSchema)
    Log.info("str2=[%s]".format(str2))
    Assert.assertEquals(str, str2)
  }

  def testSingleQuoteString(): Unit = {
    val str: String =
        AvroValueParser.parse(""" 'Here is a \'double quoted\' word.' """, StringSchema)
        .asInstanceOf[String]
    Log.info("str=[%s]".format(str))
    Assert.assertEquals("""Here is a 'double quoted' word.""", str)
    val reserialized = AvroValue.toString(str, StringSchema)
    Log.info("reserialized=[%s]".format(reserialized))
    Assert.assertEquals(""""Here is a 'double quoted' word."""", reserialized)
    val str2 = AvroValueParser.parse(reserialized, StringSchema)
    Log.info("str2=[%s]".format(str2))
    Assert.assertEquals(str, str2)
  }

  def testLayout(): Unit = {
    val layoutSchemas = schemaParser.parseSequence("""
      |enum org.kiji.schema.avro.CompressionType { NONE, GZ, LZO, SNAPPY }
      |enum org.kiji.schema.avro.SchemaType { INLINE, CLASS, COUNTER, AVRO, RAW_BYTES, PROTOBUF }
      |enum org.kiji.schema.avro.SchemaStorage { HASH, UID, FINAL }
      |record org.kiji.schema.avro.AvroSchema {
      |  union { null, long } uid = null;
      |  union { null, string } json = null;
      |}
      |enum org.kiji.schema.avro.AvroValidationPolicy { STRICT, DEVELOPER, SCHEMA_1_0, NONE }
      |record org.kiji.schema.avro.CellSchema {
      |  org.kiji.schema.avro.SchemaStorage storage = org.kiji.schema.avro.SchemaStorage(HASH);
      |  org.kiji.schema.avro.SchemaType type;
      |  union { null, string } value = null;
      |  org.kiji.schema.avro.AvroValidationPolicy avro_validation_policy =
      |      org.kiji.schema.avro.AvroValidationPolicy(SCHEMA_1_0);
      |  union { null, string } specific_reader_schema_class = null;
      |  union { null, org.kiji.schema.avro.AvroSchema } default_reader = null;
      |  union { null, array<org.kiji.schema.avro.AvroSchema> } readers = null;
      |  union { null, array<org.kiji.schema.avro.AvroSchema> } written = null;
      |  union { null, array<org.kiji.schema.avro.AvroSchema> } writers = null;
      |  union { null, string } protobuf_full_name = null;
      |  union { null, string } protobuf_class_name = null;
      |}
      |record org.kiji.schema.avro.ColumnDesc {
      |  int id = 0;
      |  string name;
      |  array<string> aliases = [];
      |  boolean enabled = true;
      |  string description = "";
      |  org.kiji.schema.avro.CellSchema column_schema;
      |  boolean delete = false;
      |  union { null, string } renamed_from = null;
      |}
      |record org.kiji.schema.avro.FamilyDesc {
      |  int id = 0;
      |  string name;
      |  array<string> aliases = [];
      |  boolean enabled = true;
      |  string description = "";
      |  union { null, org.kiji.schema.avro.CellSchema } map_schema = null;
      |  array<org.kiji.schema.avro.ColumnDesc> columns = [];
      |  boolean delete = false;
      |  union { null, string } renamed_from = null;
      |}
      |enum org.kiji.schema.avro.BloomType { NONE, ROW, ROWCOL }
      |record org.kiji.schema.avro.LocalityGroupDesc {
      |  int id = 0;
      |  string name;
      |  array<string> aliases = [];
      |  boolean enabled = true;
      |  string description = "";
      |  boolean in_memory;
      |  int max_versions;
      |  int ttl_seconds;
      |  union { null, int } block_size = null;
      |  union { null, org.kiji.schema.avro.BloomType } bloom_type = null;
      |  org.kiji.schema.avro.CompressionType compression_type;
      |  array<org.kiji.schema.avro.FamilyDesc> families = [];
      |  boolean delete = false;
      |  union { null, string } renamed_from = null;
      |}
      |enum org.kiji.schema.avro.HashType { MD5 }
      |enum org.kiji.schema.avro.RowKeyEncoding { RAW, HASH, HASH_PREFIX, FORMATTED }
      |record org.kiji.schema.avro.HashSpec {
      |  org.kiji.schema.avro.HashType hash_type = org.kiji.schema.avro.HashType(MD5);
      |  int hash_size = 16;
      |  boolean suppress_key_materialization = false;
      |}
      |record org.kiji.schema.avro.RowKeyFormat {
      |  org.kiji.schema.avro.RowKeyEncoding encoding;
      |  union { null, org.kiji.schema.avro.HashType } hash_type = null;
      |  int hash_size = 0;
      |}
      |enum org.kiji.schema.avro.ComponentType { STRING, INTEGER, LONG }
      |record org.kiji.schema.avro.RowKeyComponent {
      |  string name;
      |  org.kiji.schema.avro.ComponentType type;
      |}
      |record org.kiji.schema.avro.RowKeyFormat2 {
      |  org.kiji.schema.avro.RowKeyEncoding encoding;
      |   union { org.kiji.schema.avro.HashSpec, null } salt =
      |       org.kiji.schema.avro.HashSpec {
      |           hash_type=org.kiji.schema.avro.HashType(MD5),
      |           hash_size=2,
      |           suppress_key_materialization=false
      |       };
      |  int range_scan_start_index = 1;
      |  int nullable_start_index = 1;
      |  array<org.kiji.schema.avro.RowKeyComponent> components = [];
      |}
      |record org.kiji.schema.avro.TableLayoutDesc {
      |  string name;
      |  union { null, long } max_filesize = null;
      |  union { null, long } memstore_flushsize = null;
      |  string description = "";
      |  union {
      |      org.kiji.schema.avro.RowKeyFormat,
      |      org.kiji.schema.avro.RowKeyFormat2
      |  } keys_format;
      |  array<org.kiji.schema.avro.LocalityGroupDesc> locality_groups = [];
      |  string version;
      |  union { null, string } layout_id = null;
      |  union { null, string } reference_layout = null;
      |}
    """.stripMargin)

    val layoutSchema = schemaParser.get("org.kiji.schema.avro.TableLayoutDesc")
    Log.info("TableLayout descriptor schema: {}",
        AvroSchema.toString(layoutSchema))
  }

}
