package org.apache.avro.parser.idl

import java.util.HashMap
import java.util.{List => JList}
import java.util.{Map => JMap}

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.util.parsing.combinator.JavaTokenParsers
import scala.util.parsing.input.CharSequenceReader

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/** Parser for an Avro value. */
trait AvroValueParser
    extends JavaTokenParsers
    with StringParsers {

  val Log: Logger

  /** Parses an optional namespace. */
  private def namespace: Parser[Option[List[String]]] = {
    ("."?) ~ ((ident <~ ".")*) ^^ { parsed =>
      val nsComponents = parsed._2
      if (nsComponents.isEmpty) {
        if (parsed._1.isEmpty) {
          None  // relative
        } else {
          Some(List())
        }
      } else {
        Some(nsComponents)
      }
    }
  }

  /**
   * Parses an Avro name ".name.space.SimpleName".
   */
  private def avroName: Parser[AvroName] = {
    (namespace ~ ident) ^^ { parsed => new AvroName(name = parsed._2, ns = parsed._1) }
  }

  /** Parser for an Avro record. */
  private def avroRecord(schema: Schema): Parser[GenericData.Record] = {
    (avroName <~ "{") into {
      name: AvroName => {
        require(name.fullName == schema.getFullName)
        val record = new GenericData.Record(schema)
        (recordField(record, schema)*) <~ "}" ^^ { parsed => record }
      }
    }
  }

  /** Parser for a single record field. Populates a pre-existing record. */
  private def recordField(record: GenericData.Record, schema: Schema): Parser[Unit] = {
    (ident <~ "=") into { fieldName: String =>
      val field = schema.getField(fieldName)
      (firstAvroValue(field.schema()) <~ opt(";"|",")) ^^ { value => record.put(fieldName, value) }
    }
  }

  /** Parser for an Avro enum value. */
  private def avroEnum(schema: Schema): Parser[GenericData.EnumSymbol] = {
    avroName ~ ("(" ~> ident <~ ")") ^^ {
      parsed => {
        val avroName: AvroName = parsed._1
        val symbol: String = parsed._2
        require(avroName.fullName == schema.getFullName)
        val enum = new GenericData.EnumSymbol(schema, symbol)
        enum
      }
    }
  }

  /** Parser for an Avro fixed declaration. */
  private def avroFixed(schema: Schema): Parser[GenericData.Fixed] = {
    avroName ~ ("(" ~> avroBytes <~ ")") ^^ {
      parsed => {
        val avroName: AvroName = parsed._1
        val bytes: Array[Byte] = parsed._2
        val fixed = new GenericData.Fixed(schema, bytes)
        fixed
      }
    }
  }

  private def avroNull: Parser[Null] = {
    "null" ^^ { _ => null }
  }

  private def avroBoolean: Parser[Boolean] = {
      ("false" ^^ { _ => false }) | ("true" ^^ { _ => true })
  }

  private def avroInt: Parser[Int] = {
    wholeNumber ^^ { number => number.toInt }
  }

  private def avroLong: Parser[Long] = {
    wholeNumber <~ opt("l"|"L") ^^ { number => number.toLong }
  }

  private def avroFloat: Parser[Float] = {
    floatingPointNumber <~ opt("f"|"F") ^^ { number => number.toFloat }
  }

  private def avroDouble: Parser[Double] = {
    floatingPointNumber <~ opt("d"|"D") ^^ { number => number.toDouble }
  }

  private def avroString: Parser[String] = {
    quotedStringLiteral
  }

  /** Parses an Avro array of elements into a Java list. */
  private def avroArray(schema: Schema): Parser[JList[_]] = {
    ("[" ~> ((firstAvroValue(schema) <~ opt(";"|","))*) <~ "]") ^^ {
      elements: List[Any] => elements.asJava
    }
  }

  /** Parsed an Avro map of items into a Java hash map. */
  private def avroMap(schema: Schema): Parser[JMap[String, _]] = {
    ("{" ~> (mapItem(schema)*) <~ "}") ^^ {
      elements: List[(String, Any)] => {
        val map = new HashMap[String, Any]()
        for ((key, value) <- elements) {
          map.put(key, value)
        }
        map
      }
    }
  }

  private def mapItem(schema: Schema): Parser[(String, Any)] = {
    ((avroString <~ ":") ~ (firstAvroValue(schema) <~ opt(";"|","))) ^^ {
      parsed => {
        val key: String = parsed._1
        val value: Any = parsed._2
        (key, value)
      }
    }
  }

  /** Parses an arbitrary sequence of bytes. */
  private def avroBytes: Parser[Array[Byte]] = {
    ("bytes" ~> "(" ~> byteArray <~  ")")
  }

  /** Parses a byte sequence into an array of bytes, optionally separated by ',', ';' or ':'. */
  private def byteArray: Parser[Array[Byte]] = {
    ((byte <~ opt("," | ":" | ";"))*) ^^ { byteList: List[Byte] => byteList.toArray }
  }

  /** Parses a byte represented in hexadecimal. */
  private def byte: Parser[Byte] = {
    ("[0-9a-fA-F]{2}"r) ^^ { parsed => Integer.parseInt(parsed, 16).toByte }
  }

  /**
   * Parser for a value whose type belongs to an Avro union.
   */
  private def avroUnion(schema: Schema): Parser[Any] = Parser[Any] {
    in: Input => {
      def parseUnion(in: Input): ParseResult[Any] = {
        for (branch: Schema <- schema.getTypes.asScala) {
          firstAvroValue(branch).apply(in) match {
            case success: Success[Any] => {
              return success
            }
            case _ => /* ignore and try next union branch */
          }
        }
        return Failure("Value '%s' does not match union '%s'.".format(in, schema), in)
      }
      parseUnion(in)
    }
  }

  /** Parses exactly one Avro value, allowing no trailer. */
  def avroValue(schema: Schema): Parser[Any] = {
    phrase(firstAvroValue(schema))
  }

  /** Parses the first Avro value from the input. */
  def firstAvroValue(schema: Schema): Parser[Any] = {
    return schema.getType match {
      case Schema.Type.NULL => avroNull
      case Schema.Type.BOOLEAN => avroBoolean
      case Schema.Type.INT => avroInt
      case Schema.Type.LONG => avroLong
      case Schema.Type.FLOAT => avroFloat
      case Schema.Type.DOUBLE => avroDouble
      case Schema.Type.BYTES => avroBytes
      case Schema.Type.STRING => avroString
      case Schema.Type.ARRAY => avroArray(schema.getElementType)
      case Schema.Type.MAP => avroMap(schema.getValueType)
      case Schema.Type.FIXED => avroFixed(schema)
      case Schema.Type.ENUM => avroEnum(schema)
      case Schema.Type.RECORD => avroRecord(schema)
      case Schema.Type.UNION => avroUnion(schema)
      case _ => sys.error("Unhandled schema: " + schema)
    }
  }
}

object AvroValueParser extends AvroValueParser {
  final val Log = LoggerFactory.getLogger(classOf[AvroValueParser])

  /**
   * Parse an Avro value.
   *
   * @param text Text representation of an Avro value.
   * @param schema Avro schema of the value to parse.
   * @return the Avro value parsed from the text representation.
   */
  def parse(text: String, schema: Schema): Any = {
    val input: Input = new CharSequenceReader(text)
    val result: ParseResult[Any] = avroValue(schema).apply(input)
    result match {
      case error: Error => {
        sys.error("Parse error in '%s': %s".format(text, error.msg))
      }
      case success: Success[Any] => {
        result.get
      }
      case _ => {
        sys.error("Error parsing '%s': %s".format(text, result))
      }
    }
  }
}
