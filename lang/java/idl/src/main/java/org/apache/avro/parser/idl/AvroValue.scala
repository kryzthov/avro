package org.apache.avro.parser.idl

import java.lang.{Integer => JInt}
import java.lang.{Iterable => JIterable}
import java.lang.{Long => JLong}
import java.util.{Map => JMap}
import scala.collection.JavaConverters._
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.commons.lang.StringEscapeUtils
import org.apache.avro.generic.GenericContainer

/**
 * Helper function to serialize an Avro value into a string representation.
 * TODO: Handle recursive value?
 */
object AvroValue {

  /**
   * Serializes an Avro value to string.
   *
   * @param value Avro value to serialize.
   * @param schema Avro schema of the value to serialize.
   * @return the string representation of the Avro value.
   */
  def toString(value: Any, schema: Schema): String = {
    schema.getType match {
      case Type.NULL => {
        require(value == null)
        return "null"
      }
      case Type.BOOLEAN => {
        return value.asInstanceOf[Boolean].toString
      }
      case Type.INT => {
        return value.asInstanceOf[JInt].toString
      }
      case Type.LONG => {
        return value.asInstanceOf[JLong].toString + "L"
      }
      case Type.FLOAT => {
        return value.asInstanceOf[Float].toString + "f"
      }
      case Type.DOUBLE => {
        return value.asInstanceOf[Double].toString + "d"
      }
      case Type.BYTES => {
        val bytes = value.asInstanceOf[Array[Byte]]
        return "bytes(%s)".format(bytes.map {byte => "%02x".format(byte)}.mkString(","))
      }
      case Type.STRING => {
        return """"%s"""".format(
            StringEscapeUtils.escapeJava(value.asInstanceOf[CharSequence].toString)
        )
      }
      case Type.FIXED => {
        val fixed = value.asInstanceOf[GenericData.Fixed]
        return "%s(%s)".format(
            schema.getFullName,
            fixed.bytes.map {byte => "%02x".format(byte)}.mkString(",")
        )
      }
      case Type.ENUM => {
        return "%s(%s)".format(schema.getFullName, value.toString)
      }
      case Type.RECORD => {
        val record = value.asInstanceOf[GenericRecord]
        val fields = schema.getFields.asScala.map {
          field: Schema.Field =>
            "%s=%s".format(field.name, toString(record.get(field.name), field.schema))
        }
        return "%s{%s}".format(
            schema.getFullName,
            fields.mkString(",")
        )
      }
      case Type.ARRAY => {
        val elementSchema = schema.getElementType
        val iterator = {
          value match {
            case array: Array[_] => array.iterator
            case iterable: JIterable[_] => iterable.iterator.asScala
            case _ => sys.error("Not an array: " + value)
          }
        }
        return "[%s]".format(
            iterator
                .map { element => toString(element, elementSchema) }
                .mkString(",")
        )
      }
      case Type.MAP => {
        val valueSchema = schema.getValueType
        val iterator: Iterator[(String, _)] = {
          value match {
            case jmap: JMap[String, _] => jmap.asScala.iterator
            case _ => sys.error("Not a recognized map: " + value)
          }
        }
        return "{%s}".format(
            iterator
                .map { case (key: String, v) =>
                  """"%s":%s""".format(StringEscapeUtils.escapeJava(key), toString(v, valueSchema))
                }
                .mkString(",")
        )
      }
      case Type.UNION => {
        value match {
          case container: GenericContainer => {
            val actualSchema = container.getSchema
            require(schema.getTypes.contains(actualSchema))
            toString(container, actualSchema)
          }
          case _ => {
            for (avroType <- schema.getTypes.asScala) {
              try {
                return toString(value, avroType)
              } catch {
                // This is terrible :(
                case _: ClassCastException | _: IllegalArgumentException => {
                  // ignore and try next union type
                }
              }
            }
          }
        }
        sys.error("Unable to serialize union value {} of type: {}".format(value, schema))
      }
      case _ => sys.error("Unknown or unexpected schema: " + schema)
    }
  }
}
