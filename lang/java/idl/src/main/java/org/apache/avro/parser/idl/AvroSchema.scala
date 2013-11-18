package org.apache.avro.parser.idl

import scala.collection.JavaConverters.asScalaBufferConverter

import org.apache.avro.Schema
import org.apache.avro.Schema.Type

object AvroSchema {
  def toString(schema: Schema): String = {
    schema.getType match {
      case Type.NULL =>
        return "null"

      case Type.BOOLEAN =>
        return "boolean"

      case Type.INT =>
        return "int"

      case Type.LONG =>
        return "long"

      case Type.FLOAT =>
        return "float"

      case Type.DOUBLE =>
        return "double"

      case Type.BYTES => {
        return "bytes"
      }

      case Type.STRING =>
        return "string"

      case Type.FIXED => {
        return "fixed %s(%s)".format(schema.getFullName, schema.getFixedSize)
      }

      case Type.ENUM => {
        return "enum %s{%s}".format(
            schema.getFullName,
            schema.getEnumSymbols.asScala.mkString(",")
        )
      }
      case Type.RECORD => {
        val fields = schema.getFields.asScala.map {
          field: Schema.Field =>
            Option(field.defaultValue) match {
              case None =>
                  "%s %s".format(toString(field.schema), field.name)
              case Some(default) =>
                  "%s %s = %s".format(toString(field.schema), field.name, default)
            }
        }
        return "record %s{%s}".format(
            schema.getFullName,
            fields.mkString(",")
        )
      }

      case Type.UNION => {
        return "union{%s}".format(schema.getTypes.asScala.map { toString } mkString(","))
      }

      case Type.ARRAY => {
        return "array<%s>".format(toString(schema.getElementType))
      }

      case Type.MAP => {
        return "map<%s>".format(toString(schema.getValueType))
      }

      case _ => sys.error("Unknown/unexpected schema: " + schema)
    }
  }
}