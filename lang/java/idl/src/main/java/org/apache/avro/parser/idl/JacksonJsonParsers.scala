package org.apache.avro.parser.idl

import scala.util.parsing.combinator.JavaTokenParsers
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.node.JsonNodeFactory
import org.slf4j.Logger

/**
 * Parses JSON strings into Jackson nodes.
 */
trait JacksonJsonParser
    extends JavaTokenParsers
    with StringParsers {
  val Log: Logger

  /** Parser for JSON value. */
  def jacksonJsonValue: Parser[JsonNode] = {
    ( "null" ^^ { _ => JsonNodeFactory.instance.nullNode }
    | "true" ^^ { _ => JsonNodeFactory.instance.booleanNode(true) }
    | "false" ^^ { _ => JsonNodeFactory.instance.booleanNode(false) }
    | wholeNumber ^^ { number => JsonNodeFactory.instance.numberNode(number.toLong) }
    | floatingPointNumber ^^ { number => JsonNodeFactory.instance.numberNode(number.toDouble) }
    | quotedStringLiteral ^^ { literal => JsonNodeFactory.instance.textNode(literal) }
    | "[" ~> jsonValues <~ "]" ^^ {
      values => {
        val array = JsonNodeFactory.instance.arrayNode()
        for (value <- values) { array.add(value) }
        array
      }
    }
    | "{" ~> jsonEntries <~ "}" ^^ {
      entries: List[(String, JsonNode)] => {
        val jsonObject = JsonNodeFactory.instance.objectNode()
        for ((key, value) <- entries) {
          jsonObject.put(key, value)
        }
        jsonObject
      }
    }
    )
  }

  /** Parser for comma-separated list of JSON values. */
  private def jsonValues: Parser[List[JsonNode]] = {
    ((jacksonJsonValue <~ ",")*) ~ (jacksonJsonValue?) ^^ { parsed => parsed._1 ++ parsed._2 }
  }

  /** Parser for a single JSON entry (key, value) where the key must be a string. */
  private def jsonEntry: Parser[(String, JsonNode)] = {
    (quotedStringLiteral ~ (":" ~> jacksonJsonValue)) ^^ {
      parsed => {
        val key: String = parsed._1
        val value: JsonNode = parsed._2
        (key, value)
      }
    }
  }

  /** Parser for a list of JSON entries (part of a JSON object). */
  private def jsonEntries: Parser[List[(String, JsonNode)]] = {
    ((jsonEntry <~ ",")*) ~ (jsonEntry?) ^^ { parsed => parsed._1 ++ parsed._2 }
  }
}
