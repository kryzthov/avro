package org.apache.avro.parser.idl

import org.apache.commons.lang.StringEscapeUtils
import scala.util.parsing.combinator.RegexParsers
import org.slf4j.Logger

trait StringParsers extends RegexParsers {
  val Log: Logger

  /** Parser for a string literal surrounded by single quotes. */
  def singleQuoteStringLiteral: Parser[String] = {
    ("""[']([^'\p{Cntrl}\\]|\\[\\/bfnrt'"]|\\u[a-fA-F0-9]{4})*[']""").r ^^ {
      literal: String => {
        val str = unescapeStringLiteral(literal.slice(1, literal.length - 1))
        Log.info("Parsed single quoted string: '{}'", str)
        str
      }
    }
  }

  /** Parser for a string literal surrounded by double quotes. */
  def doubleQuoteStringLiteral: Parser[String] = {
    ("""["]([^"\p{Cntrl}\\]|\\[\\/bfnrt'"]|\\u[a-fA-F0-9]{4})*["]""").r ^^ {
      literal: String => {
        val str = unescapeStringLiteral(literal.slice(1, literal.length - 1))
        Log.info("Parsed double quoted string: '{}'", str)
        str
      }
    }
  }

  /** Parser for a string literal surrounded by 3 double quotes. */
  def tripleDoubleQuoteStringLiteral: Parser[String] = {
    (""""{3}([^\\]|\\[\\/bfnrt'"]|\\u[a-fA-F0-9]{4})*"{3}""").r ^^ {
      literal: String => {
        val str = unescapeStringLiteral(literal.slice(3, literal.length - 3))
        Log.info("Parsed triple quoted string: '{}'", str)
        str
      }
    }
  }

  /** Parser for a quoted string literal. */
  def quotedStringLiteral: Parser[String] = {
    // Note: the triple-quoted string must be first
    (tripleDoubleQuoteStringLiteral | singleQuoteStringLiteral | doubleQuoteStringLiteral)
  }

  /**
   * Unescape a string literal.
   *
   * @param literal Escaped string literal (includes leading and trailing quotes).
   * @return the unescaped string literal.
   */
  private def unescapeStringLiteral(escaped: String): String = {
    return StringEscapeUtils.unescapeJavaScript(escaped)
  }

}
