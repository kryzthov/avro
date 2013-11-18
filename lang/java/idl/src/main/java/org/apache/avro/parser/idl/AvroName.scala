package org.apache.avro.parser.idl

/**
 * Representation of an Avro name.
 */
class AvroName(
    val name: String,
    ns: Option[List[String]]
) {
  val fullName: String = {
    ns match {
      case None => "." + name
      case Some(path) => path.mkString(".") + "." + name
    }
  }

  val nameSpace: String = {
    ns match {
      case None => null
      case Some(path) => path.mkString(".")
    }
  }
}
