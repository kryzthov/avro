package org.apache.avro;

/**
 * Bidirectional mapping between Avro Schema and their JSON representation.
 */
public class JsonSchemaResolver implements SchemaResolver<String> {
  /** {@inheritDoc} */
  @Override
  public String getId(Schema schema) {
    return schema.toString(false);
  }

  /** {@inheritDoc} */
  @Override
  public Schema getSchema(String id) {
    return new Schema.Parser().parse(id);
  }
}
