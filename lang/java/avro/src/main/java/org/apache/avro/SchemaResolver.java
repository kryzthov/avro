package org.apache.avro;

/**
 * Interface of a schema resolver.
 *
 * <p>
 *   A schema resolver defines a bidirectional mapping between Avro schemas and some external
 *   schema ID.
 *   Examples include: JsonSchemaResolver, HashingSchemaResolver.
 * </p>
 */
public interface SchemaResolver<T> {
  T getId(Schema schema);

  /**
   * Resolves a schema from its ID.
   *
   * @return the resolved schema, or null.
   */
  Schema getSchema(T id);
}
