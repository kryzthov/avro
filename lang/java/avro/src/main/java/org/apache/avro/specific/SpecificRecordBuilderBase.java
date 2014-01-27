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
package org.apache.avro.specific;

import org.apache.avro.Schema;
import org.apache.avro.data.RecordBuilderBase;
import org.apache.avro.generic.IGenericRecordBuilder;

/**
 * Abstract base class for specific RecordBuilder implementations.
 * Not thread-safe.
 */
public abstract class SpecificRecordBuilderBase<T extends SpecificRecord,
                                                U extends SpecificRecordBuilderBase<?, ?>>
    extends RecordBuilderBase<T>
    implements IGenericRecordBuilder<U> {

  /**
   * Creates a SpecificRecordBuilderBase for building records of the given type.
   * @param schema the schema associated with the record class.
   */
  protected SpecificRecordBuilderBase(Schema schema) {
    super(schema, SpecificData.get());
  }

  /**
   * SpecificRecordBuilderBase copy constructor.
   * @param other SpecificRecordBuilderBase instance to copy.
   */
  protected SpecificRecordBuilderBase(SpecificRecordBuilderBase<T, U> other) {
    super(other, SpecificData.get());
  }

  /**
   * Creates a SpecificRecordBuilderBase by copying an existing record instance.
   * @param other the record instance to copy.
   */
  protected SpecificRecordBuilderBase(T other) {
    super(other.getSchema(), SpecificData.get());
  }

  /** {@inheritDoc} */
  @Override
  public U set(String fieldName, Object value) {
    return set(getSchema().getField(fieldName).pos(), value);
  }

  /** {@inheritDoc} */
  @Override
  public U clear(String fieldName) {
    return clear(getSchema().getField(fieldName).pos());
  }

  @Override
  public U set(int fieldIndex, Object value) {
    put(fieldIndex, value);
    return (U) this;
  }
}
