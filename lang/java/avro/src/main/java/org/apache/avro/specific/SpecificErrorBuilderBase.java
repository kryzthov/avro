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

import java.lang.reflect.Constructor;

import org.apache.avro.Schema;
import org.apache.avro.data.ErrorBuilder;
import org.apache.avro.data.RecordBuilderBase;

/**
 * Abstract base class for specific ErrorBuilder implementations.
 * Not thread-safe.
 */
abstract public class SpecificErrorBuilderBase<T extends SpecificExceptionBase,
                                               U extends SpecificErrorBuilderBase<?, ?>>
    extends RecordBuilderBase<T>
    implements ErrorBuilder<T, U> {

  private Constructor<T> errorConstructor;
  private Object value;
  private boolean hasValue;
  private Throwable cause;
  private boolean hasCause;

  /**
   * Creates a SpecificErrorBuilderBase for building errors of the given type.
   * @param schema the schema associated with the error class.
   */
  protected SpecificErrorBuilderBase(Schema schema) {
    super(schema, SpecificData.get());
  }

  /**
   * SpecificErrorBuilderBase copy constructor.
   * @param other SpecificErrorBuilderBase instance to copy.
   */
  protected SpecificErrorBuilderBase(SpecificErrorBuilderBase<T, U> other) {
    super(other, SpecificData.get());
    this.errorConstructor = other.errorConstructor;
    this.value = other.value;
    this.hasValue = other.hasValue;
    this.cause = other.cause;
    this.hasCause = other.hasCause;
  }

  /**
   * Creates a SpecificErrorBuilderBase by copying an existing error instance.
   * @param other the error instance to copy.
   */
  protected SpecificErrorBuilderBase(T other) {
    super(other.getSchema(), SpecificData.get());

    Object otherValue = other.getValue();
    if (otherValue != null) {
      setValue(otherValue);
    }

    Throwable otherCause = other.getCause();
    if (otherCause != null) {
      setCause(otherCause);
    }
  }

  /** {@inheritDoc} */
  @Override
  public Object getValue() {
    return value;
  }

  /** {@inheritDoc} */
  @Override
  public SpecificErrorBuilderBase<T, U> setValue(Object value) {
    this.value = value;
    hasValue = true;
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasValue() {
    return hasValue;
  }

  /** {@inheritDoc} */
  @Override
  public SpecificErrorBuilderBase<T, U> clearValue() {
    value = null;
    hasValue = false;
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public Throwable getCause() {
    return cause;
  }

  /** {@inheritDoc} */
  @Override
  public SpecificErrorBuilderBase<T, U> setCause(Throwable cause) {
    this.cause = cause;
    hasCause = true;
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasCause() {
    return hasCause;
  }

  /** {@inheritDoc} */
  @Override
  public SpecificErrorBuilderBase<T, U> clearCause() {
    cause = null;
    hasCause = false;
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public U clear(String fieldName) {
    return clear(getSchema().getField(fieldName).pos());
  }

  /** {@inheritDoc} */
  @Override
  public U set(String fieldName, Object value) {
    return set(getSchema().getField(fieldName).pos(), value);
  }

  /** {@inheritDoc} */
  @Override
  public U set(int fieldIndex, Object value) {
    put(fieldIndex, value);
    return (U) this;
  }
}
