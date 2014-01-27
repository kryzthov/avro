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
package org.apache.avro.generic;


/** Interface for builder of records whose fields are accessed by field index. */
public interface IIndexedRecordBuilder<T extends IIndexedRecordBuilder<?>>
    extends IndexedRecord {

  /**
   * Sets the value of a field.
   *
   * @param fieldIndex the field to set.
   * @param value the value to set.
   * @return this builder.
   */
  T set(int fieldIndex, Object value);

  /**
   * Checks whether a field has been set.
   *
   * @param fieldIndex the position of the field to check.
   * @return whether the specified field is set or not.
   */
  boolean has(int fieldIndex);

  /**
   * Clears the value of the given field.
   *
   * @param fieldIndex the position of the field to clear.
   * @return this builder.
   */
  T clear(int fieldIndex);
}
