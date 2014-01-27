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


/** Interface for builders of records whose fields are accessed by name. */
public interface IGenericRecordBuilder<T extends IGenericRecordBuilder<?>>
    extends IIndexedRecordBuilder<T>, GenericRecord {

  /**
   * Sets the value of a field.
   *
   * @param fieldName Name of the field to set.
   * @param value the value to set.
   * @return a reference to the RecordBuilder.
   */
  T set(String fieldName, Object value);

  /**
   * Checks whether a field has been set.
   *
   * @param fieldName Name of the field to check.
   * @return true if the given field is non-null; false otherwise.
   */
  boolean has(String fieldName);

  /**
   * Clears the value of the given field.
   *
   * @param fieldName Name of the field to clear.
   * @return a reference to the RecordBuilder.
   */
  T clear(String fieldName);
}
