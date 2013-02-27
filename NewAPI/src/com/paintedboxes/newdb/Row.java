/*******************************************************************************
 * Copyright 2013 Sebastien Diot
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
/**
 *
 */
package com.paintedboxes.newdb;

/** A row for any number of family.
 * 
 * @author monster
 * @param <ROW>
 *        The type of the row key container */
public interface Row<ROW extends RowKey<?>> extends Iterable<Family<?, ?>> {
    /** The row key container. */
    ROW rowKey();

    /** The number of column families. */
    long families();

    /** Returns one column family, if present, otherwise null. */
    <KEY extends ColumnKey<?>, VALUE extends Value<?>> Family<KEY, VALUE> family(
            final String label);

    /** Returns all the families as array. */
    <KEY extends ColumnKey<?>, VALUE extends Value<?>> Family<KEY, VALUE>[] asFamilyArray();
}