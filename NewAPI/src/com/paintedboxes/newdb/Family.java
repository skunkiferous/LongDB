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

/** A column family.
 * 
 * @author monster
 * @param <KEY>
 *        The type of the column key container
 * @param <VALUE>
 *        The type of the value container */
public interface Family<KEY extends ColumnKey<?>, VALUE extends Value<?>>
        extends Iterable<Column<KEY, VALUE>> {
    /** The column family prefix. */
    String label();

    /** The number of columns. */
    long columns();

    /** Returns one columns, if present, otherwise null. */
    Column<KEY, VALUE> column(final KEY key);

    /** Returns all the columns as array. */
    Column<KEY, VALUE>[] asColumnArray();
}
