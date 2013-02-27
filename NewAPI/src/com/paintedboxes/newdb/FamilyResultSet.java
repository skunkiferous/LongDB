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

/** A result set for a single family.
 * 
 * @author monster
 * @param <ROW>
 *        The type of the row key container
 * @param <KEY>
 *        The type of the column key container
 * @param <VALUE>
 *        The type of the value container */
public interface FamilyResultSet<ROW extends RowKey<?>, KEY extends ColumnKey<?>, VALUE extends Value<?>>
        extends Iterable<FamilyRow<ROW, KEY, VALUE>> {
    /** The number of rows. */
    long rows();

    /** Returns one column family, if present, otherwise null. */
    FamilyRow<ROW, KEY, VALUE> row(final ROW row);

    /** Returns all the rows as array. */
    FamilyRow<ROW, KEY, VALUE>[] asFamilyRowArray();
}
