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

/** A row with just one family.
 * 
 * @author monster
 * @param <ROW>
 *        The type of the row key container
 * @param <KEY>
 *        The type of the column key container
 * @param <VALUE>
 *        The type of the value container */
public interface FamilyRow<ROW extends RowKey<?>, KEY extends ColumnKey<?>, VALUE extends Value<?>> {
    /** The row key container. */
    ROW rowKey();

    /** Returns one column family, if present, otherwise null. */
    Family<KEY, VALUE> family();
}
