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

/** A result set.
 * 
 * @author monster
 * @param <ROW>
 *        The type of the row key container */
public interface Table<ROW extends RowKey<?>> {
    /** The number of rows. */
    long entries();

    /** Returns the selected rows, if any, otherwise an empty result set. Blocks
     * until the results arrive, or the query fails. Most parameters are
     * optional. You can query with a lower bound, and upper bound, both, or
     * neither.
     * 
     * @param timeout
     *        The maximum time to wait for results.
     * @param family
     *        The column family to select.
     * @param fromRow
     *        The first row (inclusive) to select, if any.
     * @param toRow
     *        The last row (inclusive) to select, if any.
     * @param fromColumn
     *        The first column (inclusive) to select, if any.
     * @param toColumn
     *        The last column (inclusive) to select, if any.
     * @param fromVersion
     *        The last version (inclusive) to select, if any.
     * @param toVersion
     *        The last version (inclusive) to select, if any. */
    <KEY extends ColumnKey<?>, VALUE extends Value<?>> FamilyResultSet<ROW, KEY, VALUE> select(
            final long timeout, final String family, final ROW fromRow,
            final ROW toRow, final KEY fromColumn, final KEY toColumn,
            final Long fromVersion, final Long toVersion);

    /** Perform the query, and inform the receiver over the results. Most
     * parameters are optional. You can query with a lower bound, and upper
     * bound, both, or neither.
     * 
     * @param queryKey
     *        The query key; it is passed back to the receiver.
     * @param receiver
     *        The query result receiver
     * @param timeout
     *        The maximum time to wait for results.
     * @param family
     *        The column family to select.
     * @param fromRow
     *        The first row (inclusive) to select, if any.
     * @param toRow
     *        The last row (inclusive) to select, if any.
     * @param fromColumn
     *        The first column (inclusive) to select, if any.
     * @param toColumn
     *        The last column (inclusive) to select, if any.
     * @param fromVersion
     *        The last version (inclusive) to select, if any.
     * @param toVersion
     *        The last version (inclusive) to select, if any. */
    <Q, KEY extends ColumnKey<?>, VALUE extends Value<?>> void select(
            final Q queryKey,
            final FamilyQueryReceiver<Q, ROW, KEY, VALUE> receiver,
            final long timeout, final String family, final ROW fromRow,
            final ROW toRow, final KEY fromColumn, final KEY toColumn,
            final Long fromVersion, final Long toVersion);

    /** Returns the selected rows, if any, otherwise an empty result set. Blocks
     * until the results arrive, or the query fails. Most parameters are
     * optional. You can query with a lower bound, and upper bound, both, or
     * neither.
     * 
     * @param timeout
     *        The maximum time to wait for results.
     * @param fromRow
     *        The first row (inclusive) to select, if any.
     * @param toRow
     *        The last row (inclusive) to select, if any.
     * @param fromVersion
     *        The last version (inclusive) to select, if any.
     * @param toVersion
     *        The last version (inclusive) to select, if any. */
    ResultSet<ROW> select(final long timeout, final ROW fromRow,
            final ROW toRow, final Long fromVersion, final Long toVersion);

    /** Perform the query, and inform the receiver over the results. Most
     * parameters are optional. You can query with a lower bound, and upper
     * bound, both, or neither.
     * 
     * @param queryKey
     *        The query key; it is passed back to the receiver.
     * @param receiver
     *        The query result receiver
     * @param timeout
     *        The maximum time to wait for results.
     * @param fromRow
     *        The first row (inclusive) to select, if any.
     * @param toRow
     *        The last row (inclusive) to select, if any.
     * @param fromVersion
     *        The last version (inclusive) to select, if any.
     * @param toVersion
     *        The last version (inclusive) to select, if any. */
    <Q> void select(final Q queryKey, final QueryReceiver<Q, ROW> receiver,
            final long timeout, final ROW fromRow, final ROW toRow,
            final Long fromVersion, final Long toVersion);
}
