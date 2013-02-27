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
package com.blockwithme.longdb;

import java.util.Iterator;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import com.blockwithme.longdb.entities.Base36;
import com.blockwithme.longdb.entities.LongHolder;
import com.carrotsearch.hppc.LongArrayList;

/** A back-end table. This is where the data is stored. Each individual
 * modification is atomic. There are no other transactional guarantees. The
 * reason the getters return a Columns object, instead of a Row object, is that
 * most of the time, we only return *parts* of a row, or in other words we
 * return a few columns, instead of a whole row. TODO: It is probably really
 * easy to cause an OOM by loading to may columns at the same time. We have to
 * add some functionality giving use the size of the individual columns, so we
 * can compute the size of the request, and somehow use an upper bound when
 * querying, so as to prevent OOM. Also, we should have a single value
 * representing the total size of the row. In case there is a possibility that
 * he sizes become incorrect, due to parallel access to the same row from two
 * processes, there should also be a refresh function that recalculate the
 * sizes, and logs if it finds any difference. In the worst case, this might
 * involve loading all columns, so better use an iterator. */
@ParametersAreNonnullByDefault
public interface BETable {

    /** Closes the table. No DB operation on this table is possible after this
     * call. */
    void close();

    /** Specifies if the columns are returned in sorted order.
     * 
     * @return true, if column sorting is supported */
    boolean columnOrdering();

    /** Returns the list of all columns headers in the row. Null if the row is
     * missing. The returned LongArrayList object can only be used until the
     * next call to columns()!
     * 
     * @param theKey
     *        the row ID (key)
     * @return the long array list containing column Ids. */
    @CheckForNull
    LongArrayList columns(long theKey);

    /** Returns the *approximate* number of columns stored in a row. 0 if the row
     * is missing.
     * 
     * @param theKey
     *        the row ID (key)
     * @return the number of columns in that row. */
    long columnsCount(long theKey);

    /** Returns an iterator over all column headers in the row. Null if the row
     * is missing. References to the returned LongHolder must not be held.
     * 
     * @param theKey
     *        the row ID (key)
     * @return the iterator can iterate over all the columns sequentially
     *         depending on the column sort order */
    @CheckForNull
    Iterator<LongHolder> columnsIterator(long theKey);

    /** Returns the database of this table.
     * 
     * @return the database instance */
    BEDatabase database();

    /** Can we detect and prevent collisions?.
     * 
     * @return true, if detect collisions is supported */
    boolean detectCollisions();

    /** Checks if a row exists with a given key.
     * 
     * @param theKey
     *        the row ID
     * @return true, if the row exists */
    boolean exists(long theKey);

    /** Returns a complete row; all it's columns. Null if the row is missing. The
     * returned Columns object can only be used until the next get()!
     * 
     * @param theKey
     *        the row ID
     * @return a complete row; all it's columns. Null if the row is missing */
    @CheckForNull
    Columns get(long theKey);

    /** Returns a Columns. Null if the row is missing. The unselected columns
     * will NOT be available. The returned Columns object can only be used until
     * the next get()!
     * 
     * @param theKey
     *        the row ID
     * @param theColumn
     *        the column ID
     * @return the columns */
    @CheckForNull
    Columns get(long theKey, long theColumn);

    /** Returns some Columns. Null if the row is missing. The unselected columns
     * will NOT be available. The returned Columns object can only be used until
     * the next get()!
     * 
     * @param theKey
     *        the row ID
     * @param theColumns
     *        the column Ids
     * @return the columns */
    @CheckForNull
    Columns get(long theKey, long... theColumns);

    /** Returns some Columns. Null if the row is missing. The unselected columns
     * will NOT be available. The returned Columns object can only be used until
     * the next get()!
     * 
     * @param theKey
     *        the row ID
     * @param theColumns
     *        the list of column Ids
     * @return the columns */
    @CheckForNull
    Columns get(long theKey, LongArrayList theColumns);

    /** Returns some Columns. Null if the row is missing. The unselected columns
     * will NOT be available. The returned Columns object can only be used until
     * the next get()!
     * 
     * @param theKey
     *        the row ID
     * @param theColumns
     *        range of column Ids
     * @return the columns */
    @CheckForNull
    Columns get(long theKey, Range theColumns);

    /** Returns the first N Columns, where first depends on the sort order, *if
     * any*. Null if the row is missing. The unselected columns will NOT be
     * available. The returned Columns object can only be used until the next
     * get()!
     * 
     * @param theKey
     *        the key
     * @param theCount
     *        the count of columns to be returned.
     * @return the columns
     * @see BETableProfile.reverseColumnsOrder */
    @CheckForNull
    Columns getLimited(long theKey, int theCount);

    /** Returns all the keys. This operation is only reliable when the table is
     * not being modified! There is no specific guaranteed order of the keys.
     * References to the returned LongHolder must not be held. TODO: You can use
     * the Iterator.remove() method to delete rows.
     * 
     * @return the iterator */
    @Nonnull
    Iterator<LongHolder> keys();

    /** Removes a complete row.
     * 
     * @param theKey
     *        the row ID */
    void remove(long theKey);

    /** Removes some columns.
     * 
     * @param theKey
     *        the row ID
     * @param theRemoveIds
     *        the column IDs to be removed */
    void remove(long theKey, long... theRemoveIds);

    /** Removes some columns.
     * 
     * @param theKey
     *        the row ID
     * @param theRemoveIds
     *        the column IDs to be removed */
    void remove(long theKey, LongArrayList theRemoveIds);

    /** Removes some columns.
     * 
     * @param theKey
     *        the row ID
     * @param theRemoveIdRange
     *        the range of column IDs to be removed */
    void remove(long theKey, Range theRemoveIdRange);

    /** Reverse column order?.
     * 
     * @return true if columns are sorted in reverse order
     * @see BETableProfile.reverseColumnsOrder */
    boolean reverse();

    /** Specifies if the underlying database supports reverse column storage.
     * 
     * @return true, if yes */
    boolean reverseSupported();

    /** Saves some Columns. Other pre-existing columns are unaffected. This
     * operation is atomic.
     * 
     * @param theKey
     *        the row ID
     * @param theInsertOrUpdate
     *        the columns to be inserted or updated */
    void set(long theKey, Columns theInsertOrUpdate);

    /** Saves some Columns. Delete others. The rest is unaffected. This operation
     * is atomic.
     * 
     * @param theKey
     *        the row ID
     * @param theInsertOrUpdate
     *        the columns to be inserted or updated
     * @param theRemoveIds
     *        the columns to be removed */
    void set(long theKey, Columns theInsertOrUpdate, long... theRemoveIds);

    /** Saves some Columns. Delete others. The rest is unaffected. This operation
     * is atomic.
     * 
     * @param theKey
     *        the row ID
     * @param theInsertOrUpdate
     *        the columns to be inserted or updated
     * @param theRemoveIds
     *        the list of columns to be removed */
    void set(long theKey, Columns theInsertOrUpdate, LongArrayList theRemoveIds);

    /** Saves some Columns. Delete others. The rest is unaffected. This operation
     * is atomic.
     * 
     * @param theKey
     *        the row ID
     * @param theInsertOrUpdate
     *        the columns to be inserted or updated
     * @param theRemoveRange
     *        the range of columns to be removed */
    void set(long theKey, Columns theInsertOrUpdate, Range theRemoveRange);

    /** Returns the *approximate* number of rows stored.
     * 
     * @return the number of rows */
    long size();

    /** Returns the ID/name of this table in the database.
     * 
     * @return the table name */
    Base36 table();

}
