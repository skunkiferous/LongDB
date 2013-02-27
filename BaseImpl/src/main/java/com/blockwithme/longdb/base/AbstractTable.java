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
package com.blockwithme.longdb.base;

import java.util.Iterator;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import com.blockwithme.longdb.BEDatabase;
import com.blockwithme.longdb.BETable;
import com.blockwithme.longdb.Columns;
import com.blockwithme.longdb.Range;
import com.blockwithme.longdb.entities.Base36;
import com.blockwithme.longdb.entities.LongHolder;
import com.carrotsearch.hppc.LongArrayList;
import com.google.common.base.Preconditions;

/** Abstract base class of all tables. The methods implemented here are final to
 * force the implementation of the abstract methods to consider all possible
 * inputs. Use AbstractKeyIterator to implement the iterator method.
 * 
 * @param <B>
 *        the type of Backend.
 * @param <D>
 *        the type of Database
 * @param <T>
 *        the type of Table. */
@ParametersAreNonnullByDefault
public abstract class AbstractTable<B extends AbstractBackend<B, D, T>, D extends AbstractDatabase<B, D, T>, T extends AbstractTable<B, D, T>>
        implements BETable {

    /** The empty key range. */
    private static final Range EMPTY_RANGE = new Range(0, -1);

    /** The full key range. */
    private static final Range FULL_RANGE = Range.fullRange();

    /** Wrapper for long[]. */
    protected final LongArrayList array = new LongArrayList(1);

    /** Are we closed?. */
    protected boolean closed;

    /** The database. */
    protected final D database;

    /** Do we detect collisions?. */
    protected final boolean detectCollisions;

    /** One single long. Do not change the size of buffer! */
    protected final LongArrayList one = new LongArrayList(1);

    /** Are we opened?. */
    protected boolean opened;

    /** Reverse column order?. */
    protected final boolean reverse;

    /** The table ID/name. */
    protected final Base36 table;

    /** Protected Constructor.
     * 
     * @param theDatabase
     *        the database instance
     * @param theTable
     *        the table instance
     * @param theReverse
     *        the reverse column sorting indicator
     * @param theDetectCollisions
     *        the detect collisions indicator */
    protected AbstractTable(final D theDatabase, final Base36 theTable,
            final boolean theReverse, final boolean theDetectCollisions) {
        Preconditions.checkNotNull(theDatabase, "database is null");
        Preconditions.checkNotNull(theTable, "table is null");
        this.database = theDatabase;
        this.table = theTable;
        this.reverse = theReverse;
        this.detectCollisions = theDetectCollisions;
        one.add(0);
    }

    /** The database specific implementation of {@link AbstractTable#close()} */
    protected abstract void closeInternal();

    /** The database specific implementation of
     * {@link AbstractTable#columnsCount(long)}.
     * 
     * @param theKey
     *        the row key
     * @return the number of columns */
    protected abstract long columnsCountInternal(final long theKey);

    /** The database specific implementation of {@lint
     * AbstractTable#columns(long)}
     * 
     * @param theKey
     *        the row key
     * @return the List of column Ids */
    @CheckForNull
    protected abstract LongArrayList columnsInternal(final long theKey);

    /** The database specific implementation of
     * {@link AbstractTable#columnsIterator(long)}.
     * 
     * @param theKey
     *        the row key
     * @return Iterator of column Ids, returns null if row not found. */
    @CheckForNull
    protected abstract Iterator<LongHolder> columnsIteratorInternal(
            final long theKey);

    /** The database specific implementation of {@link AbstractTable#get(long)}
     * 
     * @param theKey
     *        the row key
     * @param theColumns
     *        the list of column Ids
     * @return the columns object containing column Ids and values, returns null
     *         if row not found */
    @CheckForNull
    protected abstract Columns getInternal(final long theKey,
            final LongArrayList theColumns);

    /** The database specific implementation of
     * {@link AbstractTable#get(long, Range)}
     * 
     * @param theKey
     *        the row key
     * @param theColumns
     *        the range of column Ids
     * @return the internal */
    @CheckForNull
    protected abstract Columns getInternal(final long theKey,
            final Range theColumns);

    /** The database specific implementation of {@link AbstractTable#keys()}
     * 
     * @return the iterator */
    @Nonnull
    protected abstract Iterator<LongHolder> keysInternal();

    /** * The database specific implementation of {@link AbstractTable#open()} */
    protected abstract void openInternal();

    /** The database specific implementation of
     * {@link AbstractTable#remove(long, LongArrayList)}
     * 
     * @param theKey
     *        the row key
     * @param theRemoveCols
     *        list of column ids to be removed. */
    protected abstract void removeInternal(final long theKey,
            final LongArrayList theRemoveCols);

    /** The database specific implementation of
     * {@link AbstractTable#remove(long, Range)}
     * 
     * @param theKey
     *        the row key
     * @param theRemoveCols
     *        the range of column Ids to be removed */
    protected abstract void removeInternal(final long theKey,
            final Range theRemoveCols);

    /** The database specific implementation of
     * {@link AbstractTable#setInternal(long, Columns, LongArrayList)}
     * 
     * @param theKey
     *        the row key
     * @param theInsertUpdateColumns
     *        columns to be inserted or updated
     * @param theRemoveCols
     *        list of columns to be removed */
    protected abstract void setInternal(final long theKey,
            final Columns theInsertUpdateColumns,
            final LongArrayList theRemoveCols);

    /** The database specific implementation of
     * {@link AbstractTable#set(long, Columns, Range)}
     * 
     * @param theKey
     *        the row key
     * @param theInsertUpdateColumns
     *        columns to be inserted or updated.
     * @param theRemoveCols
     *        range of columns to be removed. */
    protected abstract void setInternal(final long theKey,
            final Columns theInsertUpdateColumns, final Range theRemoveCols);

    /** The database specific implementation of {@link AbstractTable#size()}
     * 
     * @return the approximate number of rows in this table. */
    protected abstract long sizeInternal();

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#close()
     */
    @Override
    public final void close() {
        if (!closed) {
            closed = true;
            if (opened) {
                opened = false;
                closeInternal();
            }
        }
    }

    /*
     * (non-Javadoc) Assumes that columns are ordered unless specified
     * otherwise. *
     * 
     * @see com.blockwithme.longdb.BETable#columnOrdering()
     */
    @Override
    public boolean columnOrdering() {
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#columns(long)
     */
    @Override
    public final LongArrayList columns(final long theKey) {
        open();
        return columnsInternal(theKey);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#columnsCount(long)
     */
    @Override
    public final long columnsCount(final long theKey) {
        open();
        return columnsCountInternal(theKey);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#columnsIterator(long)
     */
    @Override
    public final Iterator<LongHolder> columnsIterator(final long theKey) {
        open();
        return columnsIteratorInternal(theKey);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#database()
     */
    @Override
    public final BEDatabase database() {
        return database;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#detectCollisions()
     */
    @Override
    public final boolean detectCollisions() {
        return detectCollisions;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#exists(long)
     */
    @Override
    public boolean exists(final long theKey) {
        return (columnsCount(theKey) > 0);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#get(long)
     */
    @Override
    @CheckForNull
    public final Columns get(final long theKey) {
        return get(theKey, FULL_RANGE);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#get(long, long)
     */
    @Override
    @CheckForNull
    public final Columns get(final long theKey, final long theCol) {
        one.buffer[0] = theCol;
        return get(theKey, one);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#get(long, long[])
     */
    @Override
    @CheckForNull
    public final Columns get(final long theKey, final long... theColumns) {
        if (theColumns == null)
            return null;
        array.elementsCount = theColumns.length;
        array.buffer = theColumns;
        try {
            return get(theKey, array);
        } finally {
            array.buffer = null;
            array.elementsCount = 0;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#get(long,
     * com.carrotsearch.hppc.LongArrayList)
     */
    @Override
    @CheckForNull
    public final Columns get(final long theKey, final LongArrayList theColumns) {
        open();
        return getInternal(theKey, theColumns);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#get(long, com.blockwithme.longdb.Range)
     */
    @Override
    @CheckForNull
    public final Columns get(final long theKey, final Range theColumns) {
        open();
        return getInternal(theKey, theColumns);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#keys()
     */
    @Override
    @Nonnull
    public final Iterator<LongHolder> keys() {
        open();
        return keysInternal();
    }

    /** Opens the table. */
    public final void open() {
        if (closed) {
            throw new IllegalStateException("Closed!");
        }
        if (!opened) {
            opened = true;
            openInternal();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#remove(long)
     */
    @Override
    public final void remove(final long theKey) {
        remove(theKey, FULL_RANGE);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#remove(long, long[])
     */
    @Override
    public final void remove(final long theKey, final long... theRemoveIds) {
        array.elementsCount = theRemoveIds.length;
        array.buffer = theRemoveIds;
        try {
            remove(theKey, array);
        } finally {
            array.buffer = null;
            array.elementsCount = 0;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#remove(long,
     * com.carrotsearch.hppc.LongArrayList)
     */
    @Override
    public final void remove(final long theKey,
            final LongArrayList theRemoveCols) {
        open();
        removeInternal(theKey, theRemoveCols);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#remove(long, com.blockwithme.longdb.Range)
     */
    @Override
    public final void remove(final long theKey, final Range theRemoveCols) {
        open();
        removeInternal(theKey, theRemoveCols);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#reverse()
     */
    @Override
    public final boolean reverse() {
        return reverse;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#set(long, com.blockwithme.longdb.Columns)
     */
    @Override
    public final void set(final long theKey,
            final Columns theInsertUpdateColumns) {
        set(theKey, theInsertUpdateColumns, EMPTY_RANGE);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#set(long, com.blockwithme.longdb.Columns,
     * long[])
     */
    @Override
    public final void set(final long theKey,
            final Columns theInsertUpdateColumns, final long... theRemoveIds) {
        array.elementsCount = theRemoveIds.length;
        array.buffer = theRemoveIds;
        try {
            set(theKey, theInsertUpdateColumns, array);
        } finally {
            array.buffer = null;
            array.elementsCount = 0;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#set(long, com.blockwithme.longdb.Columns,
     * com.carrotsearch.hppc.LongArrayList)
     */
    @Override
    public final void set(final long theKey,
            final Columns theInsertUpdateColumns,
            final LongArrayList theRemoveCols) {
        open();
        setInternal(theKey, theInsertUpdateColumns, theRemoveCols);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#set(long, com.blockwithme.longdb.Columns,
     * com.blockwithme.longdb.Range)
     */
    @Override
    public final void set(final long theKey,
            final Columns theInsertUpdateColumns, final Range theRemoveCols) {
        open();
        setInternal(theKey, theInsertUpdateColumns, theRemoveCols);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#size()
     */
    @Override
    public final long size() {
        open();
        return sizeInternal();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#table()
     */
    @Override
    @Nonnull
    public final Base36 table() {
        return table;
    }
}
