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
// $codepro.audit.disable hidingInheritedFields
package com.blockwithme.longdb.mem;

import java.util.Iterator;

import javax.annotation.CheckForNull;
import javax.annotation.ParametersAreNonnullByDefault;

import com.blockwithme.longdb.Columns;
import com.blockwithme.longdb.Range;
import com.blockwithme.longdb.base.AbstractTable;
import com.blockwithme.longdb.entities.Base36;
import com.blockwithme.longdb.entities.LongHolder;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.LongCursor;

/** Implementation of a memory-backed BETable. Useful for testing. */
@ParametersAreNonnullByDefault
public class MemoryTable extends
        AbstractTable<MemoryBackend, MemoryDatabase, MemoryTable> {

    /** The full key range. */
    private static final Range FULL_RANGE = Range.fullRange();

    /** Temporary array */
    private LongArrayList array = new LongArrayList(); // NOPMD

    /** The rows. */
    private final LongObjectOpenHashMap<Columns> rows = new LongObjectOpenHashMap<Columns>();

    /** Instantiates a new memory table.
     * 
     * @param theDB
     *        the database
     * @param theTable
     *        the table
     * @param isReverse
     *        the reverse
     * @param theDetectCollisions
     *        the detect collisions */
    protected MemoryTable(final MemoryDatabase theDB, final Base36 theTable,
            final boolean isReverse, final boolean theDetectCollisions) {
        super(theDB, theTable, isReverse, theDetectCollisions);
    }

    /** Sets the internal.
     * 
     * @param theKey
     *        the key
     * @param theInsertUpdateColumns
     *        the insert or update */
    private void setInternal(final long theKey,
            final Columns theInsertUpdateColumns) {
        Columns row = rows.get(theKey);
        if (row == null) {
            row = new Columns(reverse);
            rows.put(theKey, row);
        }
        row.putAll(theInsertUpdateColumns);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#closeInternal()
     */
    @Override
    protected void closeInternal() {
        // NOP
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#columnsCountInternal(long)
     */
    @Override
    protected long columnsCountInternal(final long theKey) {
        final Columns row = rows.get(theKey);
        return (row == null) ? 0 : row.size();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#columnsInternal(long)
     */
    @Override
    @CheckForNull
    protected LongArrayList columnsInternal(final long theKey) {
        final Columns row = rows.get(theKey);
        if (row == null) {
            return null;
        }
        final long[] cols = row.columns();
        array = new LongArrayList();
        array.add(cols, 0, cols.length);
        // array.elementsCount = rows.size();
        return array;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#columnsIteratorInternal(long)
     */
    @Override
    @CheckForNull
    protected Iterator<LongHolder> columnsIteratorInternal(final long theKey) {
        final Columns cols = getInternal(theKey, FULL_RANGE);
        if (cols == null)
            return null;
        return cols.iterator();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#getInternal(long,
     * com.carrotsearch.hppc.LongArrayList)
     */
    @Override
    @CheckForNull
    protected Columns getInternal(final long theKey,
            final LongArrayList theColumns) {
        if ((theColumns == null) || theColumns.isEmpty())
            return null;
        final Columns row = rows.get(theKey);
        if (row == null) {
            return null;
        }
        final Columns result = row.copy();
        for (final LongHolder holder : row) {
            final long col = holder.value();
            if (!theColumns.contains(col))
                result.remove(col);
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#getInternal(long,
     * com.blockwithme.longdb.Range)
     */
    @Override
    @CheckForNull
    protected Columns getInternal(final long theKey, final Range theColumns) {
        if ((theColumns == null) || theColumns.empty())
            return null;
        final Columns row = rows.get(theKey);
        if (row == null) {
            return null;
        }
        final Columns result = row.copy();
        for (final LongHolder holder : row) {
            final long col = holder.value();
            if (!theColumns.contains(col))
                result.remove(col);
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#keysInternal()
     */
    @Override
    protected Iterator<LongHolder> keysInternal() {
        return new MemoryKeyIterator(this, rows);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#openInternal()
     */
    @Override
    protected void openInternal() {
        // NOP
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#removeInternal(long,
     * com.carrotsearch.hppc.LongArrayList)
     */
    @Override
    protected void removeInternal(final long theKey,
            final LongArrayList theRemoveCols) {
        final Columns row = rows.get(theKey);
        if (row != null) {
            for (final LongCursor cursor : theRemoveCols) {
                row.remove(cursor.value);
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#removeInternal(long,
     * com.blockwithme.longdb.Range)
     */
    @Override
    protected void removeInternal(final long theKey, final Range theRemoveCols) {
        final Columns row = rows.get(theKey);
        if ((row != null) && !theRemoveCols.empty()) {
            if (theRemoveCols.full()) {
                rows.remove(theKey);
            } else {
                for (final LongHolder holder : row) {
                    final long col = holder.value();
                    if (theRemoveCols.contains(col))
                        row.remove(col);
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#setInternal(long,
     * com.blockwithme.longdb.Columns, com.carrotsearch.hppc.LongArrayList)
     */
    @Override
    protected void setInternal(final long theKey,
            final Columns theInsertUpdateColumns,
            final LongArrayList theRemoveCols) {
        removeInternal(theKey, theRemoveCols);
        setInternal(theKey, theInsertUpdateColumns);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#setInternal(long,
     * com.blockwithme.longdb.Columns, com.blockwithme.longdb.Range)
     */
    @Override
    protected void setInternal(final long theKey,
            final Columns theInsertUpdateColumns, final Range theRemoveCols) {
        removeInternal(theKey, theRemoveCols);
        setInternal(theKey, theInsertUpdateColumns);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#sizeInternal()
     */
    @Override
    protected long sizeInternal() {
        return rows.size();
    }

    /** Called when the table was dropped. */
    public void dropped() {
        rows.clear();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#getLimited(long, int)
     */
    @Override
    @CheckForNull
    public Columns getLimited(final long theKey, final int theCount) {
        if (theCount == 0)
            return null;
        final Columns result = get(theKey);
        if (result == null)
            return null;
        final long[] keys = result.columns();
        final int total = keys.length;
        if (total > theCount) {
            for (int i = total - 1; i >= theCount; i--) {
                result.remove(keys[i]);
            }
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#reverseSupported()
     */
    @Override
    public boolean reverseSupported() {
        return false;
    }
}
