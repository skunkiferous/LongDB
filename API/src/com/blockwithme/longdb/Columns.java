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

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.annotation.CheckForNull;
import javax.annotation.ParametersAreNonnullByDefault;

import com.blockwithme.longdb.entities.Bytes;
import com.blockwithme.longdb.entities.LongHolder;
import com.carrotsearch.hppc.LongLongOpenHashMap;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.LongLongCursor;
import com.carrotsearch.hppc.cursors.LongObjectCursor;

/** Represent *some* columns from a table row. */
@ParametersAreNonnullByDefault
public class Columns implements Iterable<LongHolder> {

    /** The default initial capacity of the columns map. */
    public static final int DEFAULT_CAPACITY = 16;

    /** The Laod factor */
    private static final float LOAD_FACTOR = LongObjectOpenHashMap.DEFAULT_LOAD_FACTOR;

    /** Bytes Map */
    private final LongObjectOpenHashMap<Bytes> bmap;

    /** Reverse order? */
    private final boolean reverse;

    /** The timestamp, if any. */
    private Long timestamp;

    /** Creates an empty Columns.
     * 
     * @param theReverse
     *        the reverse */
    public Columns(final boolean theReverse) {
        this(theReverse, DEFAULT_CAPACITY);
    }

    /** Creates an empty Columns, with initial capacity.
     * 
     * @param theReverse
     *        if reverse sorting is applicable.
     * @param theInitialCapacity
     *        the initial capacity */
    public Columns(final boolean theReverse, final int theInitialCapacity) {
        this.reverse = theReverse;
        bmap = new LongObjectOpenHashMap<Bytes>(theInitialCapacity);
    }

    /** Copies the content of the given Columns.
     * 
     * @param theRow
     *        the row */
    public Columns(final Columns theRow) {
        this(theRow.reverse, 1 + (int) (LOAD_FACTOR * theRow.size()));
        putAll(theRow);
    }

    /** Copies the content of the given map.
     * 
     * @param theMap
     *        the map
     * @param theReverse
     *        if reverse sorting is applicable. */
    public Columns(final LongObjectOpenHashMap<Bytes> theMap,
            final boolean theReverse) {
        this(theReverse, 1 + (int) (LOAD_FACTOR * theMap.size()));
        putAll(theMap);
    }

    /** Copies the content of the map. */
    private void putAll2(final LongObjectOpenHashMap<Bytes> theMap) {
        for (final LongObjectCursor<Bytes> cursor : theMap) {
            final long col = cursor.key;
            // Makes a safe copy from Bytes, in case the array in Bytes gets
            // changed by the caller later.
            final Bytes value = cursor.value.copy();
            bmap.put(col, value);
        }
    }

    /** Returns the timestamp, if any. The timestamp is used to detect concurrent
     * modification (collisions). If that case, the transaction is aborted.
     * 
     * @return the timestamp */
    Long getTimestamp() {
        return timestamp;
    }

    /** Sets the timestamp.
     * 
     * @param theTimestamp
     *        the new timestamp */
    void setTimestamp(final Long theTimestamp) {
        this.timestamp = theTimestamp;
    }

    /** Iterates over the bytes. Sort order is random.
     * 
     * @return the iterator */
    public Iterator<LongObjectCursor<Bytes>> bytesIterator() {
        return bmap.iterator();
    }

    /** Returns the columns in sorted order.
     * 
     * @return the column ids array */
    public long[] columns() {
        return columns(true);
    }

    /** Returns the columns.
     * 
     * @param theSorted
     *        if sorting is needed
     * @return the column Ids array */
    public long[] columns(final boolean theSorted) {
        final long[] columns = bmap.keys().toArray();
        if (theSorted) {
            final int count = columns.length;
            Arrays.sort(columns);
            if (reverse && (count > 1)) {
                // TODO This is crazy! There must be a reverse() function
                // somewhere. Guava?
                final int stop = count / 2;
                final int top = count - 1;
                for (int i = 0; i < stop; i++) {
                    final long tmp = columns[i];
                    final int other = top - i;
                    columns[i] = columns[other];
                    columns[other] = tmp;
                }
            }
        }
        return columns;
    }

    /** Returns true if we contain the column.
     * 
     * @param theColumn
     *        the column
     * @return true, if exists */
    public boolean containsColumn(final long theColumn) {
        return bmap.containsKey(theColumn);
    }

    /** Returns a copy of this object.
     * 
     * @return the columns */
    public Columns copy() {
        return new Columns(this);
    }

    /** Returns a primitive value as long.
     * 
     * @param theColumn
     *        the column id
     * @return the bytes */
    @CheckForNull
    public Bytes getBytes(final long theColumn) {
        return bmap.get(theColumn);
    }

    /** Iterates over the sorted columns.
     * 
     * @return the iterator */
    @Override
    public Iterator<LongHolder> iterator() {
        return iterator(true);
    }

    /** Iterates over the columns.
     * 
     * @param isSorted
     *        if sorting is needed
     * @return the iterator */
    public Iterator<LongHolder> iterator(final boolean isSorted) {
        final long[] columns = columns(isSorted);
        final int count = columns.length;
        return new Iterator<LongHolder>() {
            /** The LongHolder */
            private final LongHolder holder = new LongHolder();

            /** The next column. */
            private int next;

            /** Was the current element already removed? */
            private boolean removed;

            /*
             * (non-Javadoc)
             * 
             * @see java.util.Iterator#hasNext()
             */
            @Override
            public boolean hasNext() {
                return (next < count);
            }

            /*
             * (non-Javadoc)
             * 
             * @see java.util.Iterator#next()
             */
            @Override
            public final LongHolder next() {
                if (next < count) {
                    holder.value(columns[next++]);
                    removed = false;
                    return holder;
                }
                throw new NoSuchElementException("No more values.");
            }

            /*
             * (non-Javadoc)
             * 
             * @see java.util.Iterator#remove()
             */
            @Override
            public void remove() {
                if (next == 0) {
                    throw new IllegalStateException("next() was never called");
                }
                if (removed) {
                    throw new IllegalStateException("element already removed!");
                }
                Columns.this.remove(columns[next - 1]);
                removed = true;
            }
        };
    }

    /** Copies the content of the row.
     * 
     * @param theRow
     *        the row */
    public final void putAll(final Columns theRow) {
        putAll(theRow.bmap);
    }

    /** Copies the content of the map.
     * 
     * @param theMap
     *        the map */
    public void putAll(final LongLongOpenHashMap theMap) {
        for (final LongLongCursor cursor : theMap) {
            bmap.put(cursor.key, new Bytes(cursor.value));
        }
    }

    /** Copies the content of the map.
     * 
     * @param theMap
     *        the map */
    public final void putAll(final LongObjectOpenHashMap<Bytes> theMap) {
        putAll2(theMap);
    }

    /** Sets a primitive value as long.
     * 
     * @param theColumn
     *        the column id
     * @param theValue
     *        the value
     * @return the 'previous value' in that column bytes, null if there was no
     *         value */
    @CheckForNull
    public Bytes putBytes(final long theColumn, final Bytes theValue) {
        return bmap.put(theColumn, theValue);
    }

    /** Removes the column.
     * 
     * @param theColumn
     *        the column */
    public void remove(final long theColumn) {
        bmap.remove(theColumn);
    }

    /** Sets a primitive value as long.
     * 
     * @param theColumn
     *        the column id
     * @return the 'previous value' in that column bytes, null if there was no
     *         value */
    @CheckForNull
    public Bytes removeBytes(final long theColumn) {
        return bmap.remove(theColumn);
    }

    /** The number of columns contained.
     * 
     * @return the size */
    public int size() {
        return bmap.size();
    }
}
