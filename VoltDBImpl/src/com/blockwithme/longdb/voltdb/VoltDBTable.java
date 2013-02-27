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
// $codepro.audit.disable methodInvocationInLoopCondition
package com.blockwithme.longdb.voltdb;

import java.util.Iterator;

import javax.annotation.CheckForNull;
import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;

import com.blockwithme.longdb.Columns;
import com.blockwithme.longdb.Range;
import com.blockwithme.longdb.base.AbstractTable;
import com.blockwithme.longdb.entities.Base36;
import com.blockwithme.longdb.entities.Bytes;
import com.blockwithme.longdb.entities.LongHolder;
import com.blockwithme.longdb.exception.DBException;
import com.carrotsearch.hppc.ByteArrayList;
import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.LongCursor;

/** The BETable implementation for VoltDB. */
@ParametersAreNonnullByDefault
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "CD_CIRCULAR_DEPENDENCY", justification = "This is our base class design")
public class VoltDBTable extends
        AbstractTable<VoltDBBackend, VoltDatabase, VoltDBTable> {
    /** The Constant FULL_RANGE. */
    private static final Range FULL_RANGE = Range.fullRange();

    /** Logger for this class */
    private static final Logger LOG = LoggerFactory
            .getLogger(VoltDBTable.class);

    /** Converts the 'results' (VoltTable[]) into Columns object */
    @CheckForNull
    private static Columns constructColumns(final VoltTable[] theResults) {
        final LongObjectOpenHashMap<Bytes> map = new LongObjectOpenHashMap<Bytes>();
        if (theResults.length > 0) {
            final VoltTable table = theResults[0];
            int count = 0;
            while (table.advanceRow()) {
                final VoltTableRow row = table.fetchRow(count++);
                map.put(row.getLong(1), new Bytes(row.getVarbinary(2)));
            }
            if (map.size() > 0)
                return new Columns(map, false);
        }
        return null;
    }

    /** The method combines the blobs present in All the columns into a single
     * byte array, this is a work around for the issues mentioned here :
     * https://github.com/skunkiferous/PaintedBoxes/issues/4 */
    private static void constructParameters(final Columns theColumns,
            final LongArrayList theColumnIds, final ByteArrayList theBlobs,
            final IntArrayList theIndexArray) {
        int currentIndex = 0;
        for (final LongHolder colId : theColumns) {
            theColumnIds.add(colId.value());
            final Bytes byts = theColumns.getBytes(colId.value());
            assert (byts != null);
            final byte[] blob = byts.toArray(false);
            theBlobs.add(blob, 0, blob.length);
            currentIndex += blob.length;
            if (theColumns.size() - 1 > theIndexArray.size())// check if not
                                                             // last
                theIndexArray.add(currentIndex);
        }
    }

    /** capitalizes the first char. */
    public static String capitalize(final String theLine) {
        return Character.toUpperCase(theLine.charAt(0)) + theLine.substring(1);
    }

    /* This constructor will is used for initializing a existing DB */
    /** Instantiates a new volt db table.
     * 
     * @param theDB
     *        the database
     * @param theTableName
     *        the table name */
    protected VoltDBTable(final VoltDatabase theDB, final String theTableName) {
        super(theDB, Base36.get(theTableName), false, false);
    }

    /** returns the Client object encapsulated in VoltDBBackend object. */
    private Client getClient() {
        return database.backend().client();
    }

    /** Capitalize the first character of the table name to generate the stored
     * procedure names, Stored procedure names are case sensitive and contain
     * corresponding table names. */
    private String tableName() {
        return capitalize(table.toString());
    }

    /** Called when the table was closed. */
    @Override
    protected void closeInternal() {
        // NOP
    }

    /** Returns number of columns for a row key.
     * 
     * @param theKey
     *        the row key
     * @return the count of column in the selected row. */
    @Override
    protected long columnsCountInternal(final long theKey) {
        long count = 0;
        try {
            final ClientResponse res = getClient().callProcedure(
                    "SelectColumnCount" + tableName(), theKey);
            final VoltTable tab = res.getResults()[0];
            count = tab.fetchRow(0).getLong(0);
        } catch (final Exception e) {
            throw new DBException("Error performing SelectColumnCount Query: "
                    + theKey, e);
        }
        return count;
    }

    /** Returns all columnIds for a row.
     * 
     * @param theKey
     *        the row key
     * @return the list of column ids to be retrieved. */
    @Override
    @CheckForNull
    protected LongArrayList columnsInternal(final long theKey) {
        final LongArrayList cols = new LongArrayList();
        try {
            final ClientResponse res = getClient().callProcedure(
                    "SelectColAllIds" + tableName(), theKey);
            final VoltTable[] results = res.getResults();
            final VoltTable table = results[0];
            int count = 0;
            while (table.advanceRow()) {
                final VoltTableRow row = table.fetchRow(count++);
                cols.add(row.getLong(1));
            }
            if (cols.size() > 0)
                return cols;
        } catch (final Exception e) {
            throw new DBException("Error performing SelectColAllIds Query: "
                    + theKey, e);
        }
        return null;
    }

    /** Returns iterator of all columnIds for a row.
     * 
     * @param theKey
     *        the row key
     * @return the column id iterator */
    @Override
    @CheckForNull
    protected Iterator<LongHolder> columnsIteratorInternal(final long theKey) {
        final LongArrayList cols = columnsInternal(theKey);
        if ((cols != null) && !cols.isEmpty()) {
            final Iterator<LongCursor> iter = cols.iterator();
            final LongHolder holder = new LongHolder();
            return new Iterator<LongHolder>() {
                @Override
                public boolean hasNext() {
                    return iter.hasNext();
                }

                @Override
                public LongHolder next() {
                    holder.setRaw(iter.next().value);
                    return holder;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
        return null;
    }

    /** Returns Column entries for a row, filtered by a set of columnIds.
     * 
     * @param theKey
     *        the row key
     * @param theColumns
     *        the list of column ids to be retrieved.
     * @return the retrieved column data. */
    @Override
    protected Columns getInternal(final long theKey,
            final LongArrayList theColumns) {
        if (theColumns == null || theColumns.size() == 0) {
            return null;
        }
        try {
            final long[] colIds = theColumns.toArray();
            final ClientResponse res = getClient().callProcedure(
                    "SelectInList" + tableName(), theKey, colIds);
            final VoltTable[] results = res.getResults();
            return constructColumns(results);
        } catch (final Exception e) {
            throw new DBException("Error performing SelectInList Query: "
                    + theColumns, e);
        }
    }

    /** Returns Column entries for a row, filtered by a range of columnIds.
     * 
     * @param theKey
     *        the row key
     * @param theRange
     *        the range of column ids to be retrieved.
     * @return the retrieved columns data. */
    @Override
    @CheckForNull
    protected Columns getInternal(final long theKey, final Range theRange) {
        if (theRange == null || theRange.empty())
            return null;
        try {
            final long start;
            final long end;
            // TODO: If theRange.start() > theRange.end(), then empty() is true,
            // so this is pointless!
            start = (theRange.start() < theRange.end()) ? theRange.start()
                    : theRange.end();
            end = (theRange.start() < theRange.end()) ? theRange.end()
                    : theRange.start();
            final ClientResponse res = getClient().callProcedure(
                    "SelectColumnInRange" + tableName(), theKey, start, end);
            final VoltTable[] results = res.getResults();
            return constructColumns(results);
        } catch (final Exception e) {
            throw new DBException(
                    "Error performing SelectColumnInRange Query(long key="
                            + theKey + ", Range rng=" + theRange + ")", e);
        }
    }

    /** Returns rowId iterator. */
    @Override
    protected Iterator<LongHolder> keysInternal() {
        return new VoltDBKeyIterator(this);
    }

    /** Called when the table is opened. */
    @Override
    protected void openInternal() {
        closed = false;
    }

    /** Removes columns in List specified by 'remove'.
     * 
     * @param theKey
     *        the row key
     * @param theRemoveCols
     *        the list of column ids to be removed */
    @Override
    protected void removeInternal(final long theKey,
            final LongArrayList theRemoveCols) {
        final LongArrayList colIds = new LongArrayList();
        final ByteArrayList blobs = new ByteArrayList();
        final IntArrayList indexArray = new IntArrayList();
        try {
            getClient().callProcedure("InsertUpdateRemove" + tableName(),
                    theKey, colIds.toArray(), blobs.toArray(),
                    indexArray.toArray(), theRemoveCols.toArray());

        } catch (final Exception e) {
            throw new DBException(
                    "Error performing InsertUpdateRemove Query(long key="
                            + theKey + ", LongArrayList remove="
                            + theRemoveCols + ")", e);
        }
    }

    /** Removes columns in Range specified by 'remove'.
     * 
     * @param theKey
     *        the row key
     * @param theRemoveCols
     *        the range of column ids to be removed */
    @Override
    protected void removeInternal(final long theKey, final Range theRemoveCols) {
        if (theRemoveCols.empty())
            return;
        final LongArrayList colIds = new LongArrayList();
        final ByteArrayList blobs = new ByteArrayList();
        final IntArrayList indexArray = new IntArrayList();

        try {
            long start;
            long end;
            if (!theRemoveCols.equals(Range.fullRange())) {
                // TODO: If theRange.start() > theRange.end(), then empty() is
                // true, so this is pointless!
                start = (theRemoveCols.start() < theRemoveCols.end()) ? theRemoveCols
                        .start() : theRemoveCols.end();
                end = (theRemoveCols.start() < theRemoveCols.end()) ? theRemoveCols
                        .end() : theRemoveCols.start();
            } else {
                start = Long.MIN_VALUE;
                end = Long.MAX_VALUE;
            }
            getClient().callProcedure("InsertUpdateRemoveRange" + tableName(),
                    theKey, colIds.toArray(), blobs.toArray(),
                    indexArray.toArray(), start, end);

        } catch (final Exception e) {
            LOG.error("Exception Occurred in - removeInternal(long key="
                    + theKey + ", Range remove=" + theRemoveCols + ")", e);
            throw new DBException(
                    "Error performing InsertUpdateRemoveRange Query.", e);
        }
    }

    /** Updates row with columns specified by insertOrUpdate and removes columns
     * in 'remove' List. Remove operation is performed before update.
     * 
     * @param theKey
     *        the row key
     * @param theInsertUpdateColumns
     *        columns to be inserted or updated
     * @param theRemoveCols
     *        column ids to be removed */
    @Override
    protected void setInternal(final long theKey,
            final Columns theInsertUpdateColumns,
            final LongArrayList theRemoveCols) {

        final LongArrayList colIds = new LongArrayList(
                theInsertUpdateColumns.size());
        final ByteArrayList blobs = new ByteArrayList();
        final IntArrayList indexArray = new IntArrayList(
                theInsertUpdateColumns.size() - 1);
        constructParameters(theInsertUpdateColumns, colIds, blobs, indexArray);
        try {
            getClient().callProcedure("InsertUpdateRemove" + tableName(),
                    theKey, colIds.toArray(), blobs.toArray(),
                    indexArray.toArray(), theRemoveCols.toArray());

        } catch (final Exception e) {
            throw new DBException(
                    "Error performing InsertUpdateRemove Query(long key="
                            + theKey + ", Columns insertOrUpdate="
                            + theInsertUpdateColumns
                            + ", LongArrayList remove=" + theRemoveCols + ")",
                    e);
        }
    }

    /** Updates row with columns specified by insertOrUpdate and removes columns
     * in range specified by 'remove'. */
    @Override
    protected void setInternal(final long theKey,
            final Columns theInsertUpdateColumns, final Range theRemoveCols) {
        final LongArrayList colIds = new LongArrayList(
                theInsertUpdateColumns.size());
        final ByteArrayList blobs = new ByteArrayList();
        final IntArrayList indexArray = new IntArrayList(
                theInsertUpdateColumns.size() - 1);
        constructParameters(theInsertUpdateColumns, colIds, blobs, indexArray);

        try {
            final long start;
            final long end;
            // TODO: If theRange.start() > theRange.end(), then empty() is true,
            // so this is pointless!
            start = (theRemoveCols.start() < theRemoveCols.end()) ? theRemoveCols
                    .start() : theRemoveCols.end();
            end = (theRemoveCols.start() < theRemoveCols.end()) ? theRemoveCols
                    .end() : theRemoveCols.start();
            getClient().callProcedure("InsertUpdateRemoveRange" + tableName(),
                    theKey, colIds.toArray(), blobs.toArray(),
                    indexArray.toArray(), start, end);

        } catch (final Exception e) {
            throw new DBException(
                    "Error performing InsertUpdateRemoveRange Query(long key="
                            + theKey + ", Columns insertOrUpdate="
                            + theInsertUpdateColumns + ", Range remove="
                            + theRemoveCols + ")", e);
        }
    }

    /** Returns number of rows in this table. */
    @Override
    protected long sizeInternal() {
        long size = 0;
        try {
            final ClientResponse res = getClient().callProcedure(
                    "SelectRowCount" + tableName());
            final VoltTable tab = res.getResults()[0];
            size = tab.fetchRow(0).getLong(0);
        } catch (final Exception e) {
            throw new DBException("Error performing SelectRowCount Query.", e);
        }
        return size;
    }

    /** Called when the table was dropped. */
    public void dropped() {
        throw new UnsupportedOperationException(
                "This operation is not supported for VoltDB");
    }

    /** Returns a limited number of columns for a row. */
    @Override
    public Columns getLimited(final long theKey, final int theCount) {
        if (theCount == 0) {
            return null;
        }
        try {
            final ClientResponse res = getClient().callProcedure(
                    "SelectColumnLimited" + tableName(), theKey, theCount);
            final VoltTable[] results = res.getResults();
            return constructColumns(results);
        } catch (final Exception e) {
            throw new DBException(
                    "Error performing SelectColumnLimited Query(long key="
                            + theKey + ", int count=" + theCount + ")", e);
        }
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
