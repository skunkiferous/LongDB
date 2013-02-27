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
package com.blockwithme.longdb.bdb;

import java.util.Arrays;
import java.util.Iterator;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blockwithme.longdb.Columns;
import com.blockwithme.longdb.Range;
import com.blockwithme.longdb.base.AbstractTable;
import com.blockwithme.longdb.entities.Base36;
import com.blockwithme.longdb.entities.Bytes;
import com.blockwithme.longdb.entities.LongHolder;
import com.blockwithme.longdb.exception.DBException;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;

/** The BerkeleyDB implementation of BETable interface. */
@ParametersAreNonnullByDefault
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "CD_CIRCULAR_DEPENDENCY", justification = "This is our base class design")
public class BDBTable extends AbstractTable<BDBBackend, BDBDatabase, BDBTable> {
    /** The range object of full range of long numbers. */
    private static final Range FULL_RANGE = Range.fullRange();

    /** Logger for this class */
    private static final Logger LOG = LoggerFactory.getLogger(BDBTable.class);

    /** The success status. */
    private static final OperationStatus SUCCESS = OperationStatus.SUCCESS;

    /** Table entity is represented by BDB Database Entity. */
    private Database dbInstance;

    /** name of the internal database that represents this table. */
    private final String dbName;

    /** The lock mode. */
    private final LockMode lm;

    /** The transactional. */
    private final boolean transactional;

    /** Instantiates a new bDB table.
     * 
     * @param theDB
     *        the database instance
     * @param theDataName
     *        the BDB database instance name
     * @param isReverse
     *        indicates whether columns should be sorted in reverse order */
    protected BDBTable(final BDBDatabase theDB, final String theDataName,
            final boolean isReverse) {
        super(theDB, Base36.get(theDataName), false, false);
        dbName = theDataName;
        final BDBConfig config = theDB.backend().config();
        lm = config.lockMode();
        transactional = config.transactional();

    }

    /* Utility method */
    /** Abort close.
     * 
     * @param theTransaction
     *        the transaction object to be closed
     * @param theCursor
     *        the cur object to be closed. */
    private void abortClose(final Transaction theTransaction,
            final Cursor theCursor) {
        closeCursor(theCursor);
        if (theTransaction != null) {
            try {
                theTransaction.abort();
            } catch (final Exception e) {
                LOG.warn("exception ignored - abortClose(Transaction txn="
                        + theTransaction + ", Cursor cur=" + theCursor + ")", e);
                // Ignore this.
            }
        }
    }

    /** Checks search status and if current cursor key is less or equal to end
     * range key */
    private boolean checkSearch(final OperationStatus theStatus,
            final Database theDB, final DatabaseEntry theCurrentEntry,
            final DatabaseEntry theEndEntry) {
        if (theStatus == SUCCESS
                && theDB.compareKeys(theCurrentEntry, theEndEntry) <= 0)
            return true;
        return false;
    }

    /** Closes cursor. */
    private void closeCursor(final Cursor theCursor) {
        if (theCursor != null) {
            try {
                theCursor.close(); // $codepro.audit.disable closeInFinally
            } catch (final Exception e) {
                LOG.warn("exception ignored - closeCursor(Cursor cur="
                        + theCursor + ")", e);
                // Ignore this.
            }
        }
    }

    /** closes the cursor and commits the transaction.
     * 
     * @param theTransaction
     *        the transaction object
     * @param theCursor
     *        the cursor object */
    private void commitClose(final Transaction theTransaction,
            final Cursor theCursor) {
        closeCursor(theCursor);
        if (theTransaction != null && theTransaction.isValid()) {
            try {
                theTransaction.commit();
            } catch (final Exception e) {
                LOG.warn("exception ignored - commitClose(Transaction txn="
                        + theTransaction + ", Cursor cur=" + theCursor + ")", e);

                // Ignore this.
            }
        }
    }

    /** Deletes data. */
    private void deleteData(final long theKey, final Cursor theCursor,
            final Transaction theTransaction, final long theColumnId) {
        // delete row_col_combination => blob entry.
        theCursor.delete();
        // now delete row_id => col_id entry
        final DatabaseEntry keyOnly = entry(theKey, null);
        final DatabaseEntry colIDOnly = entry(null, theColumnId);
        final Cursor cur2 = newCursor(theTransaction);
        try {
            final OperationStatus st = cur2.getSearchBoth(keyOnly, colIDOnly,
                    lm);
            if (st == SUCCESS) {
                cur2.delete();
            }
        } finally {
            cur2.close();
        }
    }

    /** creates DatabaseEntry object using required input. */
    // TODO It is my understanding, that one can reuse the same DatabaseEntry
    // multiple times, so it is probably inefficient to call this method in a
    // loop
    @CheckForNull
    private DatabaseEntry entry(final Long theKey, final Long theColID) {
        // TODO This method looks very suspicious. If it is correct,
        // then maybe it should be split in two. It doesn't make sense for
        // someone who doesn't know the code, that the method returns the
        // same output on (X, null) and (null, X) ...
        // In other words, it violates the:
        // http://en.wikipedia.org/wiki/Principle_of_least_astonishment
        if (theKey != null && theColID != null)
            return new DatabaseEntry(DataConversionUtil.combine(theKey,
                    theColID));
        else if (theKey != null)
            return DataConversionUtil.longtoEntry(theKey);
        else if (theColID != null)
            return DataConversionUtil.longtoEntry(theColID);
        return null;
    }

    /** Gets the next cursor value.
     * 
     * @param theCursor
     *        the cursor
     * @param theKeyEntry
     *        the key entry will get populated with next key.
     * @param theDataEntry
     *        the data entry will get populated with next entry.
     * @param theDuplicate
     *        if false skips all the duplicate values and goes to next
     *        non-duplicate value
     * @return operation status */
    private OperationStatus getNext(final Cursor theCursor,
            final DatabaseEntry theKeyEntry, final DatabaseEntry theDataEntry,
            final boolean theDuplicate) {
        if (theCursor == null)
            return OperationStatus.NOTFOUND;
        if (theDuplicate)
            return theCursor.getNextDup(theKeyEntry, theDataEntry, lm);
        else
            return theCursor.getNext(theKeyEntry, theDataEntry, lm);
    }

    /** Initializes the cursor and sets the position to a particular entry. */
    @CheckForNull
    private Cursor initCursor(final DatabaseEntry theRowEntry) {
        final Cursor cur = dbInstance.openCursor(null, null);
        final OperationStatus s = cur.getSearchKey(theRowEntry,
                new DatabaseEntry(), lm);
        if (s == SUCCESS) {
            return cur;
        }
        // close if status not successful
        commitClose(null, cur);
        return null;
    }

    /** creates a new cursor.
     * 
     * @param theTransaction
     *        the transaction object
     * @return the cursor */
    private Cursor newCursor(final Transaction theTransaction) {
        return dbInstance.openCursor(theTransaction, null);
    }

    /** creates a new transaction.
     * 
     * @return the transaction */
    @CheckForNull
    private Transaction newTransaction() {
        if (transactional) {
            return dbInstance.getEnvironment().beginTransaction(null,
                    TransactionConfig.DEFAULT);
        } else
            return null;
    }

    /** Removes the in list.
     * 
     * @param theKey
     *        the key
     * @param theRemoveCols
     *        the remove
     * @param theTransaction
     *        the transaction object */
    private void removeInList(final long theKey,
            final LongArrayList theRemoveCols, final Transaction theTransaction) {
        final Cursor cur = newCursor(theTransaction);
        try {
            for (final LongCursor longCursor : theRemoveCols) {
                final DatabaseEntry keyEntry = entry(theKey, longCursor.value);
                final OperationStatus s = cur.getSearchKey(keyEntry,
                        new DatabaseEntry(), lm);
                if (s == SUCCESS) {
                    deleteData(theKey, cur, theTransaction, longCursor.value);
                }
            }
        } finally {
            closeCursor(cur);
        }
    }

    /** Removes the entries in a range.
     * 
     * @param theKey
     *        the row key
     * @param theRemoveCols
     *        the remove
     * @param theTransaction
     *        the transaction object */
    private void removeInRange(final long theKey, final Range theRemoveCols,
            final Transaction theTransaction) {
        final Cursor cur = newCursor(theTransaction);
        try {
            final DatabaseEntry currentEntry = entry(theKey,
                    theRemoveCols.start());
            final DatabaseEntry endEntry = entry(theKey, theRemoveCols.end());
            OperationStatus s = cur.getSearchKeyRange(currentEntry,
                    new DatabaseEntry(), lm);
            while (s == SUCCESS) {
                // compare key if less than high range then delete data.
                if (cur.getDatabase().compareKeys(currentEntry, endEntry) <= 0) {
                    final long colid = DataConversionUtil
                            .splitColumn(currentEntry.getData());
                    deleteData(theKey, cur, theTransaction, colid);
                } else {
                    // break the loop.
                    break;
                }
                s = getNext(cur, currentEntry, new DatabaseEntry(), false);
            }
        } finally {
            closeCursor(cur);
        }
    }

    /* Method for update/insert row-column combination */
    /** Sets the only.
     * 
     * @param theKey
     *        the key
     * @param theInsertUpdateColumns
     *        the insert or update
     * @param theTransaction
     *        the transaction object */
    private void setOnly(final long theKey,
            final Columns theInsertUpdateColumns,
            final Transaction theTransaction) {
        final Cursor cur = newCursor(theTransaction);
        try {

            for (final LongHolder longCursor : theInsertUpdateColumns) {
                final DatabaseEntry keyEntry = entry(theKey, longCursor.value());
                final Bytes byts = theInsertUpdateColumns.getBytes(longCursor
                        .value());
                assert (byts != null);
                final DatabaseEntry valueEntry = new DatabaseEntry(
                        byts.toArray(false));

                final OperationStatus s = cur.getSearchKey(keyEntry,
                        new DatabaseEntry(), lm);

                if (s == SUCCESS) {
                    // this means row/column combination already exists.
                    cur.delete(); // delete is necessary to ensure duplicate is
                    // not inserted.
                    cur.put(keyEntry, valueEntry);
                } else {
                    // this means we have to create a new row/column
                    // combination.
                    // insert row_col_combination => blob (duplicates allowed)
                    cur.put(keyEntry, valueEntry);
                    // insert rowId => col_id (duplicates allowed)
                    final DatabaseEntry keyOnly = entry(theKey, null);
                    final DatabaseEntry colIDOnly = entry(null,
                            longCursor.value());
                    dbInstance.put(theTransaction, keyOnly, colIDOnly);
                }
            }
        } catch (final Exception e) {
            LOG.error("Exception Occurred in - setOnly(long key=" + theKey
                    + ", Columns insertOrUpdate=" + theInsertUpdateColumns
                    + ", Transaction txn=" + theTransaction + ")", e);
            abortClose(theTransaction, cur);
            throw new DBException("Error performing Insert Update Operation.",
                    e);
        } finally {
            commitClose(theTransaction, cur);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#closeInternal()
     */
    @Override
    protected void closeInternal() {
        if (dbInstance != null) {
            dbInstance.close(); // $codepro.audit.disable closeInFinally
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#columnsCountInternal(long)
     */
    @Override
    protected long columnsCountInternal(final long theKey) {
        final long count = 0;
        final DatabaseEntry rowEntry = entry(theKey, null);
        final Cursor cur = initCursor(rowEntry);
        try {
            if (cur != null)
                // returns count of duplicate entries, which is nothing but the
                // number of columns
                return cur.count();
        } catch (final Exception e) {
            LOG.error("Exception Occurred in - columnsCountInternal(long key="
                    + theKey + ")", e);
            throw new DBException("Error performing Column Count", e);
        } finally {
            commitClose(null, cur);
        }
        return count;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#columnsInternal(long)
     */
    @Override
    @CheckForNull
    protected LongArrayList columnsInternal(final long theKey) {
        /** Create a new Cursor, Use cursor.SearchKey() to move to first entry
         * for the row. Use cursor.getNextDup() to get next column entry for the
         * same row. */
        final LongArrayList cols = new LongArrayList();
        final DatabaseEntry rowEntry = entry(theKey, null);
        final Transaction txn = newTransaction();
        final Cursor cur = newCursor(txn);
        try {
            // TODO How do we read only the columns?
            final DatabaseEntry ve = new DatabaseEntry();
            OperationStatus s = cur.getSearchKey(rowEntry, ve, lm);
            while (s == SUCCESS) {
                final long columnId = DataConversionUtil.toLong(ve.getData());
                cols.add(columnId);
                s = getNext(cur, rowEntry, ve, true);
            }
            if (cols.size() > 0)
                return cols;
        } catch (final Exception e) {
            LOG.error("Exception Occurred in - columnsInternal(long key="
                    + theKey + ")", e);
            abortClose(txn, cur);
            throw new DBException("Error performing All Column Id Search.", e);
        } finally {
            commitClose(txn, cur);
        }
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#columnsIteratorInternal(long)
     */
    @Override
    @CheckForNull
    protected Iterator<LongHolder> columnsIteratorInternal(final long theKey) {
        final LongArrayList list = columnsInternal(theKey);
        if (list != null)
            return new Iterator<LongHolder>() {
                /** The LongHolder */
                private final LongHolder holder = new LongHolder();

                /** The real iterator. */
                private final Iterator<LongCursor> iter = list.iterator();

                /*
                 * (non-Javadoc)
                 * 
                 * @see java.util.Iterator#hasNext()
                 */
                @Override
                public boolean hasNext() {
                    return iter.hasNext();
                }

                /*
                 * (non-Javadoc)
                 * 
                 * @see java.util.Iterator#next()
                 */
                @Override
                public final LongHolder next() {
                    final LongCursor next = iter.next();
                    holder.value(next.value);
                    return holder;
                }

                /*
                 * (non-Javadoc)
                 * 
                 * @see java.util.Iterator#remove()
                 */
                @Override
                public void remove() {
                    iter.remove();
                }
            };
        return null;
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
            @Nullable final LongArrayList theColumns) {
        /** Sorts the input columnIDs in ascending order. Creates a cursor.
         * Creates a rowIDcolID combination using the rowID and first columnID
         * in columns list. Sets the cursor position to the first rowIDcolID
         * combination. Moves next position using cur.getSearchKey() with next
         * rowIDcolID combination from there on. NOTE : Sorting of input column
         * Ids should be done in reverse order when we start supporting reverse
         * column sorting */
        if (theColumns == null || theColumns.isEmpty()) {
            return null;
        }
        final long[] colIDs = theColumns.toArray();
        Arrays.sort(colIDs);

        final LongObjectOpenHashMap<Bytes> map = new LongObjectOpenHashMap<Bytes>();
        final Transaction txn = newTransaction();
        final Cursor cur = newCursor(txn);
        try {
            final DatabaseEntry dataEntry = new DatabaseEntry();
            for (final long colID : colIDs) {
                final DatabaseEntry currentEntry = entry(theKey, colID);
                final OperationStatus s = cur.getSearchKey(currentEntry,
                        dataEntry, lm);
                if (s == OperationStatus.SUCCESS) {
                    final long columnId = DataConversionUtil
                            .splitColumn(currentEntry.getData());
                    map.put(columnId, new Bytes(dataEntry.getData()));
                }
            }

        } catch (final Exception e) {
            LOG.error("Exception Occurred in - getInternal(long key=" + theKey
                    + ", LongArrayList cols=" + theColumns + ")", e);
            abortClose(txn, cur);
            throw new DBException("Error Select In List Search.", e);
        } finally {
            commitClose(txn, cur);
        }
        return new Columns(map, false);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#getInternal(long,
     * com.blockwithme.longdb.Range)
     */
    @Override
    @CheckForNull
    protected Columns getInternal(final long theKey, final Range theRange) {
        /** Creates a cursor. Creates a rowIDcolID combination using row id and
         * range.start and sets the cursor position to nearest next position
         * using cursor.getSearchKeyRange(), Moves to the next position by
         * incrementing the current colunmId and searching next combination
         * using cursor.getSearchKeyRange(). NOTE : This logic should be
         * modified when reverse column sorting is supported. */
        if (theRange == null || theRange.empty())
            return null;

        final LongObjectOpenHashMap<Bytes> map = new LongObjectOpenHashMap<Bytes>();
        final Transaction txn = newTransaction();
        final Cursor cur = newCursor(txn);
        try {
            long currentColumn = theRange.start();
            final DatabaseEntry endEntry = entry(theKey, theRange.end());
            final DatabaseEntry dataEntry = new DatabaseEntry();

            while (true) { // $codepro.audit.disable
                           // constantConditionalExpression
                final DatabaseEntry currentEntry = entry(theKey, currentColumn);
                final OperationStatus s = cur.getSearchKeyRange(currentEntry,
                        dataEntry, lm);
                if (checkSearch(s, cur.getDatabase(), currentEntry, endEntry)) {
                    final long columnId = DataConversionUtil
                            .splitColumn(currentEntry.getData());
                    currentColumn = columnId + 1;
                    map.put(columnId, new Bytes(dataEntry.getData()));
                } else {
                    break;
                }
            }
            if (map.size() > 0)
                return new Columns(map, false);

        } catch (final Exception e) {
            LOG.error("Exception Occurred in - getInternal(long key=" + theKey
                    + ", Range rng=" + theRange + ")", e);
            throw new DBException("Error performing Select In Range Search.", e);
        } finally {
            // close if status not successful
            commitClose(txn, cur);
        }
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#keysInternal()
     */
    @Override
    protected Iterator<LongHolder> keysInternal() {
        return new BDBKeyIterator(this);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#openInternal()
     */
    @Override
    protected void openInternal() {
        dbInstance = database.env().openDatabase(null, this.dbName,
                database.dbConfig());
        closed = false;
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
        final Transaction txn = newTransaction();
        try {
            removeInList(theKey, theRemoveCols, txn);
        } catch (final Exception e) {
            LOG.error("Exception Occurred in - removeInternal(long key="
                    + theKey + ", LongArrayList remove=" + theRemoveCols + ")",
                    e);
            abortClose(txn, null);
            throw new DBException("Error performing remove Operation ", e);
        } finally {
            commitClose(txn, null);
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
        if (theRemoveCols.empty())
            return;
        final Transaction txn = newTransaction();
        try {
            removeInRange(theKey, theRemoveCols, txn);
        } catch (final Exception e) {
            LOG.error("Exception Occurred in - removeInternal(long key="
                    + theKey + ", Range remove=" + theRemoveCols + ")", e);
            abortClose(txn, null);
            throw new DBException(
                    "Error performing Remove In Range Operation.", e);
        } finally {
            // close if status not successful
            commitClose(txn, null);
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
        final Transaction txn = newTransaction();
        try {
            removeInList(theKey, theRemoveCols, txn);
            setOnly(theKey, theInsertUpdateColumns, txn);
        } catch (final Exception e) {
            LOG.error("Exception Occurred in - setInternal(long key=" + theKey
                    + ", Columns insertOrUpdate=" + theInsertUpdateColumns
                    + ", LongArrayList remove=" + theRemoveCols + ")", e);
            abortClose(txn, null);
            throw new DBException("Exception while updating data.", e);
        } finally {
            commitClose(txn, null);
        }
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
        final Transaction txn = newTransaction();
        try {
            removeInRange(theKey, theRemoveCols, txn);
            setOnly(theKey, theInsertUpdateColumns, txn);
        } catch (final Exception e) {
            LOG.error("Exception Occurred in - setInternal(long key=" + theKey
                    + ", Columns insertOrUpdate=" + theInsertUpdateColumns
                    + ", Range remove=" + theRemoveCols + ")", e);
            abortClose(txn, null);
            throw new DBException("Exception while updating data.", e);
        } finally {
            commitClose(txn, null);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#sizeInternal()
     */
    @Override
    protected long sizeInternal() {
        /** Iterates over keys to get number of rows. TODO: for better
         * performance maintain special key-value pairs for row counter. */
        try {
            long count = 0;
            final Iterator<LongHolder> itr = this.keys();
            while (itr.hasNext()) {
                itr.next();
                count++;
            }
            return count;
        } catch (final Exception e) {
            LOG.error("Exception Occurred in - sizeInternal()", e);
            throw new DBException("Error obtaining row count.", e);
        }
    }

    /** Database instance. */
    public Database dbInstance() {
        return dbInstance;
    }

    /** Called when the table was dropped. */
    public void dropped() {
        if (dbInstance != null) {
            dbInstance.close(); // $codepro.audit.disable closeInFinally
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#getLimited(long, int)
     */
    @Override
    @CheckForNull
    public Columns getLimited(final long theKey, final int theCount) {
        if (theCount == 0) {
            return null;
        }

        DatabaseEntry rowEntry = entry(theKey, null);
        final DatabaseEntry valueEntry = new DatabaseEntry();
        final LongObjectOpenHashMap<Bytes> map = new LongObjectOpenHashMap<Bytes>();
        final Transaction txn = newTransaction();
        final Cursor cur = newCursor(txn);
        OperationStatus s = cur.getSearchKey(rowEntry, valueEntry, lm);
        try {
            if (s == SUCCESS) {
                final long column = DataConversionUtil.toLong(valueEntry
                        .getData());
                rowEntry = entry(theKey, column);
                int currCount = 0;
                s = cur.getSearchKey(rowEntry, valueEntry, lm);
                while (SUCCESS == s) {
                    final long columnId = DataConversionUtil
                            .splitColumn(rowEntry.getData());
                    final long rowId = DataConversionUtil.splitRow(rowEntry
                            .getData());
                    if (rowId == theKey)
                        map.put(columnId, new Bytes(valueEntry.getData()));
                    if (++currCount >= theCount)
                        break;
                    getNext(cur, rowEntry, valueEntry, false);
                }
            }

        } catch (final Exception e) {
            LOG.error("Exception Occurred in - getLimited(long key=" + theKey
                    + ", int count=" + theCount + ")", e);
            abortClose(txn, cur);
            throw new DBException("Error performing Get limited Operation.", e);
        } finally {
            commitClose(txn, cur);
        }
        return new Columns(map, false);
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
