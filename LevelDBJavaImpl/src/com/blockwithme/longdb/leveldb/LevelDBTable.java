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
package com.blockwithme.longdb.leveldb;

import static com.blockwithme.longdb.common.constants.ByteConstants.LONG_BYTES;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;

import javax.annotation.CheckForNull;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.commons.lang.ArrayUtils;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBFactory;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.impl.Iq80DBFactory;
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

/** The Class LevelDBTable. */
@ParametersAreNonnullByDefault
public class LevelDBTable extends
        AbstractTable<LevelDBBackend, LevelDBDatabase, LevelDBTable> {

    /** The leveldb factory */
    private static final DBFactory FCTRY = Iq80DBFactory.factory;

    /** The range object with full range of long numbers. */
    private static final Range FULL_RANGE = Range.fullRange();

    /** Logger for this class */
    private static final Logger LOG = LoggerFactory
            .getLogger(LevelDBTable.class);

    /** The bit wise max value (all bits on). */
    private static final long MAX_COLUMN = -1L;

    /** The bin wise min value (all bits off). */
    private static final long MIN_COLUMN = 0L;

    /** The byte wise comparator. */
    private BytewiseComparator comparator;

    /** The connected flag */
    private boolean connected;

    /** The folder of database files */
    private final File dbFolder;

    /** The db instance. */
    private DB dbInstance;

    /** The default options. */
    private Options defaultOptions;

    /** The read opts. */
    private ReadOptions readOpts;

    /** The write opts. */
    private WriteOptions writeOpts;

    /** Internal method: To bytes array */
    private static byte[] toArray(final Bytes theBytes) {
        return theBytes.toArray(false);
    }

    /** Instantiates a new level db table.
     * 
     * @param theDB
     *        the database object
     * @param theDBfolder
     *        the database folder
     * @param theName
     *        the table
     * @param isReverse
     *        the reverse column sorting flag */
    protected LevelDBTable(final LevelDBDatabase theDB, final File theDBfolder,
            final Base36 theName, final boolean isReverse) {
        super(theDB, theName, false, false);
        init();
        dbFolder = theDBfolder;
    }

    /** Internal method : Checks range. */
    private Entry<byte[], byte[]> checkRange(
            final Entry<byte[], byte[]> theEntry, final long theLimit,
            final long theRowKey, final boolean isLastInclude) {
        if (theEntry == null)
            return null;
        if (isLastInclude
                && comparator.compare(theEntry.getKey(),
                        Util.combine(theRowKey, theLimit)) <= 0)
            return theEntry;
        else if (comparator.compare(theEntry.getKey(),
                Util.combine(theRowKey, theLimit)) < 0)
            return theEntry;
        return null;
    }

    /** Internal method: resolves the config object */
    private LevelDBConfig config() {
        return ((LevelDBBackend) database.backend()).config();
    }

    /** Connect. */
    private void connect() {
        if (connected)
            return;
        try {
            defaultOptions.createIfMissing(true);
            dbInstance = FCTRY.open(dbFolder, defaultOptions);
            connected = true;
        } catch (final Exception e) {
            LOG.error("Exception Occurred in - connect()", e);
            throw new DBException("Error creating connection.", e);
        }
    }

    /** Gets the results in specified range and list. */
    @CheckForNull
    private void getInRangeAndList(final long theKey, final long theStart,
            final long theEnd, final LongArrayList theColumns,
            final LongObjectOpenHashMap<Bytes> theMap) {

        // final LongObjectOpenHashMap<Bytes> map = new
        // LongObjectOpenHashMap<Bytes>();
        DBIterator itr = null;
        try {
            itr = iteratorAtPosition(theKey, theStart);
            java.util.Map.Entry<byte[], byte[]> entry = null;
            while (itr.hasNext()
                    && (entry = checkRange(itr.next(), theEnd, theKey, true)) != null) {
                final long colId = Util.splitColumn(entry.getKey());
                if (theColumns.contains(colId))
                    theMap.put(colId, new Bytes(entry.getValue()));
            }
        } catch (final Exception e) {
            LOG.error("Exception Occurred in - getInRangeAndList(long key="
                    + theKey + ", long start=" + theStart + ", long end="
                    + theEnd + ", LongArrayList cols=" + theColumns
                    + ", LongObjectOpenHashMap<Bytes> map=" + theMap + ")", e);
            throw new DBException("Error Select In List Search.", e);
        } finally {
            if (itr != null) // $codepro.audit.disable unnecessaryNullCheck
                itr.close();
        }
    }

    /** Initializes read_opts, write_opts, defaultOptions */
    private void init() {

        defaultOptions = new Options();
        defaultOptions.createIfMissing(true);
        defaultOptions.cacheSize(config().cacheSize());
        if (!config().compressionOn())
            defaultOptions.compressionType(CompressionType.NONE);
        defaultOptions.writeBufferSize(config().writeBufferSize());
        readOpts = new ReadOptions();
        readOpts.fillCache(config().readFillCache());
        readOpts.verifyChecksums(false);
        writeOpts = new WriteOptions();
        writeOpts.sync(config().writeSynchronously());
        comparator = new BytewiseComparator();
    }

    /** Creates an Iterator and sets the position. */
    private DBIterator iteratorAtPosition(final long theKey,
            final long theColumnId) {
        final DBIterator itr = dbInstance.iterator();
        final byte[] combinedKeys = Util.combine(theKey, theColumnId);
        itr.seek(combinedKeys);
        return itr;
    }

    /** Removes the column ids. */
    private void removeColIds(final long theKey, final long theColumnId) {
        final Bytes rowIdBytes = new Bytes(Util.toByta(theKey));
        if (dbInstance.get(toArray(rowIdBytes), readOpts) != null) {
            final byte[] allColIds = dbInstance.get(toArray(rowIdBytes),
                    readOpts);
            final long position = Util.indexOf(allColIds, theColumnId);
            if (position == -1)
                return;
            final byte[] part1Bytes = Arrays.copyOfRange(allColIds, 0,
                    (int) position);
            final byte[] part2Bytes = Arrays.copyOfRange(allColIds,
                    (int) (position + LONG_BYTES), allColIds.length);
            // TODO try to use dbInstance.write() instead of delete()/put().
            // write() uses WriteBatch to 'batch' all the db updates
            // belonging to a single transaction.
            if (part1Bytes.length == 0 && part2Bytes.length == 0) {
                dbInstance.delete(toArray(rowIdBytes), writeOpts);
            } else {
                final byte[] newColIds = ArrayUtils.addAll(part1Bytes,
                        part2Bytes);
                dbInstance.put(toArray(rowIdBytes), newColIds, writeOpts);
            }
        }
    }

    /** Removes entries in range. */
    private void removeInRange(final long theKey, final long theStart,
            final long theEnd) throws Exception {
        DBIterator itr = null;
        try {
            itr = iteratorAtPosition(theKey, theStart);
            Entry<byte[], byte[]> entry = null;
            while (itr.hasNext()
                    && (entry = checkRange(itr.next(), theEnd, theKey, true)) != null) {
                dbInstance.delete(entry.getKey());
                final long colId = Util.splitColumn(entry.getKey());
                removeColIds(theKey, colId);
            }
        } finally {
            if (itr != null)
                itr.close();
        }
    }

    /** Searches entries in range. */
    @CheckForNull
    private void serchInRange(final long theKey, final long theStart,
            final long theEnd, final LongObjectOpenHashMap<Bytes> theMap) {
        DBIterator itr = null;
        try {
            itr = iteratorAtPosition(theKey, theStart);
            java.util.Map.Entry<byte[], byte[]> entry = null;
            while (itr.hasNext()
                    && (entry = checkRange(itr.next(), theEnd, theKey, true)) != null) {
                final long colId = Util.splitColumn(entry.getKey());
                theMap.put(colId, new Bytes(entry.getValue()));
            }
        } catch (final Exception e) {
            LOG.error("Exception Occurred in - serchInRange(long key=" + theKey
                    + ", long start=" + theStart + ", long end=" + theEnd
                    + ", LongObjectOpenHashMap<Bytes> map=" + theMap + ")", e);
            throw new DBException("Error Performaing Select In Range Search.",
                    e);
        } finally {
            if (itr != null) // $codepro.audit.disable unnecessaryNullCheck
                itr.close();
        }
    }

    /** method for update/insert row-column combination */
    private void setOnly(final long theKey, final Columns theInsertUpdateColumns) {
        try {
            for (final LongHolder longCursor : theInsertUpdateColumns) {
                // update blob with the combined key
                final byte[] rowColumnPair = Util.combine(theKey,
                        longCursor.value());
                final Bytes byts = theInsertUpdateColumns.getBytes(longCursor
                        .value());
                assert (byts != null);
                final byte[] valueBytes = byts.toArray(false);
                dbInstance.put(rowColumnPair, valueBytes, writeOpts);
                // update column Id list.
                final Bytes rowIdBytes = new Bytes(Util.toByta(theKey));
                final Bytes colIdBytes = new Bytes(Util.toByta(longCursor
                        .value()));
                // TODO use dbInstance.write() instead of delete()/put().
                // write() uses WriteBatch to 'batch' all the db updates
                // belonging to a single transaction.
                if (dbInstance.get(toArray(rowIdBytes), readOpts) == null) {
                    dbInstance.put(toArray(rowIdBytes), toArray(colIdBytes),
                            writeOpts);
                } else {
                    final ReadOptions readOptsNoCache = new ReadOptions();
                    readOptsNoCache.fillCache(false);

                    final byte[] allColIds = dbInstance.get(
                            toArray(rowIdBytes), readOptsNoCache);
                    if (allColIds == null || allColIds.length == 0) {
                        dbInstance.put(toArray(rowIdBytes),
                                toArray(colIdBytes), writeOpts);
                    } else {
                        final long position = Util.indexOf(allColIds,
                                longCursor.value());
                        if (position < 0) {
                            final byte[] newColIds = ArrayUtils.addAll(
                                    allColIds, toArray(colIdBytes));
                            dbInstance.put(toArray(rowIdBytes), newColIds,
                                    writeOpts);
                        }
                    }
                }
            }
        } catch (final Exception e) {
            LOG.error("Exception Occurred in - setOnly(long key=" + theKey
                    + ", Columns insertOrUpdate=" + theInsertUpdateColumns
                    + ")", e);
            throw new DBException("Error Performaing Insert Update Operation.",
                    e);
        }
    }

    /** Called when table was closed. */
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
        connect();
        final LongArrayList cols = columnsInternal(theKey);
        return (cols != null) ? cols.size() : 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#columnsInternal(long)
     */
    @Override
    @CheckForNull
    protected LongArrayList columnsInternal(final long theKey) {
        connect();
        LongArrayList colIds = null;
        try {
            final byte[] allColumns = dbInstance.get(Util.toByta(theKey),
                    readOpts);
            colIds = Util.splitColumnIds(allColumns);
        } catch (final Exception e) {
            LOG.error("Exception Occurred in - columnsInternal(long key="
                    + theKey + ")", e);
            throw new DBException("Error Performaing All Column Id Search.", e);
        }
        return colIds;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#columnsIteratorInternal(long)
     */
    @Override
    protected Iterator<LongHolder> columnsIteratorInternal(final long theKey) {
        connect();
        final Columns cols = getInternal(theKey, FULL_RANGE);
        if (cols != null)
            return cols.iterator();
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
            final LongArrayList theColumns) {
        connect();
        if (theColumns == null || theColumns.size() == 0) {
            return null;
        }
        final long[] sortedColIds = theColumns.toArray();
        Arrays.sort(sortedColIds);
        final long start = sortedColIds[0];
        final long end = sortedColIds[sortedColIds.length - 1];
        final LongObjectOpenHashMap<Bytes> map = new LongObjectOpenHashMap<Bytes>();
        if (start < 0) {
            getInRangeAndList(theKey, start, -1L, theColumns, map);
            getInRangeAndList(theKey, 0, end, theColumns, map);
        } else {
            getInRangeAndList(theKey, start, end, theColumns, map);
        }
        if (map.size() > 0) {
            return new Columns(map, false);
        }
        return null;
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
        connect();
        if (theRange == null || theRange.empty())
            return null;
        final long start;
        final long end;
        start = (theRange.start() < theRange.end()) ? theRange.start()
                : theRange.end();
        end = (theRange.start() < theRange.end()) ? theRange.end() : theRange
                .start();
        final LongObjectOpenHashMap<Bytes> map = new LongObjectOpenHashMap<Bytes>();
        if (start < 0) {
            serchInRange(theKey, start, -1L, map);
            serchInRange(theKey, 0, end, map);
        } else {
            serchInRange(theKey, start, end, map);
        }
        if (map.size() > 0)
            return new Columns(map, false);
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#keysInternal()
     */
    @Override
    protected Iterator<LongHolder> keysInternal() {
        connect();
        return new LevelDBKeyIterator(this);
    }

    /** Called when table is opened. */
    @Override
    protected void openInternal() {
        connect();
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
        connect();
        try {
            for (final LongCursor longCursor : theRemoveCols) {
                // remove blob entry.
                final byte[] rowColumnPair = Util.combine(theKey,
                        longCursor.value);
                dbInstance.delete(rowColumnPair, writeOpts);
                // update columnIds list.
                removeColIds(theKey, longCursor.value);
            }
        } catch (final Exception e) {
            LOG.error("Exception Occurred in - removeInternal(long key="
                    + theKey + ", LongArrayList remove=" + theRemoveCols + ")",
                    e);
            throw new DBException("Error Performaing remove Operation ", e);
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
        connect();
        if (theRemoveCols.empty())
            return;
        try {
            if (!theRemoveCols.equals(FULL_RANGE)) {
                final long start;
                final long end;
                start = (theRemoveCols.start() < theRemoveCols.end()) ? theRemoveCols
                        .start() : theRemoveCols.end();
                end = (theRemoveCols.start() < theRemoveCols.end()) ? theRemoveCols
                        .end() : theRemoveCols.start();
                if (start < 0) {
                    removeInRange(theKey, start, -1L);
                    removeInRange(theKey, 0, end);
                } else
                    removeInRange(theKey, start, end);
            } else {
                removeInRange(theKey, theRemoveCols.start(),
                        theRemoveCols.end());
            }
        } catch (final Exception e) {
            LOG.error("Exception Occurred in - removeInternal(long key="
                    + theKey + ", Range remove=" + theRemoveCols + ")", e);
            throw new DBException(
                    "Error Performaing Remove In Range Operation.", e);
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
        connect();
        removeInternal(theKey, theRemoveCols);
        setOnly(theKey, theInsertUpdateColumns);
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
        connect();
        removeInternal(theKey, theRemoveCols);
        setOnly(theKey, theInsertUpdateColumns);
    }

    /** Returns number rows in this table.
     * 
     * @return the long */
    @Override
    protected long sizeInternal() {
        connect();
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
            throw new DBException("Error obtaining row count", e);
        }
    }

    /** Db instance. */
    public DB dbInstance() {
        return dbInstance;
    }

    /** Called when the table was dropped. */
    public void dropped() {
        connect();
        closeInternal();
        try {
            FCTRY.destroy(dbFolder, new Options());
        } catch (final IOException e) {
            LOG.error("Exception Occurred in - dropped()", e);

            e.printStackTrace();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#getLimited(long, int)
     */
    @Override
    public Columns getLimited(final long theKey, final int theCount) {
        connect();
        if (theCount == 0) {
            return null;
        }
        DBIterator itr = null;
        final LongObjectOpenHashMap<Bytes> map = new LongObjectOpenHashMap<Bytes>();
        try {
            itr = iteratorAtPosition(theKey, MIN_COLUMN);
            java.util.Map.Entry<byte[], byte[]> entry = null;
            int i = 0;
            while (itr.hasNext()
                    && (entry = checkRange(itr.next(), MAX_COLUMN, theKey, true)) != null
                    && i++ < theCount) {
                final long colId = Util.splitColumn(entry.getKey());
                map.put(colId, new Bytes(entry.getValue()));
            }
        } catch (final Exception e) {
            LOG.error("Exception Occurred in - getLimited(long key=" + theKey
                    + ", int count=" + theCount + ")", e);
            throw new DBException("Error Performaing Get limited Operation.", e);
        } finally {
            if (itr != null) // $codepro.audit.disable unnecessaryNullCheck
                itr.close();
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
