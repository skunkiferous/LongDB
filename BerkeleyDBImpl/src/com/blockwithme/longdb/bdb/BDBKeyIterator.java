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
import java.util.NoSuchElementException;

import javax.annotation.ParametersAreNonnullByDefault;

import com.blockwithme.longdb.base.AbstractKeyIterator;
import com.blockwithme.longdb.common.constants.ByteConstants;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.OperationStatus;

/** Iterator of ALL the keys in a particular table. Uses BDB database
 * cursor.getSearchKeyRange() to fetch row keys. Doesn't keep an open DB cursor
 * in memory While iterating on rows, but keeps a cache of keys */
@ParametersAreNonnullByDefault
public class BDBKeyIterator extends AbstractKeyIterator<BDBTable> {

    /** Long.MIN_VALUE bytes */
    private static final byte[] MIN_VALUE_BYTES = DataConversionUtil
            .toByta(Long.MIN_VALUE);

    /** The table on which the keys are iterated. */
    private final BDBTable bdtab;

    /** Last row-key until where the row ids are present in 'itr'. */
    private Long currentKey;

    /** Last row-key bytes until where the row ids are present in 'itr'. */
    private byte[] currentKeyBytes;

    /** Iterator of cached row keys */
    private Iterator<LongCursor> itr;

    /** The last key. */
    private DatabaseEntry lastKey;

    /** Instantiates a new key iterator.
     * 
     * @param theTable
     *        the table on which key iterator is created */
    protected BDBKeyIterator(final BDBTable theTable) {
        super(theTable);
        bdtab = theTable;
        getNext();
    }

    /** resolve backend object. */
    private BDBBackend backend() {
        return (BDBBackend) bdtab.database().backend();
    }

    /** Populates the row iterator with a certain number of row keys */
    private boolean getNext() {
        Cursor cur = null;
        try {
            // TODO If we are *iterating* over the DB, can we not *reuse* the
            // same cursor for each getNext() call? In most normal DB, the point
            // of cursor is so that you can iterate over the data. Why do we
            // have
            // to iterate once per key, instead of once for all keys?
            // Isn't that inefficient?
            cur = bdtab.dbInstance().openCursor(null, null);
            final DatabaseEntry ve = new DatabaseEntry();
            OperationStatus s;
            if (currentKey == null) {
                currentKey = Long.MIN_VALUE;
                currentKeyBytes = MIN_VALUE_BYTES;
            }

            lastKey = new DatabaseEntry(currentKeyBytes);
            s = cur.getSearchKeyRange(lastKey, ve, backend().config()
                    .lockMode());

            final LongArrayList buffer = new LongArrayList();
            int count = 0;
            while (s == OperationStatus.SUCCESS
                    && count < BDBConstants.ITERATOR_LIMIT) {
                s = processNext(cur, ve, buffer);
                count++;
            }

            itr = buffer.iterator();
            return (buffer.size() > 0) ? true : false;

        } finally {
            if (cur != null)
                cur.close();
        }
    }

    /** Process next. */
    private OperationStatus processNext(final Cursor theCursor,
            final DatabaseEntry theValueEntry, final LongArrayList theBuffer) {

        final byte[] bytes = lastKey.getData();
        if (bytes.length == ByteConstants.LONG_BYTES) {
            currentKeyBytes = bytes;
            currentKey = DataConversionUtil.toLong(bytes);
            lastKey = new DatabaseEntry(bytes);
        } else {
            currentKeyBytes = Arrays.copyOfRange(bytes, 0,
                    ByteConstants.LONG_BYTES);
            currentKey = DataConversionUtil.toLong(currentKeyBytes);
            lastKey = new DatabaseEntry(currentKeyBytes);
        }
        theBuffer.add(currentKey);
        currentKey++;
        return theCursor.getSearchKeyRange(lastKey, theValueEntry, backend()
                .config().lockMode());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractKeyIterator#nextKey()
     */
    @Override
    protected long nextKey() {
        if (hasNext()) {
            return itr.next().value;
        } else {
            throw new NoSuchElementException("No more values");
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractKeyIterator#hasNext()
     */
    @Override
    public boolean hasNext() {
        return itr.hasNext() || getNext();
    }
}
