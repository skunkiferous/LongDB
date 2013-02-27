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
package com.blockwithme.longdb.leveldb;

import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.annotation.ParametersAreNonnullByDefault;

import org.iq80.leveldb.DBIterator;

import com.blockwithme.longdb.base.AbstractKeyIterator;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.LongCursor;

/** Iterates rowID for a particular table. */
@ParametersAreNonnullByDefault
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "CD_CIRCULAR_DEPENDENCY", justification = "This is our base class design")
public class LevelDBKeyIterator extends AbstractKeyIterator<LevelDBTable> {

    /** The current key. */
    private Long currentKey;

    /** Keeps a buffer */
    private Iterator<LongCursor> itr;

    /** indicates that last row is reached. */
    boolean findMore = true;

    /** Instantiates a new level db key iterator. */
    protected LevelDBKeyIterator(final LevelDBTable theTable) {
        super(theTable);
        getMore();
    }

    /** Gets the more.
     * 
     * @return the more */
    private boolean getMore() {
        if (!findMore)
            return false;
        DBIterator cur = null;
        try {
            final LongArrayList list = new LongArrayList();
            cur = table.dbInstance().iterator();
            int i = 0;
            while (i++ < LevelDBConstants.ITERATOR_LIMIT && newKey(cur)) {
                list.add(currentKey);
            }
            if (i < LevelDBConstants.ITERATOR_LIMIT)
                findMore = false;
            if (list.size() > 0) {
                itr = list.iterator();
                return true;
            } else
                return false;
        } finally {
            if (cur != null)
                cur.close();
        }
    }

    /** New key. */
    private boolean newKey(final DBIterator theCursor) {

        if (currentKey == null) {
            theCursor.seekToFirst();
        } else {
            final byte[] keys = Util.toByta(++currentKey);
            theCursor.seek(keys);
        }
        if (theCursor.hasNext()) {
            final java.util.Map.Entry<byte[], byte[]> entry = theCursor.next();
            currentKey = Util.splitRow(entry.getKey());
            return true;
        }
        return false;
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
        return itr.hasNext() || getMore();
    }
}
