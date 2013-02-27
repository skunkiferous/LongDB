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
package com.blockwithme.longdb.cassandra.embedded;

import static com.blockwithme.longdb.cassandra.embedded.CassandraEmbConstants.DEFAULT_CONSISTENCY_LEVEL;
import static com.blockwithme.longdb.common.constants.ByteConstants.BLANK_STR_BYTES;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.cassandra.thrift.CassandraServer;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;

import com.blockwithme.longdb.base.AbstractKeyIterator;
import com.blockwithme.longdb.exception.DBException;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.LongCursor;

/** The Class Cassandra (Embedded) table row id Iterator. */
@ParametersAreNonnullByDefault
public class CassandraEmbKeyIterator extends
        AbstractKeyIterator<CassandraEmbTable> {

    /**  */
    private static final byte[] ZERO_BYTES = new byte[0];

    /** Empty iterator. */
    @SuppressWarnings("unchecked")
    private static final Iterator<LongCursor> EMPTY = Collections.EMPTY_LIST
            .iterator();

    /** The key at buffer last position. */
    private Long bufferEndkey;

    /** The iterator buffer. */
    private Iterator<LongCursor> itr;

    /** Instantiates a new embedded cassandra key iterator.
     * 
     * @param theTable
     *        the table object */
    protected CassandraEmbKeyIterator(final CassandraEmbTable theTable) {
        super(theTable);
        itr = EMPTY;
        getMore();
    }

    /** Internal method: Constructs column range (empty range since we don't need
     * column values) */
    private SliceRange constructRange() {
        // TODO: If the range is always the same, should it not be a static
        // constant instead?
        final SliceRange range = new SliceRange();
        // to return only keys.
        range.setCount(0);
        range.setStart(ByteBuffer.wrap(ZERO_BYTES));
        range.setFinish(ByteBuffer.wrap(ZERO_BYTES));
        return range;
    }

    /** Internal method: Gets the column parent. */
    private ColumnParent getColumnParent() {
        final ColumnParent columnParent = new ColumnParent();
        columnParent.setColumn_family(table.table().toFixedString());
        return columnParent;
    }

    /** Internal method: Gets the key range. */
    private KeyRange getKeyRange(final Long theStartKey) {
        final KeyRange kRange = new KeyRange();
        if (theStartKey == null)
            kRange.setStart_key(BLANK_STR_BYTES);
        else
            kRange.setStart_key(ByteBufferUtil.bytes(theStartKey));
        kRange.setEnd_key(BLANK_STR_BYTES);
        kRange.setCount(CassandraEmbConstants.ROW_SLICE_COUNT);
        return kRange;
    }

    /** Internal method: populates the iterator buffer. */
    private boolean getMore() {

        try {
            // create cassandra range slice query using:
            // row key range from 'startKey' and return
            // count='ITERATOR_BUFFER_SIZE'
            // empty column slice range, since we don't need column values.

            // TODO: This looks VERY suspicious! *Our* databases have multiple
            // tables.
            // a Cassandra keyspace is the equivalent of our tables. So why do
            // we use
            // here the name of the *database* instead of the name of the
            // *table* to
            // define the keyspace?
            server().set_keyspace(table.database().database());
            final SliceRange range = constructRange();
            final KeyRange kRange = getKeyRange(bufferEndkey);
            final SlicePredicate predicate = new SlicePredicate();
            predicate.setSlice_range(range);
            final List<KeySlice> res;
            // run the query.
            res = server().get_range_slices(getColumnParent(), predicate,
                    kRange, DEFAULT_CONSISTENCY_LEVEL);
            // iterate the results.
            if (!res.isEmpty()) {
                final LongArrayList ll = new LongArrayList();
                final Iterator<KeySlice> kItr = res.iterator();
                while (kItr.hasNext()) {
                    final KeySlice ks = kItr.next();
                    if (ks.getKey().length != 0)
                        // add key to buffer
                        ll.add(ks.key.getLong());
                }
                itr = ll.iterator();
                final long last = ll.get(ll.size() - 1);
                if (bufferEndkey != null && bufferEndkey == last)
                    return false;
                if (bufferEndkey != null)
                    itr.next(); // this is intentional as ranges are
                // inclusive.
                bufferEndkey = last;
                return true;
            }
            return false;
        } catch (final Exception e) {
            throw new DBException("Error while iterating Columns", e);
        }
    }

    /** Internal method: resolves the CassandraServer object */
    private CassandraServer server() {
        return ((CassandraEmbBackend) table.database().backend()).server();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractKeyIterator#nextKey()
     */
    @Override
    protected long nextKey() {
        if (!hasNext())
            throw new NoSuchElementException("No more values");
        return itr.next().value;
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
