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
package com.blockwithme.longdb.cassandra;

import static com.blockwithme.longdb.cassandra.CassandraConstants.ROW_SLICE_COUNT;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.annotation.ParametersAreNonnullByDefault;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

import com.blockwithme.longdb.base.AbstractKeyIterator;

/** The Class CassandraKeyIterator. */
@ParametersAreNonnullByDefault
public class CassandraKeyIterator extends AbstractKeyIterator<CassandraTable> {

    /** Empty iterator. */
    @SuppressWarnings("unchecked")
    private static final Iterator<Row<Long, Long, byte[]>> EMPTY = Collections.EMPTY_LIST
            .iterator();

    /** The key at buffer last position. */
    private Long bufferEndkey;

    /** The internal iterator buffer. */
    private Iterator<Row<Long, Long, byte[]>> itr;

    /** Instantiates a new cassandra key iterator.
     * 
     * @param theTable
     *        the table instance */
    protected CassandraKeyIterator(final CassandraTable theTable) {
        super(theTable);
        itr = EMPTY;
        getMore();
    }

    /** Internal method: populates the buffer when empty. */
    private boolean getMore() {

        final Keyspace keyspace = HFactory.createKeyspace(table.database()
                .database(), ((CassandraBackend) table.database().backend())
                .cluster());

        final RangeSlicesQuery<Long, Long, byte[]> query = HFactory
                .createRangeSlicesQuery(keyspace, LongSerializer.get(),
                        LongSerializer.get(), BytesArraySerializer.get());

        query.setColumnFamily(table.table().toFixedString());
        query.setKeys(bufferEndkey, null);
        query.setReturnKeysOnly();
        query.setRowCount(ROW_SLICE_COUNT);
        final OrderedRows<Long, Long, byte[]> rows = query.execute().get();

        if (rows.getCount() > 0) {
            final Row<Long, Long, byte[]> row = rows.peekLast();
            if (row != null) {
                final long lastValue = row.getKey();
                if (bufferEndkey != null && lastValue == bufferEndkey) {
                    return false;
                }
                itr = rows.iterator();
                assert (itr != null);
                if (bufferEndkey != null)
                    itr.next(); // this is intentional as ranges are
                // inclusive.
                bufferEndkey = lastValue;
                return true;
            }
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
        if (!hasNext())
            throw new NoSuchElementException("No more values");
        return itr.next().getKey();
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
