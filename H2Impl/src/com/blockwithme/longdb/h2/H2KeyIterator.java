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
package com.blockwithme.longdb.h2;

import static com.blockwithme.longdb.h2.H2Constants.ITERATOR_LIMIT;

import java.sql.Connection;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.annotation.ParametersAreNonnullByDefault;

import com.blockwithme.longdb.base.AbstractKeyIterator;
import com.blockwithme.longdb.exception.DBException;
import com.blockwithme.longdb.h2.config.SQLUtil;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.LongCursor;

/** Iterator for all the keys in a table. */
@ParametersAreNonnullByDefault
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "CD_CIRCULAR_DEPENDENCY", justification = "This is our base class design")
public class H2KeyIterator extends AbstractKeyIterator<H2Table> {
    /* The iterator gets populated chunk by chunk */
    /** The iterator. */
    @SuppressWarnings("unchecked")
    private Iterator<LongCursor> iter = Collections.EMPTY_LIST.iterator();

    /** The offset. */
    private long offset;

    // stores the query string.
    /** The query buff. */
    private final StringBuilder queryBuff;

    /** Instantiates a new h2 key iterator.
     * 
     * @param theTable
     *        the table object */
    protected H2KeyIterator(final H2Table theTable) {
        super(theTable);
        queryBuff = new StringBuilder();
        queryBuff.append("SELECT DISTINCT ROW_KEY FROM ")
                .append(this.table.database().database()).append('.')
                .append(this.table.table().toString())
                .append(" ORDER BY ROW_KEY LIMIT ").append(ITERATOR_LIMIT)
                .append(" OFFSET ");
        getMore();
    }

    /** Gets the more.
     * 
     * @return the more */
    private boolean getMore() {
        try {
            final LongArrayList res = SQLUtil.getResultsLongList(
                    queryBuff.toString() + offset, connection(), "ROW_KEY");
            if (res != null) {
                iter = res.iterator();
                return iter.hasNext();
            }
        } catch (final Exception e) {
            throw new DBException("Error performing Iterator Query.", e);
        }
        return false;
    }

    /** Connection.
     * 
     * @return the connection */
    protected Connection connection() {
        return ((H2Backend) table.database().backend()).connection();
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
        offset++;
        return iter.next().value;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractKeyIterator#hasNext()
     */
    @Override
    public boolean hasNext() {
        return iter.hasNext() || getMore();
    }

}
