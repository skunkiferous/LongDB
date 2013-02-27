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
package com.blockwithme.longdb.voltdb;

import static com.blockwithme.longdb.voltdb.VoltDBConstants.ITERATOR_LIMIT;

import java.io.Closeable;
import java.util.NoSuchElementException;

import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;

import com.blockwithme.longdb.base.AbstractKeyIterator;
import com.blockwithme.longdb.exception.DBException;

/** Row id Iterator for a particular Table. Keeps the DB connection alive while
 * iterator is being used, so its important to call close() method, once
 * iteration is done */
@ParametersAreNonnullByDefault
public class VoltDBKeyIterator extends AbstractKeyIterator<VoltDBTable>
        implements Closeable {
    /** Logger for this class */
    private static final Logger LOG = LoggerFactory
            .getLogger(VoltDBKeyIterator.class);

    /** VoltDb client handle */
    private Client client; // NOPMD

    /** Offset to be passed to Database query. */
    private int currentOffset;

    /** Current cursor position. */
    private int currentPos;

    /** Indicates if the number of rows currently filled in tableData is more
     * less than ITERATOR_LIMIT */
    private boolean lookForMoreRows = true;

    /** Table on which the row Iterator is created. */
    private VoltTable tableData;

    /** Instantiates a new volt db key iterator.
     * 
     * @param theTable
     *        the table */
    protected VoltDBKeyIterator(final VoltDBTable theTable) {
        super(theTable);
        getMore();
    }

    /** Method is called to populate tableData, making a call to database. */
    private boolean getMore() {
        if (!lookForMoreRows)
            return false;
        try {
            client = ((VoltDBBackend) table.database().backend()).client();
            final String tablename = table.table().toString();
            final ClientResponse res = client.callProcedure("SelectColItertor"
                    + VoltDBTable.capitalize(tablename), ITERATOR_LIMIT,
                    currentOffset);
            tableData = res.getResults()[0];
            if (tableData.getRowCount() < ITERATOR_LIMIT) {
                lookForMoreRows = false;
            }
            currentOffset = tableData.getRowCount();
            currentPos = 0;
            return true;

        } catch (final Exception e) {
            LOG.error("Exception Occurred in - getMore()", e);
            throw new DBException("Error performing Iterator Query.", e);
        }
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
        tableData.advanceRow();
        return tableData.fetchRow(currentPos++).getLong(0);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.Closeable#close()
     */
    @Override
    public void close() {
        // NOP
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractKeyIterator#hasNext()
     */
    @Override
    public boolean hasNext() {
        return (tableData.getRowCount()) > currentPos || getMore();
    }
}
