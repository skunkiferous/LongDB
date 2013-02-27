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

import java.util.Map;

import javax.annotation.ParametersAreNonnullByDefault;

import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;

import com.blockwithme.longdb.BETableProfile;
import com.blockwithme.longdb.base.AbstractDatabase;
import com.blockwithme.longdb.entities.Base36;
import com.blockwithme.longdb.exception.DBException;

/** Implementation of a VoltDB BEDatabase. */
@ParametersAreNonnullByDefault
public class VoltDatabase extends
        AbstractDatabase<VoltDBBackend, VoltDatabase, VoltDBTable> {

    /* This constructor is called for initializing default database */
    /** Instantiates a new volt database.
     * 
     * @param theBackend
     *        the backend */
    protected VoltDatabase(final VoltDBBackend theBackend) {
        super(theBackend, "default");
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractDatabase#closeInternal()
     */
    @Override
    protected void closeInternal() {
        // NOP
    }

    /** Throws UnsupportedOperationException as dynamic table creation is not
     * supported currently by voltdb implementation.
     * 
     * @param theTable
     *        the table
     * @param theProfile
     *        the profile
     * @return the volt db table */
    @Override
    protected VoltDBTable createInternal(final Base36 theTable,
            final BETableProfile theProfile) {
        throw new UnsupportedOperationException(
                "This operation is not supported for VoltDB");
    }

    /** Throws UnsupportedOperationException as dyanamic table deletion is not
     * supported currently by voltdb implementation.
     * 
     * @param theTable
     *        the table */
    @Override
    protected void dropInternal(final VoltDBTable theTable) {
        throw new UnsupportedOperationException(
                "This operation is not supported for VoltDB");
    }

    /** Queries all the tables present in default database and load them into
     * 'tables' map.
     * 
     * @param theTables
     *        the tables */
    @Override
    protected void openInternal(final Map<Base36, VoltDBTable> theTables) {

        theTables.clear();
        final VoltTable[] results;
        try {
            results = backend.client()
                    .callProcedure("@SystemCatalog", "TABLES").getResults();
            for (final VoltTable node : results) {
                int count = 0;
                while (node.advanceRow()) {
                    final VoltTableRow row = node.fetchRow(count++);
                    final String tName = row.getString("TABLE_NAME");
                    theTables.put(Base36.get(tName.toLowerCase()),
                            new VoltDBTable(this, tName));
                }
            }
        } catch (final Exception e) {
            throw new DBException("Error Opening Database: " + theTables, e);
        }
    }
}
