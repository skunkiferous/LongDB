/*******************************************************************************
 * Copyright (c) 2013 Sebastien Diot..
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0
 * which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/gpl.html
 *
 * Contributors:
 *     Sebastien Diot. - initial API and implementation
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
     * @param theBackend the backend */
    protected VoltDatabase(final VoltDBBackend theBackend) {
        super(theBackend, "default");
    }

    /* (non-Javadoc)
     * @see com.blockwithme.longdb.base.AbstractDatabase#closeInternal() */
    @Override
    protected void closeInternal() {
        // NOP
    }

    /** Throws UnsupportedOperationException as dynamic table creation is not
     * supported currently by voltdb implementation.
     * 
     * @param theTable the table
     * @param theProfile the profile
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
     * @param theTable the table */
    @Override
    protected void dropInternal(final VoltDBTable theTable) {
        throw new UnsupportedOperationException(
                "This operation is not supported for VoltDB");
    }

    /** Queries all the tables present in default database and load them into
     * 'tables' map.
     * 
     * @param theTables the tables */
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
