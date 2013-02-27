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
package com.blockwithme.longdb.voltdb.server.base;

import javax.annotation.ParametersAreNonnullByDefault;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/** The base class for 'SelectColumnLimited' store procedure. */
@ParametersAreNonnullByDefault
public abstract class SelectColumnLimited extends VoltProcedure implements
        IBaseStoredProc {

    /** The sql. */
    protected final SQLStmt sql = new SQLStmt(
            "SELECT ROW_KEY, COLUMN_KEY, DATA_BLOB, LAST_MODIFIED FROM "
                    + tableName() + " WHERE ROW_KEY = ? ORDER BY ROW_KEY, "
                    + "COLUMN_KEY LIMIT ?  ;");

    /** The run method for this stored procedure.
     * 
     * @param theRow
     *        the row key
     * @param theLimit
     *        the number of columns to be returned.
     * @return the resultant VoltTable[]
     * @throws VoltAbortException
     *         the volt abort exception */
    public VoltTable[] run(final long theRow, final long theLimit)
            throws VoltAbortException {

        voltQueueSQL(sql, theRow, theLimit);
        return voltExecuteSQL();
    }
}
