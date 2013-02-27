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

/** The base class for 'SelectColItertor' store procedure. */
@ParametersAreNonnullByDefault
public abstract class SelectColItertor extends VoltProcedure implements
        IBaseStoredProc {

    /** The sql. */
    protected final SQLStmt sql = new SQLStmt("SELECT DISTINCT ROW_KEY FROM "
            + tableName() + " ORDER BY ROW_KEY  " + "LIMIT ? OFFSET ? ;");

    /** Run.
     * 
     * @param theLimit
     *        the limit
     * @param theOffset
     *        the offset
     * @return the volt table[]
     * @throws VoltAbortException
     *         the volt abort exception */
    public VoltTable[] run(final int theLimit, final int theOffset)
            throws VoltAbortException {
        voltQueueSQL(sql, theLimit, theOffset);
        return voltExecuteSQL();
    }

}
