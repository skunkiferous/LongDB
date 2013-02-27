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
package com.blockwithme.longdb.voltdb.server.t1;

import org.voltdb.ProcInfo;
import org.voltdb.VoltTable;

/** The 'SelectRowCount' stored procedure for 'T1'. */
@ProcInfo(partitionInfo = "T1.ROW_KEY: 0", singlePartition = true)
public class SelectRowT1 extends
        com.blockwithme.longdb.voltdb.server.base.SelectRow {

    /* (non-Javadoc)
     * @see com.blockwithme.longdb.voltdb.server.base.SelectRow#run(long) */
    @Override
    public VoltTable[] run(final long theRow) throws VoltAbortException { // NOPMD
        return super.run(theRow);
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.voltdb.server.base.IBaseStoredProc#tableName() */
    @Override
    public String tableName() {
        return "T1";
    }

}
