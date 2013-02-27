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
package com.blockwithme.longdb.voltdb.server.t2;

import org.voltdb.ProcInfo;
import org.voltdb.VoltTable;

/** The 'SelectColumnInRange' stored procedure for table 'T2'. */
@ProcInfo(partitionInfo = "T2.ROW_KEY: 0", singlePartition = true)
public class SelectColumnInRangeT2 extends
        com.blockwithme.longdb.voltdb.server.base.SelectColumnInRange {

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.voltdb.server.base.SelectColumnInRange#run(long,
     * long, long) */
    @Override
    public VoltTable[] run(final long theRow, final long theStart,
            final long theEnd) throws VoltAbortException {
        return super.run(theRow, theStart, theEnd);
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.voltdb.server.base.IBaseStoredProc#tableName() */
    @Override
    public String tableName() {
        return "T2";
    }
}
