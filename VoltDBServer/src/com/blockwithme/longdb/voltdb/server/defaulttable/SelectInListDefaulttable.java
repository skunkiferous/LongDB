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
package com.blockwithme.longdb.voltdb.server.defaulttable;

import org.voltdb.ProcInfo;
import org.voltdb.VoltTable;

/** The 'SelectInList' store procedure for 'DefaultTable'. */
@ProcInfo(partitionInfo = "defaulttable.ROW_KEY: 0", singlePartition = true)
public class SelectInListDefaulttable extends
        com.blockwithme.longdb.voltdb.server.base.SelectInList {

    /* (non-Javadoc)
     * @see com.blockwithme.longdb.voltdb.server.base.SelectInList#run(long,
     * long[]) */
    @Override
    public VoltTable[] run(final long theRow, final long[] theColumnIds)
            throws VoltAbortException {
        return super.run(theRow, theColumnIds);
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.voltdb.server.base.IBaseStoredProc#tableName() */
    @Override
    public String tableName() {
        return "defaulttable";
    }
}
