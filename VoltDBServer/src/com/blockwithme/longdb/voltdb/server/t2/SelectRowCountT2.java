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

import org.voltdb.VoltTable;

/** The 'SelectRowCount' stored procedure for 'T2'. */
public class SelectRowCountT2 extends
        com.blockwithme.longdb.voltdb.server.base.SelectRowCount {

    /* (non-Javadoc)
     * @see com.blockwithme.longdb.voltdb.server.base.SelectRowCount#run() */
    @Override
    public VoltTable[] run() throws VoltAbortException { // NOPMD
        return super.run();
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.voltdb.server.base.IBaseStoredProc#tableName() */
    @Override
    public String tableName() {
        return "T2";
    }
}
