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

import org.voltdb.VoltTable;

/** The 'SelectColItertor' stored procedure for 'DefaultTable'. */
public class SelectColItertorDefaulttable extends
        com.blockwithme.longdb.voltdb.server.base.SelectColItertor {

    /* (non-Javadoc)
     * @see com.blockwithme.longdb.voltdb.server.base.SelectColItertor#run(int,
     * int) */
    @Override
    public VoltTable[] run(final int theLimit, final int theOffset)
            throws VoltAbortException {
        return super.run(theLimit, theOffset);
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.voltdb.server.base.IBaseStoredProc#tableName() */
    @Override
    public String tableName() {
        return "defaulttable";
    }
}
