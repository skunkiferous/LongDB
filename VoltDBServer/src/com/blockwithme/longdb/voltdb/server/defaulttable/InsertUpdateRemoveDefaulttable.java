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

/** The stored procedure 'InsertUpdateRemoveDefaultTable' */
@ProcInfo(partitionInfo = "defaulttable.ROW_KEY: 0", singlePartition = true)
public class InsertUpdateRemoveDefaulttable extends
        com.blockwithme.longdb.voltdb.server.base.InsertUpdateRemove {

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.voltdb.server.base.InsertUpdateRemove#run(long,
     * long[], byte[], int[], long[]) */
    @Override
    public VoltTable[] run(final long theRowKey, final long[] theUpdateColIDs,
            final byte[] theCombinedBlobs, final int[] theIndexArray,
            final long[] theRemoveIds) {
        return super.run(theRowKey, theUpdateColIDs, theCombinedBlobs,
                theIndexArray, theRemoveIds);
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.voltdb.server.base.IBaseStoredProc#tableName() */
    @Override
    public String tableName() {
        return "defaulttable";
    }
}
