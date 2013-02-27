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
package com.blockwithme.longdb.voltdb.server.t2;

import org.voltdb.ProcInfo;
import org.voltdb.VoltTable;

/** The 'InsertUpdateRemoveRange' stored procedure for table 'T2'. */
@ProcInfo(partitionInfo = "T2.ROW_KEY: 0", singlePartition = true)
public class InsertUpdateRemoveRangeT2 extends
        com.blockwithme.longdb.voltdb.server.base.InsertUpdateRemoveRange {

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.paintedboxes.voltdb.server.base.InsertUpdateRemoveRange#run(long,
     * long[], byte[], int[], long, long)
     */
    @Override
    public VoltTable[] run(final long theRowKey, final long[] theUpdateColIDs,
            final byte[] theUpdateBolbs, final int[] theIndexArray,
            final long theRemoveStartRange, final long theRemoveEndRange) {
        return super.run(theRowKey, theUpdateColIDs, theUpdateBolbs,
                theIndexArray, theRemoveStartRange, theRemoveEndRange);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.paintedboxes.voltdb.server.base.IBaseStoredProc#tableName()
     */
    @Override
    public String tableName() {
        return "T2";
    }

}
