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
package com.blockwithme.longdb.voltdb.server.t1;

import org.voltdb.ProcInfo;
import org.voltdb.VoltTable;

/** The 'SelectColumnInRange' stored procedure for table 'T1'. */
@ProcInfo(partitionInfo = "T1.ROW_KEY: 0", singlePartition = true)
public class SelectColumnInRangeT1 extends
        com.blockwithme.longdb.voltdb.server.base.SelectColumnInRange {

    /*
     * (non-Javadoc)
     * 
     * @see com.paintedboxes.voltdb.server.base.SelectColumnInRange#run(long,
     * long, long)
     */
    @Override
    public VoltTable[] run(final long theRow, final long theStart,
            final long theEnd) throws VoltAbortException {
        return super.run(theRow, theStart, theEnd);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.paintedboxes.voltdb.server.base.IBaseStoredProc#tableName()
     */
    @Override
    public String tableName() {
        return "T1";
    }
}
