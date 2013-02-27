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
package com.blockwithme.longdb.voltdb.server.defaulttable;

import org.voltdb.ProcInfo;
import org.voltdb.VoltTable;

/** The 'SelectColumnLimited' stored procedure for 'DefaultTable'. */
@ProcInfo(partitionInfo = "defaulttable.ROW_KEY: 0", singlePartition = true)
public class SelectColumnLimitedDefaulttable extends
        com.blockwithme.longdb.voltdb.server.base.SelectColumnLimited {

    /*
     * (non-Javadoc)
     * 
     * @see com.paintedboxes.voltdb.server.base.SelectColumnLimited#run(long,
     * long)
     */
    @Override
    public VoltTable[] run(final long theRow, final long theLimit)
            throws VoltAbortException {
        return super.run(theRow, theLimit);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.paintedboxes.voltdb.server.base.IBaseStoredProc#tableName()
     */
    @Override
    public String tableName() {
        return "defaulttable";
    }

}