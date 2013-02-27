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

import org.voltdb.VoltTable;

/** The 'SelectColItertor' stored procedure for 'DefaultTable'. */
public class SelectColItertorDefaulttable extends
        com.blockwithme.longdb.voltdb.server.base.SelectColItertor {

    /*
     * (non-Javadoc)
     * 
     * @see com.paintedboxes.voltdb.server.base.SelectColItertor#run(int, int)
     */
    @Override
    public VoltTable[] run(final int theLimit, final int theOffset)
            throws VoltAbortException {
        return super.run(theLimit, theOffset);
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
