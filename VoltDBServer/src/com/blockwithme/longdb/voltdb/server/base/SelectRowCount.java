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
package com.blockwithme.longdb.voltdb.server.base;

import javax.annotation.ParametersAreNonnullByDefault;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/** The base class for 'SelectRowCount' stored procedure. */
@ParametersAreNonnullByDefault
public abstract class SelectRowCount extends VoltProcedure implements
        IBaseStoredProc {

    /** The sql. */
    protected final SQLStmt sql = new SQLStmt(
            "SELECT COUNT(DISTINCT ROW_KEY) FROM " + tableName() + ";");

    /** The 'run' method for this store procedure.
     * 
     * @return the resultant VoltTable[]
     * @throws VoltAbortException
     *         the volt abort exception */
    public VoltTable[] run() throws VoltAbortException {
        voltQueueSQL(sql);
        return voltExecuteSQL();
    }
}
