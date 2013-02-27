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
// $codepro.audit.disable com.instantiations.assist.eclipse.subClassShouldOverrideMethod, hidingInheritedFields
package com.blockwithme.longdb.voltdb.server.base;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.ParametersAreNonnullByDefault;

import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

/** The stored procedure base class for 'InsertUpdateRemoveRange' */
@ParametersAreNonnullByDefault
public abstract class InsertUpdateRemoveRange extends InsertUpdateRemove {

    /** The delete sql. */
    protected final SQLStmt deleteSql = new SQLStmt("DELETE FROM "
            + tableName() + " WHERE ROW_KEY=? AND COLUMN_KEY >= ?"
            + " AND COLUMN_KEY <= ?;");

    /** The delete sql all. */
    protected final SQLStmt deleteSqlAll = new SQLStmt("DELETE FROM "
            + tableName() + " WHERE ROW_KEY=? ;");

    /** The 'run' method of this stored procedure.
     * 
     * @param theRowKey
     *        the row key
     * @param theUpdateColIDs
     *        the update column ids
     * @param theUpdateBolbs
     *        the combined blobs for update operation
     * @param theIndexArray
     *        the indexes to be used to break 'updateBolbs'
     * @param theRemoveStartRange
     *        the start range for columns to be removed.
     * @param theRemoveEndRange
     *        the end range for columns to be removed.
     * @return the resultant VoltTable[]
     * @throws VoltAbortException
     *         the volt abort exception */
    public VoltTable[] run(final long theRowKey, final long[] theUpdateColIDs,
            final byte[] theUpdateBolbs, final int[] theIndexArray,
            final long theRemoveStartRange, final long theRemoveEndRange)
            throws VoltAbortException {
        try {
            // final long lastModified = this.getTransactionTime().getTime();
            final long lastModified = System.currentTimeMillis();
            final List<VoltTable> results = new ArrayList<VoltTable>();
            if (theRemoveStartRange == Long.MIN_VALUE
                    && theRemoveEndRange == Long.MAX_VALUE)
                voltQueueSQL(deleteSqlAll, theRowKey);
            else
                voltQueueSQL(deleteSql, theRowKey, theRemoveStartRange,
                        theRemoveEndRange);

            results.addAll(Arrays.asList(voltExecuteSQL()));

            return updateData(theRowKey, theUpdateColIDs, theUpdateBolbs,
                    theIndexArray, lastModified, results);

        } catch (final VoltAbortException e) {
            e.printStackTrace();
            throw e;
        } catch (final Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
