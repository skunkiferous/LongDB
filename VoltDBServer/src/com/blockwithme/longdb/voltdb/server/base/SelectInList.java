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

import java.util.Arrays;

import javax.annotation.ParametersAreNonnullByDefault;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;

/** The base class for 'SelectInList' store procedure. */
@ParametersAreNonnullByDefault
public abstract class SelectInList extends VoltProcedure implements
        IBaseStoredProc {

    /** The sql. */
    protected final SQLStmt sql = new SQLStmt(
            "SELECT ROW_KEY, COLUMN_KEY, DATA_BLOB, " + "LAST_MODIFIED FROM "
                    + tableName() + " WHERE ROW_KEY = ? "
                    + "AND COLUMN_KEY >= ? AND COLUMN_KEY <= "
                    + "? ORDER BY ROW_KEY, COLUMN_KEY ;");

    /** Contains number in an array.
     * 
     * @param theArray
     *        the array of long numbers
     * @param theNumber
     *        the number to look for
     * @return true, if successful */
    private static boolean contains(final long[] theArray, final long theNumber) {
        for (final long l : theArray) {
            if (l == theNumber)
                return true;
        }
        return false;
    }

    /** The run method for this stored procedure.
     * 
     * @param theRow
     *        the row key
     * @param theColumnIds
     *        the column ids
     * @return the resultant VoltTable[]
     * @throws VoltAbortException
     *         the volt abort exception */
    public VoltTable[] run(final long theRow, final long[] theColumnIds)
            throws VoltAbortException {
        if (theColumnIds == null || theColumnIds.length == 0)
            return null;
        Arrays.sort(theColumnIds);
        voltQueueSQL(sql, theRow, theColumnIds[0],
                theColumnIds[theColumnIds.length - 1]);

        final VoltTable[] results = voltExecuteSQL();
        // TODO: This looks like a constant, maybe it should be static ...
        final ColumnInfo[] columns = new ColumnInfo[] {
                new ColumnInfo("ROW_KEY", VoltType.BIGINT),
                new ColumnInfo("COLUMN_KEY", VoltType.BIGINT),
                new ColumnInfo("DATA_BLOB", VoltType.VARBINARY),
                new ColumnInfo("LAST_MODIFIED", VoltType.TIMESTAMP) };

        final VoltTable results2 = new VoltTable(columns);

        final VoltTable table = results[0];
        int count = 0;
        while (table.advanceRow()) {
            final VoltTableRow r = table.fetchRow(count++);
            // Since the columns are sorted, the very least we should
            // do is use a binary search in contains()
            if (contains(theColumnIds, r.getLong(1))) {
                results2.add(r);
            }
        }
        return new VoltTable[] { results2 };
    }
}
