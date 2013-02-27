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
// $codepro.audit.disable methodInvocationInLoopCondition
package com.blockwithme.longdb.voltdb.server.base;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.ParametersAreNonnullByDefault;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;

/** The Base class for 'InsertUpdateRemove' store procedure */
@ParametersAreNonnullByDefault
public abstract class InsertUpdateRemove extends VoltProcedure implements
        IBaseStoredProc {

    /**  */
    private static final VoltTable[] VOLT_TABLES_EMPTY_ARRAY = new VoltTable[0];

    /** The delete sql. */
    protected final SQLStmt deleteSql = new SQLStmt("DELETE FROM "
            + tableName() + " WHERE ROW_KEY=? AND COLUMN_KEY=?;");

    /** The insert sql. */
    protected final SQLStmt insertSql = new SQLStmt("INSERT INTO "
            + tableName() + " VALUES (?, ?, ?, ?);");

    /** The select sql. */
    protected final SQLStmt selectSql = new SQLStmt("SELECT COLUMN_KEY FROM "
            + tableName() + " WHERE ROW_KEY=? ;");

    /** The update sql. */
    protected final SQLStmt updateSql = new SQLStmt("UPDATE " + tableName()
            + " SET DATA_BLOB=?, LAST_MODIFIED=? "
            + "WHERE ROW_KEY=? AND COLUMN_KEY=?;");

    /** Gets the 2D blobs from combined byte arrays and index array. */
    private static Map<Long, byte[]> get2Dblobs(final long[] theColumnIds,
            final byte[] theCombinedBlob, final int[] theIndexArray) {

        final Map<Long, byte[]> blobsMap = new LinkedHashMap<Long, byte[]>();

        if (theColumnIds.length - 1 != theIndexArray.length)
            throw new IllegalArgumentException(
                    "Number of column ids do not match with number of blobs");
        if (theColumnIds.length == 1) {
            blobsMap.put(theColumnIds[0], theCombinedBlob);
            return blobsMap;
        }
        int count = 0;
        int startIndex = 0;
        int endIndex = 0;
        for (final long colId : theColumnIds) {
            endIndex = (count < theIndexArray.length) ? theIndexArray[count]
                    : theCombinedBlob.length;
            final byte[] blob = Arrays.copyOfRange(theCombinedBlob, startIndex,
                    endIndex);
            blobsMap.put(colId, blob);
            startIndex = endIndex;
            count++;
        }
        return blobsMap;
    }

    /** Check existing values. */
    private void checkExistingValues(final Map<Long, byte[]> theBlobsMap,
            final VoltTable[] theSelResults, final List<Long> theUpdateList,
            final List<Long> theInsertList) {
        int count = 0;
        if (theSelResults.length > 0) {
            final VoltTable table = theSelResults[0];
            while (table.advanceRow()) {
                final VoltTableRow row = table.fetchRow(count++);
                final Long colId = row.getLong(0);
                if (theBlobsMap.containsKey(colId))
                    theUpdateList.add(colId);
            }
        }
        theInsertList.addAll(theBlobsMap.keySet());
        theInsertList.removeAll(theUpdateList);
    }

    /** Check remove only. */
    private boolean checkRemoveOnly(final long[] theUpdateColIDs,
            final byte[] theUpdateBolbs, final int[] theIndexArray) {
        if (theUpdateColIDs == null || theUpdateColIDs.length == 0)
            return true;
        return false;
    }

    /** Execute inserts. */
    private void executeInserts(final long theRowKey,
            final Map<Long, byte[]> theBlobsMap, final long theLastModified,
            final List<Long> theInsertList, final List<VoltTable> theResults) {
        for (final Long colId : theInsertList) {
            if (theBlobsMap.containsKey(colId)) {
                voltQueueSQL(insertSql, theRowKey, colId,
                        theBlobsMap.get(colId), theLastModified);
                theResults.addAll(Arrays.asList(voltExecuteSQL()));
            }
        }
    }

    /** Execute updates. */
    private void executeUpdates(final long theRowKey,
            final Map<Long, byte[]> theBlobsMap, final long theLastModified,
            final List<Long> theUpdateList, final List<VoltTable> theResults) {

        for (final Long colId : theUpdateList) {
            voltQueueSQL(updateSql, theBlobsMap.get(colId), theLastModified,
                    theRowKey, colId);
            theResults.addAll(Arrays.asList(voltExecuteSQL()));
        }
    }

    /** Performs update sql for already existing records.
     * 
     * @param theRowKey
     *        the row key
     * @param theUpdateColIDs
     *        the column ids to be updated
     * @param theCombinedBlob
     *        all the blobs combined into single byte array
     * @param theIndexArray
     *        the indexes to be used to break the 'blobsCombined'
     * @param theLastModified
     *        the last modified timestamp
     * @param theResults
     *        the results from previous query, results of this query will be
     *        combined and resultant VoltTable[] array is returned by this
     *        method
     * @return the resultant VoltTable[] */
    protected VoltTable[] updateData(final long theRowKey,
            final long[] theUpdateColIDs, final byte[] theCombinedBlob,
            final int[] theIndexArray, final long theLastModified,
            final List<VoltTable> theResults) {

        if (checkRemoveOnly(theUpdateColIDs, theCombinedBlob, theIndexArray)) {
            return theResults.toArray(VOLT_TABLES_EMPTY_ARRAY);
        }

        final Map<Long, byte[]> blobsMap = get2Dblobs(theUpdateColIDs,
                theCombinedBlob, theIndexArray);

        voltQueueSQL(selectSql, theRowKey);
        final VoltTable[] selResults = voltExecuteSQL();

        if (theUpdateColIDs != null && theUpdateColIDs.length > 0) {
            final List<Long> updateList = new ArrayList<Long>();
            final List<Long> insertList = new ArrayList<Long>();
            checkExistingValues(blobsMap, selResults, updateList, insertList);
            if (!updateList.isEmpty()) {
                executeUpdates(theRowKey, blobsMap, theLastModified,
                        updateList, theResults);
            }
            if (!insertList.isEmpty()) {
                executeInserts(theRowKey, blobsMap, theLastModified,
                        insertList, theResults);
            }
        }
        return theResults.toArray(VOLT_TABLES_EMPTY_ARRAY);
    }

    /** The 'run' method for this stored procedure.
     * 
     * @param theRowKey
     *        the row key
     * @param theUpdateColIDs
     *        the column ids to be updated
     * @param theCombinedBlob
     *        all the blobs combined into single byte array
     * @param theIndexArray
     *        the indexes to be used to break the 'blobsCombined'
     * @param theRemoveIds
     *        the column ids to be removed
     * @return the resultant VoltTable[]
     * @throws VoltAbortException
     *         the volt abort exception */
    public VoltTable[] run(final long theRowKey, final long[] theUpdateColIDs,
            final byte[] theCombinedBlob, final int[] theIndexArray,
            final long[] theRemoveIds) throws VoltAbortException {

        // final long lastModified = this.getTransactionTime().getTime();
        final long lastModified = System.currentTimeMillis();
        final List<VoltTable> results = new ArrayList<VoltTable>();
        if (theRemoveIds != null && theRemoveIds.length > 0) {
            for (final long colId : theRemoveIds) {
                voltQueueSQL(deleteSql, theRowKey, colId);
                results.addAll(Arrays.asList(voltExecuteSQL()));
            }
        }
        return updateData(theRowKey, theUpdateColIDs, theCombinedBlob,
                theIndexArray, lastModified, results);
    }

}
