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
// $codepro.audit.disable handleNumericParsingErrors
package com.blockwithme.longdb.test.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.blockwithme.longdb.BETable;
import com.blockwithme.longdb.Columns;
import com.blockwithme.longdb.Range;
import com.blockwithme.longdb.entities.Bytes;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.google.common.base.Charsets;

/** Utility Methods to work with Test Data file in JSON format. All the JSON
 * related objects will be dealt with in this class */
public class JSONUtil {

    /** 1k bytes chunk size */
    private static final int ONE_K = 1024;

    /** The blob map. */
    private static final Map<String, byte[]> BLOB_MAP = new HashMap<String, byte[]>();

    /** The j table. */
    private final JSONObject jTable;

    /** The root json. */
    private final JSONObject rootJSON;

    /** Reads file as string.
     * 
     * @param theFilePath
     *        the file path
     * @return the the file as a string
     * @throws IOException
     *         Signals that an I/O exception has occurred. */
    private static String readFileAsString(final String theFilePath)
            throws java.io.IOException {
        final StringBuilder fileData = new StringBuilder();
        final InputStream inputStream = JSONUtil.class
                .getResourceAsStream(theFilePath);
        final BufferedReader reader = new BufferedReader(new InputStreamReader(
                inputStream, Charsets.UTF_8.name()));
        try {
            char[] buf = new char[ONE_K];
            int numRead = 0;
            while ((numRead = reader.read(buf)) != -1) {
                final String readData = String.valueOf(buf, 0, numRead);
                fileData.append(readData);
                buf = new char[ONE_K];
            }
        } finally {
            reader.close();
        }
        return fileData.toString();
    }

    /** This constructor reads the file contents and initializes JSON objects, it
     * expects that there is a top level element with the name "table".
     * 
     * @param theFile
     *        the json file
     * @throws IOException
     *         Signals that an I/O exception has occurred.
     * @throws JSONException
     *         the jSON exception */
    public JSONUtil(final String theFile) throws IOException, JSONException {

        final String content = readFileAsString(theFile);
        rootJSON = new JSONObject(content);
        jTable = rootJSON.getJSONObject("table");
    }

    /** Adds the column data.
     * 
     * @param theRowID
     *        the row id
     * @param theColumn
     *        the column
     * @throws JSONException
     *         the jSON exception */
    private void addColumnData(final long theRowID, final JSONObject theColumn)
            throws JSONException {

        final JSONArray rowArray = jTable.getJSONArray("row");
        for (int i = 0; i < rowArray.length(); i++) {
            final JSONObject row = rowArray.getJSONObject(i);
            final long rowKey = row.getLong("ID");
            if (rowKey == theRowID) {
                final JSONArray colArray = row.getJSONArray("column");
                final JSONObject columnCopy = copyColumn(theColumn);
                colArray.put(colArray.length(), columnCopy);
            }
        }
    }

    /** Copy column.
     * 
     * @param theColumn
     *        the column
     * @return the jSON object
     * @throws JSONException
     *         the jSON exception */
    private JSONObject copyColumn(final JSONObject theColumn)
            throws JSONException {
        return new JSONObject(theColumn.toString());
    }

    /** Generate random byte array.
     * 
     * @param theBlobsize
     *        the blobsize
     * @return the byte[] */
    private byte[] generateRandomByteArray(final int theBlobsize) {
        final byte[] bytesBuffer = new byte[theBlobsize];
        final Random rnd = new Random();
        rnd.nextBytes(bytesBuffer);
        return bytesBuffer;
    }

    /** Gets the column index.
     * 
     * @param theColArray
     *        the column array
     * @param theColumnID
     *        the column id
     * @return the column index
     * @throws JSONException
     *         the jSON exception */
    private int getColumnIndex(final JSONArray theColArray,
            final long theColumnID) throws JSONException {
        int index = -1;
        for (int i = 0; i < theColArray.length(); i++) {
            final long oldID = theColArray.getJSONObject(i).getLong("ID");
            if (oldID == theColumnID)
                index = i;
        }
        return index;

    }

    /** Load column. */
    private Bytes loadColumn(final JSONObject theColumn, final long theRowId,
            final int theIndex) throws JSONException,
            UnsupportedEncodingException {

        Bytes returnValue = null;
        final String type = theColumn.getString("type");

        if ("int".equals(type)) {
            final int value = theColumn.getInt("value");
            returnValue = new Bytes(value);
        } else if ("long".equals(type)) {
            final long value = theColumn.getLong("value");
            returnValue = new Bytes(value);
        } else if ("double".equals(type)) {
            final double value = theColumn.getDouble("value");
            returnValue = new Bytes(value);
        } else if ("short".equals(type)) {
            final short value = Short.valueOf(theColumn.getString("value"));
            returnValue = new Bytes(value);
        } else if ("byte".equals(type)) {
            final byte value = Byte.valueOf(theColumn.getString("value"));
            returnValue = new Bytes(value);
        } else if ("String".equals(type)) {
            final String value = theColumn.getString("value");
            returnValue = new Bytes(value.getBytes("UTF-8"));
        } else if (type.contains("blob")) {

            final String sizeStr = type.substring(type.indexOf('(') + 1,
                    type.indexOf(')'));
            final int blobsize = Integer.valueOf(sizeStr);
            final byte[] blobBytes = generateRandomByteArray(blobsize);
            returnValue = new Bytes(blobBytes);
            theColumn.put("type", "convertedBolb");
            theColumn.put("value", "convertedBolb");
            final String mapKey = theColumn.getString("ID") + ':' + theRowId;
            BLOB_MAP.put(mapKey, blobBytes);

        } else if ("convertedBolb".equals(type)) {
            final String mapKey = theColumn.getString("ID") + ":" + theRowId;
            final byte[] blobBytes = BLOB_MAP.get(mapKey);
            returnValue = new Bytes(blobBytes);
        }

        return returnValue;
    }

    /** Load row. */
    private void loadRow(final BETable theTable, final JSONArray theRowArray,
            final int theIndex) throws JSONException,
            UnsupportedEncodingException {
        final JSONObject row = theRowArray.getJSONObject(theIndex);
        final long rowKey = row.getLong("ID");
        final JSONArray colArray = row.getJSONArray("column");
        final LongObjectOpenHashMap<Bytes> colMap = new LongObjectOpenHashMap<Bytes>();

        for (int j = 0; j < colArray.length(); j++) {
            final JSONObject column = colArray.getJSONObject(j);
            final long cID = column.getLong("ID");
            final Bytes bytes = loadColumn(column, rowKey, j);
            colMap.put(cID, bytes);
        }
        final Columns cols = new Columns(colMap, false);
        theTable.set(rowKey, cols);
    }

    /** Modify column data. */
    private void modifyColumnData(final long theRowID,
            final JSONObject theNewColumn) throws JSONException {

        final JSONArray rowArray = jTable.getJSONArray("row");
        for (int i = 0; i < rowArray.length(); i++) {
            final JSONObject row = rowArray.getJSONObject(i);
            final long rowKey = row.getLong("ID");
            if (rowKey == theRowID) {
                final JSONArray colArray = row.getJSONArray("column");
                final JSONObject columnCopy = copyColumn(theNewColumn);
                final int columnIndex = getColumnIndex(colArray,
                        theNewColumn.getLong("ID"));
                colArray.put(columnIndex, columnCopy);
            }
        }
    }

    /** Prepare remove column list. */
    private void processRemoveColList(final LongArrayList theRemoveList,
            final JSONObject theRow, final long theRowKey) throws JSONException {
        // get column ids to be deleted.
        final JSONArray removeColIDArray = theRow.getJSONArray("deleteCols");
        final LongArrayList currentColumns = this.getColumnIDs(theRowKey);
        if (removeColIDArray == null || removeColIDArray.length() == 0)
            return;
        for (int j = 0; j < removeColIDArray.length(); j++) {
            final JSONObject column = removeColIDArray.getJSONObject(j);
            final long cID = column.getLong("ID");
            if (!currentColumns.contains(cID))
                throw new JSONException("Column ID:" + cID
                        + " does not exsit in row ID " + theRowKey);
            theRemoveList.add(cID);
            removeColFromRow(theRowKey, cID);
        }
    }

    /** Process remove column range. */
    private void processRemoveColRange(final Range theRemoveRange,
            final JSONObject theRow, final long theRowKey) throws JSONException {

        final JSONObject jRange = theRow.getJSONObject("deleteRange");
        if (jRange == null)
            return;
        theRemoveRange.start(jRange.getLong("RangeStart"));
        theRemoveRange.end(jRange.getLong("RangeEnd"));

        final LongArrayList columnIDs = this.getColumnIDs(theRowKey);
        for (final LongCursor longCursor : columnIDs) {
            if (theRemoveRange.contains(longCursor.value))
                this.removeColFromRow(theRowKey, longCursor.value);
        }
    }

    /** Removes the column from row. */
    private void removeColFromRow(final long theRowID, final long theId)
            throws JSONException {

        final JSONArray rowArray = jTable.getJSONArray("row");
        for (int i = 0; i < rowArray.length(); i++) {
            final JSONObject row = rowArray.getJSONObject(i);
            final long rowKey = row.getLong("ID");
            if (rowKey == theRowID) {
                final JSONArray colArray = row.getJSONArray("column");
                final int columnIndex = getColumnIndex(colArray, theId);
                colArray.remove(columnIndex);
            }
        }

    }

    /** Removes the row by id. */
    private void removeRowByID(final long theRowId) throws JSONException {

        final JSONArray rowArray = jTable.getJSONArray("row");
        int index = -1;
        for (int i = 0; i < rowArray.length(); i++) {
            final JSONObject row = rowArray.getJSONObject(i);
            final long rowKey = row.getLong("ID");
            if (rowKey == theRowId) {
                index = i;
            }
        }
        if (index >= 0)
            rowArray.remove(index);

    }

    /** This method reads the element represented by "elementName" from the JSON
     * file. This test data in JSON file will contain new columns to be added to
     * existing rows AND column ids of the columns to be removed. Two Map
     * objects are passed as arguments to this method which get populated as per
     * the data in the JSON file. The fist Map arg is the Map of row Ids vs new
     * Columns. The second Map is a map of row Ids vs List of column Ids to be
     * removed. The current Test data encapsulated by this class is also updated
     * accordingly.
     * 
     * @param theElementName
     *        the element name
     * @param theRowsMap
     *        the rows map
     * @param theRemoveColumnsMap
     *        the columns map to be removed
     * @param theRemoveRangeMap
     *        the remove range map
     * @throws JSONException
     *         the jSON exception
     * @throws UnsupportedEncodingException */
    public void columnsAdded(final String theElementName,
            final LongObjectOpenHashMap<Columns> theRowsMap,
            final LongObjectOpenHashMap<LongArrayList> theRemoveColumnsMap,
            final LongObjectOpenHashMap<Range> theRemoveRangeMap)
            throws JSONException, UnsupportedEncodingException {

        final LongArrayList allRows = getRowIDs();
        final JSONObject addColumns = rootJSON.getJSONObject(theElementName);
        final JSONArray rowArray = addColumns.getJSONArray("row");

        for (int i = 0; i < rowArray.length(); i++) {

            final JSONObject row = rowArray.getJSONObject(i);
            final long rowKey = row.getLong("ID");

            if (theRowsMap != null) {
                if (!allRows.contains(rowKey))
                    throw new JSONException("Row ID:" + rowKey
                            + " mentioned in addColumns Does not exist.");
                final JSONArray colArray = row.getJSONArray("column");
                final LongObjectOpenHashMap<Bytes> colMap = new LongObjectOpenHashMap<Bytes>();
                final LongArrayList currentTableColumns = this
                        .getColumnIDs(rowKey);

                for (int j = 0; j < colArray.length(); j++) {
                    final JSONObject column = colArray.getJSONObject(j);
                    final long cID = column.getLong("ID");
                    if (currentTableColumns.contains(cID))
                        throw new JSONException("Column ID:" + cID
                                + " already exsits in row ID " + rowKey);
                    final Bytes bytes = loadColumn(column, rowKey, j);
                    colMap.put(cID, bytes);
                    addColumnData(rowKey, column);
                }
                final Columns cols = new Columns(colMap, false);
                theRowsMap.put(rowKey, cols);
            }
            if (theRemoveColumnsMap != null) {
                final LongArrayList colsToRemove = new LongArrayList();
                processRemoveColList(colsToRemove, row, rowKey);
                if (colsToRemove.size() > 0)
                    theRemoveColumnsMap.put(rowKey, colsToRemove);
            }
            if (theRemoveRangeMap != null) {
                final Range removeRange = new Range(0, 0);
                processRemoveColRange(removeRange, row, rowKey);
                if (!removeRange.empty())
                    theRemoveRangeMap.put(rowKey, removeRange);
            }
        }
    }

    /** This method reads the element represented by "elementName" from the JSON
     * file. This test data in JSON file will contain modified values of the
     * existing columns AND column ids of the columns to be removed. Two Map
     * objects are passed as arguments to this method which get populated as per
     * the data in the JSON file. The fist Map arg is the Map of row Ids vs
     * Modified Columns. The second Map is a map of row Ids vs List of column
     * Ids to be removed. The current Test data encapsulated by this class is
     * also updated accordingly.
     * 
     * @param theElementName
     *        the element name
     * @param theRowsMap
     *        the rows map
     * @param theRemoveList
     *        the remove list
     * @param theRemoveRangeMap
     *        the remove range map
     * @throws JSONException
     *         the jSON exception
     * @throws UnsupportedEncodingException */
    public void columnsModified(final String theElementName,
            final LongObjectOpenHashMap<Columns> theRowsMap,
            final LongObjectOpenHashMap<LongArrayList> theRemoveList,
            final LongObjectOpenHashMap<Range> theRemoveRangeMap)
            throws JSONException, UnsupportedEncodingException {

        final LongArrayList allRows = getRowIDs();

        final JSONObject addColumns = rootJSON.getJSONObject(theElementName);
        final JSONArray rowArray = addColumns.getJSONArray("row");

        for (int i = 0; i < rowArray.length(); i++) {

            final JSONObject row = rowArray.getJSONObject(i);
            final long rowKey = row.getLong("ID");

            if (theRowsMap != null) {
                if (!allRows.contains(rowKey))
                    throw new JSONException("Row ID:" + rowKey
                            + " mentioned in addColumns Does not exist.");
                final JSONArray colArray = row.getJSONArray("column");
                final LongObjectOpenHashMap<Bytes> colMap = new LongObjectOpenHashMap<Bytes>();
                final LongArrayList currentTableColumns = this
                        .getColumnIDs(rowKey);

                for (int j = 0; j < colArray.length(); j++) {
                    final JSONObject column = colArray.getJSONObject(j);
                    final long cID = column.getLong("ID");
                    if (!currentTableColumns.contains(cID))
                        throw new JSONException("Column ID:" + cID
                                + " does not exsit in row ID " + rowKey);
                    final Bytes bytes = loadColumn(column, rowKey, j);
                    colMap.put(cID, bytes);
                    modifyColumnData(rowKey, column);

                }
                final Columns cols = new Columns(colMap, false);
                theRowsMap.put(rowKey, cols);
            }
            if (theRemoveList != null) {
                final LongArrayList colsToRemove = new LongArrayList();
                processRemoveColList(colsToRemove, row, rowKey);
                if (colsToRemove.size() > 0)
                    theRemoveList.put(rowKey, colsToRemove);
            }
            if (theRemoveRangeMap != null) {
                final Range removeRange = new Range(0, 0);
                processRemoveColRange(removeRange, row, rowKey);
                if (!removeRange.empty())
                    theRemoveRangeMap.put(rowKey, removeRange);
            }
        }
    }

    /** Gets the column count.
     * 
     * @param theId
     *        the row Id
     * @return the column count
     * @throws JSONException
     *         the jSON exception */
    public long getColumnCount(final long theId) throws JSONException {
        long colms = -1;
        final JSONArray rowArray = jTable.getJSONArray("row");

        for (int i = 0; i < rowArray.length(); i++) {
            final JSONObject row = rowArray.getJSONObject(i);
            final long rowKey = row.getLong("ID");
            if (rowKey == theId) {
                final JSONArray colArray = row.getJSONArray("column");
                colms = colArray.length();
            }
        }
        return colms;
    }

    /** Method returns column data for all the columns for a given row from the
     * current Test data.
     * 
     * @param theId
     *        the row Id
     * @return the column data
     * @throws JSONException
     *         the jSON exception
     * @throws UnsupportedEncodingException */
    public LongObjectOpenHashMap<Bytes> getColumnData(final long theId)
            throws JSONException, UnsupportedEncodingException {

        final LongObjectOpenHashMap<Bytes> colmsData = new LongObjectOpenHashMap<Bytes>();
        final JSONArray rowArray = jTable.getJSONArray("row");

        for (int i = 0; i < rowArray.length(); i++) {
            final JSONObject row = rowArray.getJSONObject(i);
            final long rowKey = row.getLong("ID");
            if (rowKey == theId) {
                final JSONArray colArray = row.getJSONArray("column");
                for (int j = 0; j < colArray.length(); j++) {
                    final JSONObject column = colArray.getJSONObject(j);
                    final long cID = column.getLong("ID");
                    final Bytes bytes = loadColumn(column, rowKey, j);
                    colmsData.put(cID, bytes);
                }
            }
        }
        return colmsData;
    }

    /** Method returns columnIds for a given row from the current Test data.
     * 
     * @param theId
     *        the row Id
     * @return the column Ids
     * @throws JSONException
     *         the jSON exception */
    public LongArrayList getColumnIDs(final long theId) throws JSONException {
        final LongArrayList colmsIDs = new LongArrayList();
        final JSONArray rowArray = jTable.getJSONArray("row");

        for (int i = 0; i < rowArray.length(); i++) {
            final JSONObject row = rowArray.getJSONObject(i);
            final long rowKey = row.getLong("ID");
            if (rowKey == theId) {
                final JSONArray colArray = row.getJSONArray("column");
                for (int j = 0; j < colArray.length(); j++) {
                    final JSONObject column = colArray.getJSONObject(j);
                    colmsIDs.add(column.getLong("ID"));
                }
            }
        }
        return colmsIDs;
    }

    /** Method returns all the rowIds for the current Test data.
     * 
     * @return the row Ids
     * @throws JSONException
     *         the jSON exception */
    public LongArrayList getRowIDs() throws JSONException {
        final LongArrayList rowIDs = new LongArrayList();
        final JSONArray rowArray = jTable.getJSONArray("row");
        for (int i = 0; i < rowArray.length(); i++) {
            final JSONObject row = rowArray.getJSONObject(i);
            final long rowKey = row.getLong("ID");
            rowIDs.add(rowKey);
        }
        return rowIDs;
    }

    /** Method performs DB inserts for the data inside the array of "table":"row"
     * elements.
     * 
     * @param theTable
     *        the database table
     * @throws IOException
     *         Signals that an I/O exception has occurred.
     * @throws JSONException
     *         the jSON exception */
    public void loadTableFromJSONFile(final BETable theTable)
            throws IOException, JSONException {
        final JSONArray rowArray = jTable.getJSONArray("row");
        for (int i = 0; i < rowArray.length(); i++) {
            loadRow(theTable, rowArray, i);
        }
    }

    /** Removes the rows.
     * 
     * @param theElementName
     *        the element name
     * @return the long array list
     * @throws JSONException
     *         the jSON exception */
    public LongArrayList removeRows(final String theElementName)
            throws JSONException {

        final LongArrayList removeRows = new LongArrayList();
        final JSONObject jRemoveRows = rootJSON.getJSONObject(theElementName);
        final JSONArray rowArray = jRemoveRows.getJSONArray("rowID");
        for (int i = 0; i < rowArray.length(); i++) {
            removeRows.add(rowArray.getLong(i));
            removeRowByID(rowArray.getLong(i));
        }
        return removeRows;

    }

}
