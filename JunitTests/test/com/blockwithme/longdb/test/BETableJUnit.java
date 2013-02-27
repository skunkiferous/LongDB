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
// $codepro.audit.disable assignmentToNonFinalStatic, debuggingCode
//CHECKSTYLE stop magic number check
package com.blockwithme.longdb.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.json.JSONException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.blockwithme.longdb.BEDatabase;
import com.blockwithme.longdb.BETable;
import com.blockwithme.longdb.BETableProfile;
import com.blockwithme.longdb.Backend;
import com.blockwithme.longdb.Columns;
import com.blockwithme.longdb.Range;
import com.blockwithme.longdb.entities.Base36;
import com.blockwithme.longdb.entities.Bytes;
import com.blockwithme.longdb.entities.LongHolder;
import com.blockwithme.longdb.test.util.JSONUtil;
import com.blockwithme.longdb.test.util.Util;
import com.blockwithme.longdb.voltdb.VoltDBBackend;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.LongCursor;

/** JUunitTest class for all the public methods in BETable Test Data is loaded
 * from an external JSON file. */
public abstract class BETableJUnit {

    /**  */
    private static final long[] ZERO_LONG_ARRAY = new long[0];

    /** The backend instance. */
    private static Backend BE;

    /** The DB_NAME. */
    private static final String DB_NAME = "default";

    /** The json file reader utility. */
    private static JSONUtil J_UTIL;

    /** The TEST TABLE NAME. */
    private static final String TEST_TABLE_NAME = "DEFAULTTABLE";

    /** The database. */
    private BEDatabase database;

    /** The table. */
    private BETable table;

    // CHECKSTYLE IGNORE FOR NEXT 3 LINES
    /** The name. */
    @Rule
    public final TestName name = new TestName();

    /** Sort desending. */
    private static long[] sortDesending(final long[] theArray) {
        Arrays.sort(theArray);
        final int count = theArray.length;
        if (count > 1) {
            final int stop = count / 2;
            final int top = count - 1;
            for (int i = 0; i < stop; i++) {
                final long tmp = theArray[i];
                final int other = top - i;
                theArray[i] = theArray[other];
                theArray[other] = tmp;
            }
        }
        return theArray;
    }

    /** Clean up method. */
    public static void clean() throws IOException {
        // NOP
    }

    /* Loading the JSON file here. */
    /** Sets the up before class. */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        clean();
    }

    /** Shut down. */
    @AfterClass
    public static void shutDown() throws IOException {
        BE.shutdown();
    }

    /** Tear down after class. */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        clean();
    }

    /** Instantiates a new BDB Junit tests. */
    protected BETableJUnit() {
        Util.setLogConfig();
        doInit();
    }

    /** Clean up.
     * 
     * @throws IOException
     *         Signals that an I/O exception has occurred.
     * @throws JSONException
     *         the jSON exception */
    private void cleanUp() throws IOException, JSONException {
        try {
            database.drop(Base36.get(TEST_TABLE_NAME));
        } catch (final UnsupportedOperationException e) {
            // ignore this.
        } catch (final Exception e) {
            System.err.println("Exception in cleanup " + name.getMethodName());
            e.printStackTrace(System.err);
        } finally {
            database.close();
        }
    }

    /** Do init. */
    private void doInit() {
        BE = this.backend();
        BEDatabase dbInstance = BE.openDatabase(DB_NAME);
        if (dbInstance == null)
            dbInstance = BE.createDatabase(DB_NAME);
        final Base36 tbl = Base36.get(TEST_TABLE_NAME);
        if (dbInstance.get(tbl) != null && !(BE instanceof VoltDBBackend))
            dbInstance.drop(tbl);
        else if ((BE instanceof VoltDBBackend)) {
            final BETable t = dbInstance.get(tbl);
            assertNotNull("Table not loaded: " + tbl, t);
            final Iterator<LongHolder> itr = t.keys();
            while (itr.hasNext()) {
                t.remove(itr.next().value());
            }
        }
    }

    /** Gets the min max range from id list.
     * 
     * @param theIDList
     *        the i d list
     * @return the min max range from id list */
    private Range getMinMaxRangeFromIDList(final LongArrayList theIDList) {

        if (theIDList == null || theIDList.isEmpty() || theIDList.size() == 1)
            return null;
        if (theIDList.size() == 2) {
            return (theIDList.get(0) < theIDList.get(1)) ? new Range(
                    theIDList.get(0), theIDList.get(1)) : new Range(
                    theIDList.get(1), theIDList.get(0));
        } else {
            final long[] ids = theIDList.toArray();
            // OK for tests, but not OK for impl: sort takes "n*log(n)" but
            // finding min and max takes only "n".
            Arrays.sort(ids);
            return new Range(ids[0], ids[ids.length - 1]);
        }
    }

    /** Initializes the db.
     * 
     * @throws IOException
     *         Signals that an I/O exception has occurred.
     * @throws JSONException
     *         the jSON exception */
    private void initDB() throws IOException, JSONException {
        J_UTIL = new JSONUtil("/InputJSON.txt");
        database = BE.openDatabase(DB_NAME);
        assert (database != null);
        final Base36 tbl = Base36.get(TEST_TABLE_NAME);
        final BETableProfile profile = new BETableProfile();
        try {
            table = database.create(tbl, profile);
        } catch (final IllegalArgumentException e) {
            // TODO fix this, not normal
            table = database.get(tbl);
        }
        /** All the data mentioned in the JSON file under the element name
         * "table" will be inserted as Test data. */
        J_UTIL.loadTableFromJSONFile(table);
    }

    /** List to array.
     * 
     * @param theColIdList
     *        the column id list
     * @return the long[] */
    private long[] listToArray(final List<Long> theColIdList) {
        final long[] colIds = new long[theColIdList.size()];
        int count = 0;
        for (final Long long1 : theColIdList) {
            colIds[count++] = long1;
        }
        return colIds;
    }

    /*
     * This method validated all the test data rows and compares the column
     * values with the Test data encapsulated in JSONUtil class.
     */
    /** Validate all test rows. */
    private void validateAllTestRows() {

        try {
            final LongArrayList rowIDs = J_UTIL.getRowIDs();

            for (final LongCursor rowCursor : rowIDs) {
                final LongObjectOpenHashMap<Bytes> jColumnsData = J_UTIL
                        .getColumnData(rowCursor.value);
                final Columns dbColumns = table.get(rowCursor.value);
                assertNotNull("No Columns found for row ID:" + rowCursor.value,
                        dbColumns);
                for (final LongHolder longHolder : dbColumns) {
                    final long colkey = longHolder.value();
                    assertTrue(colkey + " Not present in row "
                            + rowCursor.value, dbColumns.containsColumn(colkey));
                    final Bytes dbBytes = dbColumns.getBytes(colkey);
                    assertNotNull("data not present for column id " + colkey
                            + " row id " + rowCursor.value, dbBytes);
                    final Bytes jBytes = jColumnsData.get(colkey);
                    assertEquals("Assertion failure for row : "
                            + rowCursor.value + " column ID :" + colkey,
                            dbBytes, jBytes);
                }
            }
        } catch (final JSONException exp) {
            fail(exp.getMessage());
        } catch (final UnsupportedEncodingException e) {
            fail(e.getMessage());
        }
    }

    /** Abstract method to be implemented by sub classes.
     * 
     * @return the backend */
    protected abstract Backend backend();

    /** Loads a Table using BE interfaces.
     * 
     * @throws Exception
     *         the exception */
    @Before
    public void setUp() throws Exception {
        initDB();
    }

    /** Tear down. */
    @After
    public void tearDown() throws Exception {
        cleanUp();
    }

    /** Testing BETable.columns(long key) Test is performed by retrieving all
     * Test-rows-ids from test data file. Calling the columns() method for each
     * test row and comparing result with column IDs for corresponding rows in
     * the Test data */
    @Test
    public void testColumns() throws Exception {
        final LongArrayList rowIDs = J_UTIL.getRowIDs();
        for (final LongCursor rowCursor : rowIDs) {
            final LongArrayList dbColumns = table.columns(rowCursor.value);
            assertNotNull("No columns present for row id " + rowCursor.value,
                    dbColumns);
            final LongArrayList jColumns = J_UTIL.getColumnIDs(rowCursor.value);
            assertEquals("Column count mismatch for row ID:" + rowCursor.value,
                    jColumns.size(), dbColumns.size());
            for (final LongCursor cloumnCursor : dbColumns) {
                assertTrue("Column ID mismatch for row ID:" + rowCursor.value,
                        jColumns.contains(cloumnCursor.value));
            }
        }

    }

    /** Testing BETable.columnsCount(long key) Test is performed by retrieving
     * all Test-rows-ids from test data file. Calling the getColumnCount()
     * method for each test row and comparing result with column counts for
     * corresponding rows in the Test data */
    @Test
    public void testColumnsCount() throws Exception {
        final LongArrayList rowIDs = J_UTIL.getRowIDs();
        for (final LongCursor longCursor : rowIDs) {
            assertEquals(
                    "Column count mismatch for row ID:" + longCursor.value,
                    J_UTIL.getColumnCount(longCursor.value),
                    table.columnsCount(longCursor.value));
        }

    }

    /** Testing BETable.columnsIterator(long key) Test is performed by retrieving
     * all Test-rows-ids from test data file. Calling the columnsIterator()
     * method for each test row and comparing result with column IDs for
     * corresponding rows in the Test data */
    @Test
    public void testColumnsIterator() throws Exception {
        final LongArrayList rowIDs = J_UTIL.getRowIDs();
        for (final LongCursor rowCursor : rowIDs) {
            final Iterator<LongHolder> dbColumns = table
                    .columnsIterator(rowCursor.value);
            assertNotNull("No columns present for row id" + rowCursor.value,
                    dbColumns);
            final LongArrayList jColumnsIDs = J_UTIL
                    .getColumnIDs(rowCursor.value);
            while (dbColumns.hasNext()) {
                final long dbColumnID = dbColumns.next().value();
                assertTrue("dbColumnID ID:" + dbColumnID
                        + " not found in Test data for row ID:"
                        + rowCursor.value, jColumnsIDs.contains(dbColumnID));
            }
        }
    }

    /** Test database drop create database. */
    @Test
    public void testDBDropCreateDB() {
        try {
            // drop if db already exists
            final String dbName = "TestDummyDB";
            BEDatabase d = BE.openDatabase(dbName);
            if (d != null)
                BE.dropDatabase(dbName);

            // not create a new DB.
            d = BE.createDatabase(dbName);
            d.create(Base36.get("DBOpen1"), new BETableProfile());
            d.create(Base36.get("DumCFbsdas"), new BETableProfile());

            d = BE.openDatabase(dbName);
            assertNotNull("Couldn't find Database: " + dbName, d);
            assertEquals("Database name mismatch", dbName.toLowerCase(), d
                    .database().toLowerCase());
            // drop db.
            BE.dropDatabase(dbName);
        } catch (final UnsupportedOperationException e) {
            // ignore this. this exception is expected for implementations that
            // do not support table/database creation
        }
    }

    /** Test db open close db. */
    @Test
    public void testDBOpenCloseDB() throws Exception {
        final Base36 tbl = Base36.get("DBOpen1");
        final Base36 tbl2 = Base36.get("DBOpen2");
        try {
            if (database.get(tbl) != null)
                database.drop(tbl);
            if (database.get(tbl2) != null)
                database.drop(tbl2);

            final BETableProfile profile = new BETableProfile();
            database.create(tbl, profile);

            final BETableProfile profile2 = new BETableProfile();
            database.create(tbl2, profile2);
            database = BE.openDatabase(database.database());
            assertNotNull("Could not load database ", database);
            assertFalse("Could not close database ", database.closed());

        } catch (final UnsupportedOperationException exp) {
            // ignore.
        } finally {
            try {
                database.drop(tbl);
                database.drop(tbl2);
            } catch (final UnsupportedOperationException e) {
                // ignore.
            }
        }
    }

    /** Test exists. */
    @Test
    public void testExists() throws Exception {
        final LongArrayList rowIDs = J_UTIL.getRowIDs();
        for (final LongCursor longCursor : rowIDs) {
            assertTrue("Expected row ID Does not exist:" + longCursor.value,
                    table.exists(longCursor.value));
        }
        final long[] rs = rowIDs.toArray();
        Arrays.sort(rs);
        final long nonExistentID = rs[rs.length - 1] + 1;
        assertFalse("Non Existent row ID Should not exist:" + nonExistentID,
                table.exists(nonExistentID));

    }

    /** Testing BETable.getLimited(long key, int count); */
    @Test
    public void testGetLimited() throws Exception {

        final LongArrayList rowIDs = J_UTIL.getRowIDs();
        for (final LongCursor rowCursor : rowIDs) {
            final LongObjectOpenHashMap<Bytes> jColumnsData = J_UTIL
                    .getColumnData(rowCursor.value);
            final Iterator<LongCursor> jKeyCursor = jColumnsData.keys()
                    .iterator();
            final LongArrayList colIDList = new LongArrayList();
            while (jKeyCursor.hasNext()) {
                final long colkey = jKeyCursor.next().value;
                colIDList.add(colkey);
                // the following method (BETable.getLimited(long row, int
                // count)) is being tested
                final Columns dbColumns = table.getLimited(rowCursor.value,
                        colIDList.size());
                assertNotNull("No Columns found for row ID:" + rowCursor.value,
                        dbColumns);
                for (final LongHolder longHolder : dbColumns) {
                    final long colkey2 = longHolder.value();
                    assertTrue("Test column ID:" + colkey2
                            + " not found in DB row ID:" + rowCursor.value,
                            dbColumns.containsColumn(colkey2));
                    final Bytes dbBytes = dbColumns.getBytes(colkey2);
                    assertNotNull("Data not found for column: " + colkey2
                            + " row: " + rowCursor.value, dbBytes);
                    final Bytes jBytes = jColumnsData.get(colkey2);
                    assertEquals("Column Values do not match for row ID:"
                            + rowCursor.value + " column ID:" + colkey2,
                            dbBytes, jBytes);
                }
                // negative tests:
                // pass limit more than the column size.
                final Columns dbColumns2 = table.getLimited(rowCursor.value,
                        jColumnsData.size() + 1);
                assertNotNull("Columns not found for row id: "
                        + rowCursor.value, dbColumns2);
                assertEquals("Limit more than the column size.",
                        jColumnsData.size(), dbColumns2.size());
                // pass zero as limit.
                final Columns dbColumns3 = table.getLimited(rowCursor.value, 0);
                assertNull("Setting Limit to zero should return null ",
                        dbColumns3);
                // TODO: The "normal case" is more like:
                // table.getLimited(key, jColumnsData.size()/2)
                // and it should be tested too!
            }
        }
    }

    /** Testing BETable.get(long key) Test is performed by retrieving all
     * Test-rows-ids from test data file. Calling the get(long key) method for
     * each test row and comparing result with column IDs and Bytes for
     * corresponding columns and rows in the Test data */
    @Test
    public void testGetLong() throws Exception {

        final LongArrayList rowIDs = J_UTIL.getRowIDs();
        for (final LongCursor rowCursor : rowIDs) {
            final Columns dbColumns = table.get(rowCursor.value);
            assertNotNull("Columns not found for row id: " + rowCursor.value,
                    dbColumns);
            final LongObjectOpenHashMap<Bytes> jColumns = J_UTIL
                    .getColumnData(rowCursor.value);
            assertEquals("Should return all the columns in a particular row.",
                    dbColumns.size(), jColumns.size());

            for (final LongHolder longHolder : dbColumns) {
                final long id1 = longHolder.value();
                assertTrue("dbColumnID ID:" + id1
                        + " not found in Test data for row ID:"
                        + rowCursor.value, jColumns.containsKey(id1));
                final Bytes bytes1 = dbColumns.getBytes(id1);
                assertNotNull("data not found for column: " + id1 + " row id: "
                        + rowCursor.value, bytes1);
                final Bytes bytes2 = jColumns.get(id1);
                assertEquals("Column Values do not match for row ID:"
                        + rowCursor.value + " column ID:" + id1, bytes1, bytes2);
            }
        }
    }

    /** Testing BETable.get(long key) Test is performed by retrieving all
     * Test-rows-ids from test data file. Calling the get(long key, long col)
     * method for each test row, column and comparing result with column IDs and
     * Bytes for corresponding columns and rows in the Test data */
    @Test
    public void testGetLongLong() throws Exception {
        final LongArrayList rowIDs = J_UTIL.getRowIDs();

        for (final LongCursor rowCursor : rowIDs) {
            final LongObjectOpenHashMap<Bytes> jColumnsData = J_UTIL
                    .getColumnData(rowCursor.value);
            final Iterator<LongCursor> jKeyCursor = jColumnsData.keys()
                    .iterator();
            while (jKeyCursor.hasNext()) {
                final long colkey = jKeyCursor.next().value;
                // following method (BETable.get(long row,long col)) is
                // being tested here
                final Columns dbColumns = table.get(rowCursor.value, colkey);
                assertNotNull("No Columns found for row ID:" + rowCursor.value,
                        dbColumns);
                assertEquals(
                        "Should return one column for every valid combination of rowID and colID",
                        dbColumns.size(), 1);
                assertTrue("Test column ID:" + colkey
                        + " not found in DB row ID:" + rowCursor.value,
                        dbColumns.containsColumn(colkey));
                final Bytes dbBytes = dbColumns.getBytes(colkey);
                assertNotNull("Data not found for column id: " + colkey
                        + " row Id: " + rowCursor.value, dbBytes);
                final Bytes jBytes = jColumnsData.get(colkey);
                assertEquals("Column Values do not match for row ID:"
                        + rowCursor.value + " column ID:" + colkey, dbBytes,
                        jBytes);
            }
        }
    }

    /** Testing BETable.get(long key, long... columns) Test is performed by
     * retrieving all Test-rows-ids from test data file. Getting all the column
     * ids for each row in the test data, creating multiple arrays of column ids
     * by adding one column id at a time to the array and Calling the get(long
     * key, long... columns) method. Comparing result with the Test data. */
    @Test
    public void testGetLongLongArray() throws Exception {

        final LongArrayList rowIDs = J_UTIL.getRowIDs();
        final Random rndm = new Random();

        for (final LongCursor rowCursor : rowIDs) {
            final LongObjectOpenHashMap<Bytes> jColumnsData = J_UTIL
                    .getColumnData(rowCursor.value);
            final Iterator<LongCursor> jKeyCursor = jColumnsData.keys()
                    .iterator();
            final List<Long> colIDList = new ArrayList<Long>();
            while (jKeyCursor.hasNext()) {
                final long colkey = jKeyCursor.next().value;
                colIDList.add(colkey);
                final long[] currentKeys = listToArray(colIDList);
                final Columns dbColumns = table.get(rowCursor.value,
                        currentKeys);
                assertNotNull("No Columns found for row ID:" + rowCursor.value,
                        dbColumns);
                assertEquals("Incorrect size of columns returned row ID:"
                        + rowCursor.value, colIDList.size(), dbColumns.size());
                assertTrue("Test column ID:" + colkey
                        + " not found in DB row ID:" + rowCursor.value,
                        dbColumns.containsColumn(colkey));
                for (final LongHolder longHolder : dbColumns) {
                    final long colkey2 = longHolder.value();
                    final Bytes dbBytes = dbColumns.getBytes(colkey2);
                    assertNotNull("No bytes found for row ID:"
                            + rowCursor.value + " Col ID: " + colkey2, dbBytes);
                    final Bytes jBytes = jColumnsData.get(colkey2);

                    assertEquals("Column Values do not match for row ID:"
                            + rowCursor.value + " column ID:" + colkey2,
                            Arrays.toString(jBytes.toArray(false)),
                            Arrays.toString(dbBytes.toArray(false)));
                    assertEquals("Column Values do not match for row ID:"
                            + rowCursor.value + " column ID:" + colkey2,
                            dbBytes, jBytes);

                }
            }
            // Other tests:
            // pass some extra ids to column array.
            final int currentListSize = colIDList.size();
            Long rnd = rndm.nextLong();
            if (!colIDList.contains(rnd))
                colIDList.add(rnd);
            rnd = rndm.nextLong();
            if (!colIDList.contains(rnd))
                colIDList.add(rnd);
            rnd = rndm.nextLong();
            if (!colIDList.contains(rnd))
                colIDList.add(rnd);
            final Columns dbColumns2 = table.get(rowCursor.value,
                    listToArray(colIDList));
            assertNotNull("Columns not found for row id: " + rowCursor.value,
                    dbColumns2);
            assertEquals("Column Size mismatch for row id: " + rowCursor.value,
                    dbColumns2.size(), currentListSize);
            // pass empty list.
            final Columns dbColumns3 = table.get(rowCursor.value,
                    ZERO_LONG_ARRAY);
            assertNull("Setting Empty Array should return null ", dbColumns3);
            // final Columns dbColumns4 = table.get(rowCursor.value,
            // (long[]) null);
            // assertNull("Setting Empty array should return null", dbColumns4);

        }
        // pass zero length array
        final Columns dbColumns5 = table.get(0, ZERO_LONG_ARRAY);
        assertNull("Passing zero as rowId should return null", dbColumns5);
        // // pass zero as rowId
        // final Columns dbColumns6 = table.get(0, (Range) null);
        // assertNull("Passing zero as rowId should return null", dbColumns6);

    }

    /** Testing BETable.get(long key, LongArrayList columns) Test is performed by
     * retrieving all Test-rows-ids from test data file. Getting all the column
     * ids for each row in the test data, creating multiple LongArrayList of
     * column ids by adding one column id at a time to the LongArrayList and
     * Calling the get(long key, LongArrayList columns) method. Comparing result
     * with the Test data. */
    @Test
    public void testGetLongLongArrayList() throws Exception {

        final LongArrayList rowIDs = J_UTIL.getRowIDs();
        final Random rndm = new Random();

        for (final LongCursor rowCursor : rowIDs) {
            final LongObjectOpenHashMap<Bytes> jColumnsData = J_UTIL
                    .getColumnData(rowCursor.value);
            final Iterator<LongCursor> jColCursor = jColumnsData.keys()
                    .iterator();
            final LongArrayList colIDList = new LongArrayList();

            while (jColCursor.hasNext()) {

                final long colkey = jColCursor.next().value;
                colIDList.add(colkey);
                // the following method (BETable.get(long row,LongArrayList
                // columns)) is being tested
                final Columns dbColumns = table.get(rowCursor.value, colIDList);
                assertNotNull("No Columns found for row ID:" + rowCursor.value,
                        dbColumns);
                assertEquals("Incorrect size of columns returned row ID:"
                        + +rowCursor.value, colIDList.size(), dbColumns.size());
                assertTrue("Test column ID:" + colkey
                        + " not found in DB row ID:" + rowCursor.value,
                        dbColumns.containsColumn(colkey));
                for (final LongHolder longHolder : dbColumns) {
                    final long colkey2 = longHolder.value();
                    final Bytes dbBytes = dbColumns.getBytes(colkey2);
                    assertNotNull("No bytes found for row ID:"
                            + rowCursor.value + " Col ID: " + colkey2, dbBytes);
                    final Bytes jBytes = jColumnsData.get(colkey2);
                    assertEquals("Column Values do not match for row ID:"
                            + rowCursor.value + " column ID:" + colkey2,
                            dbBytes, jBytes);
                }
            }
            // Other tests:
            // pass some extra ids to column lists.
            final int currentListSize = colIDList.size();
            Long rnd = rndm.nextLong();
            for (int i = 0; i < 4; i++) {
                if (!colIDList.contains(rnd))
                    colIDList.add(rnd);
                rnd = rndm.nextLong();
            }
            final Columns dbColumns2 = table.get(rowCursor.value, colIDList);
            assertNotNull("Colmns not found for row id: " + rowCursor.value,
                    dbColumns2);
            assertEquals("Number of columns mismatch for row id: "
                    + rowCursor.value, dbColumns2.size(), currentListSize);
            // pass empty list.
            final Columns dbColumns3 = table.get(rowCursor.value,
                    new LongArrayList());
            assertNull(
                    "Setting List to zero element list, should return null ",
                    dbColumns3);
        }
        // pass zero size list
        final Columns dbColumns5 = table.get(0, new LongArrayList());
        assertNull("Passing zero as rowId should return null", dbColumns5);
    }

    /** Testing BETable.get(long key, Range range) Test is performed by
     * retrieving all Test-rows-ids from test data file. Getting all the column
     * ids for each row in the test data, creating multiple Range of column ids
     * by adding one column id at a time to the Range and Calling the get(long
     * key, Range cols) method. Comparing result with the Test data. */
    @Test
    public void testGetLongRange() throws Exception {

        final LongArrayList rowIDs = J_UTIL.getRowIDs();

        for (final LongCursor rowCursor : rowIDs) {
            final LongObjectOpenHashMap<Bytes> jColumnsData = J_UTIL
                    .getColumnData(rowCursor.value);
            final Iterator<LongCursor> jColCursor = jColumnsData.keys()
                    .iterator();
            final LongArrayList colIDList = new LongArrayList();
            while (jColCursor.hasNext()) {
                final long colkey = jColCursor.next().value;
                colIDList.add(colkey);
                if (colIDList.size() == 1)
                    continue;

                // the following method (BETable.get(long row,Range cols))
                // is being tested
                final Range range = getMinMaxRangeFromIDList(colIDList);
                final Columns dbColumns = table.get(rowCursor.value, range);
                assertNotNull("No Columns found for row ID:" + rowCursor.value,
                        dbColumns);
                for (final LongHolder longHolder : dbColumns) {
                    final long colkey2 = longHolder.value();
                    assertTrue("Test column ID:" + colkey2
                            + " not found in DB row ID:" + rowCursor.value,
                            dbColumns.containsColumn(colkey2));
                    final Bytes dbBytes = dbColumns.getBytes(colkey2);
                    assertNotNull("Data not found in column id: " + colkey2
                            + " row ID:" + rowCursor.value, dbBytes);
                    final Bytes jBytes = jColumnsData.get(colkey2);
                    assertEquals("Column Values do not match for row ID:"
                            + rowCursor.value + " column ID:" + colkey2,
                            dbBytes, jBytes);
                }
            }
            // Other tests:
            // pass Range more than the column size.
            final Range rng = getMinMaxRangeFromIDList(colIDList);
            if (rng != null) {
                rng.end(rng.end() + 10);
                Columns dbColumns2 = table.get(rowCursor.value, rng);
                assertNotNull(
                        "No columns found for row id: " + rowCursor.value,
                        dbColumns2);
                assertEquals("Bigger range returns wrong number of columns ",
                        jColumnsData.size(), dbColumns2.size());
                rng.start(rng.start() - 10);
                dbColumns2 = table.get(rowCursor.value, rng);
                assertNotNull("Could not load column ", dbColumns2);
                assertEquals("Bigger range returns wrong number of columns",
                        jColumnsData.size(), dbColumns2.size());
            }

            // pass zero as Range.
            final Columns dbColumns3 = table.get(rowCursor.value, new Range(0,
                    0));
            assertNull("Setting Range to zero should return null", dbColumns3);

        }
        // pass empty range.
        final Columns dbColumns5 = table.get(0, new Range(0, 0));
        assertNull("Passing zero as rowId should return null", dbColumns5);
    }

    /** Test key iterator. */
    @Test
    public void testKeyIterator() throws Exception {
        final Iterator<LongHolder> keys = table.keys();
        final LongArrayList jrowIds = J_UTIL.getRowIDs();
        int count = 0;
        while (keys.hasNext()) {
            final LongHolder lhldr = keys.next();
            assertTrue("Row id mismatch ", jrowIds.contains(lhldr.value()));
            count++;
        }
        assertEquals("Number of rows mismatch ", jrowIds.size(), count);
    }

    /** Test BETable.remove(long row) Test data contains row ids to be removed to
     * remove the entire row. */
    @Test
    public void testRemoveLong() throws Exception { // $codepro.audit.disable
                                                    // missingAssertInTestMethod
        final LongArrayList removeRowIDs = J_UTIL.removeRows("removeRows");
        for (final LongCursor longCursor : removeRowIDs) {
            table.remove(longCursor.value);
        }
        validateAllTestRows();
    }

    /** Test BETable.remove(long row) Test data contains row ids and array of
     * columnIds to be remvoed for each row. entire row should not get removed
     * only the specified columns should get deleted. */
    @Test
    public void testRemoveLongLongArray() throws Exception {
        final LongObjectOpenHashMap<LongArrayList> removeMap = new LongObjectOpenHashMap<LongArrayList>();
        J_UTIL.columnsModified("removeColumns1", null, removeMap, null);
        if (removeMap.keys() == null)
            fail("Row data to be removed not present.");
        final Iterator<LongCursor> removedRows = removeMap.keys().iterator();
        while (removedRows.hasNext()) {
            final long key = removedRows.next().value;
            final LongArrayList removeIDs = removeMap.get(key);
            table.remove(key, removeIDs.toArray());
        }
        validateAllTestRows();
    }

    /** Test BETable.remove(long row) Test data contains row ids and List of
     * columnIds to be removed for each row. entire row should not get removed
     * only the specified columns should get deleted. */
    @Test
    public void testRemoveLongLongArrayList() throws Exception {
        final LongObjectOpenHashMap<LongArrayList> removeMap = new LongObjectOpenHashMap<LongArrayList>();
        J_UTIL.columnsAdded("removeColumns1", null, removeMap, null);
        if (removeMap.keys() == null)
            fail("New data to be inserted not present.");
        final Iterator<LongCursor> removedRows = removeMap.keys().iterator();

        while (removedRows.hasNext()) {
            final long key = removedRows.next().value;
            final LongArrayList removeIDs = removeMap.get(key);
            table.remove(key, removeIDs);
        }
        validateAllTestRows();

    }

    /** Test BETable.remove(long row) Test data contains row ids and Range of
     * columnIds to be removed for each row. entire row should not get removed
     * only the specified columns should get deleted. */
    @Test
    public void testRemoveLongRange() throws Exception {
        final LongObjectOpenHashMap<Range> removeRange = new LongObjectOpenHashMap<Range>();
        J_UTIL.columnsAdded("removeColumns2", null, null, removeRange);
        if (removeRange.keys() == null)
            fail("Row data to be removed not present.");
        final Iterator<LongCursor> modifiedRows = removeRange.keys().iterator();
        while (modifiedRows.hasNext()) {
            final long key = modifiedRows.next().value;
            final Range rng = removeRange.get(key);
            table.remove(key, rng);
        }
        validateAllTestRows();
    }

    /** Test reverse. */
    @Test
    public void testReverse() throws Exception {

        final Base36 tbl = Base36.get("TNonRev");
        final Base36 tbl2 = Base36.get("TRev");
        try {
            if (database.get(tbl) != null)
                database.drop(tbl);
            if (database.get(tbl2) != null)
                database.drop(tbl2);

            final BETableProfile profile = new BETableProfile();
            database.create(tbl, profile);
            final BETable tb12 = database.get(tbl);
            assertNotNull("Could not load table: " + tbl, tb12);
            assertFalse("Table was created in non reverse column Order",
                    tb12.reverse());

            // test this only if reverse order is supported.
            if (tb12.reverseSupported()) {
                final BETableProfile profile2 = new BETableProfile();
                profile2.reverseColumnsOrder(true);
                database.create(tbl2, profile2);
                final BETable tb22 = database.get(tbl2);
                assertNotNull("Could not load table: " + tbl2, tb22);
                assertTrue("Table was created in reverse column Order",
                        tb22.reverse());
            }

        } catch (final UnsupportedOperationException e) {
            // ignore.
        } finally {
            database.drop(tbl);
            database.drop(tbl2);
        }
    }

    /** Test reverse columns. */
    @Test
    public void testReverseColumns() throws Exception {

        final Base36 tbl2 = Base36.get("TRev");

        try {
            if (database.get(tbl2) != null)
                database.drop(tbl2);

            final BETableProfile profile2 = new BETableProfile();
            profile2.reverseColumnsOrder(true);
            database.create(tbl2, profile2);
            final BETable tb22 = database.get(tbl2);
            assertNotNull("Could not load table: " + tbl2, tb22);
            J_UTIL.loadTableFromJSONFile(tb22);
            if (tb22.reverseSupported()) {
                final LongArrayList rowIDs = J_UTIL.getRowIDs();
                for (final LongCursor rowCursor : rowIDs) {
                    final LongArrayList dbColumns = tb22
                            .columns(rowCursor.value);
                    final LongArrayList jColumns = J_UTIL
                            .getColumnIDs(rowCursor.value);
                    final long[] cols = sortDesending(jColumns.toArray());
                    int count = 0;
                    for (final long col : cols) {
                        assertEquals("Column ID mismatch for row ID:"
                                + rowCursor.value, col, dbColumns.get(count));
                        count++;
                    }
                }
            }
        } catch (final IllegalArgumentException exp) {
            // ignore this.
        } catch (final UnsupportedOperationException exp) {
            // ignore this.
        } finally {
            database.drop(tbl2);
        }
    }

    /** Test BETable.set(long row, Columns cols) by adding new columns to
     * existing rows. */
    @Test
    public void testSetLongColumns1() throws Exception {

        final LongObjectOpenHashMap<Columns> newColumns = new LongObjectOpenHashMap<Columns>();
        J_UTIL.columnsAdded("addColumns", newColumns, null, null);
        if (newColumns.keys() == null)
            fail("New data to be inserted not present.");
        final Iterator<LongCursor> modifiedRows = newColumns.keys().iterator();

        while (modifiedRows.hasNext()) {
            final long key = modifiedRows.next().value;
            table.set(key, newColumns.get(key));
        }
        validateAllTestRows();
    }

    /** Test BETable.set(long row, Columns cols) by modifying the values of
     * existing columns. */
    @Test
    public void testSetLongColumns2() throws Exception {

        final LongObjectOpenHashMap<Columns> newColumns = new LongObjectOpenHashMap<Columns>();
        J_UTIL.columnsModified("updateColumns", newColumns, null, null);
        if (newColumns.keys() == null)
            fail("New data to be inserted not present.");
        final Iterator<LongCursor> modifiedRows = newColumns.keys().iterator();

        while (modifiedRows.hasNext()) {
            final long key = modifiedRows.next().value;
            table.set(key, newColumns.get(key));
        }
        validateAllTestRows();
    }

    /** Test BETable.set(long row, Columns cols, long[] removeList) Test data
     * contains columns to be added to existing rows and Array of column ids to
     * be removed for multiple rows. */
    @Test
    public void testSetLongColumnsLongArray1() throws Exception {

        final LongObjectOpenHashMap<Columns> newColumns = new LongObjectOpenHashMap<Columns>();
        final LongObjectOpenHashMap<LongArrayList> removeList = new LongObjectOpenHashMap<LongArrayList>();
        J_UTIL.columnsAdded("addRemoveColumns", newColumns, removeList, null);
        if (newColumns.keys() == null)
            fail("New data to be inserted not present.");
        final Iterator<LongCursor> modifiedRows = newColumns.keys().iterator();
        while (modifiedRows.hasNext()) {
            final long key = modifiedRows.next().value;
            final LongArrayList removeIDs = removeList.get(key);
            table.set(key, newColumns.get(key), removeIDs.toArray());
        }
        validateAllTestRows();
    }

    /** Test BETable.set(long row, Columns column, long[] removeList) Test data
     * contains existing columns to be modified and array of column ids to be
     * removed from multiple Rows */
    @Test
    public void testSetLongColumnsLongArray2() throws Exception {
        final LongObjectOpenHashMap<Columns> newColumns = new LongObjectOpenHashMap<Columns>();
        final LongObjectOpenHashMap<LongArrayList> removeList = new LongObjectOpenHashMap<LongArrayList>();
        J_UTIL.columnsModified("updateRemoveColumns", newColumns, removeList,
                null);
        if (newColumns.keys() == null)
            fail("New data to be inserted not present.");
        final Iterator<LongCursor> modifiedRows = newColumns.keys().iterator();
        while (modifiedRows.hasNext()) {
            final long key = modifiedRows.next().value;
            final LongArrayList removeIDs = removeList.get(key);
            table.set(key, newColumns.get(key), removeIDs.toArray());
        }
        validateAllTestRows();
    }

    /** Test BETable.set(long row, Columns column,
     * LongObjectOpenHashMap<LongArrayList> removeList) Test data contains
     * columns to be added to existing rows and List of column ids to be removed
     * for multiple rows. */
    @Test
    public void testSetLongColumnsLongArrayList1() throws Exception {

        final LongObjectOpenHashMap<Columns> newColumns = new LongObjectOpenHashMap<Columns>();
        final LongObjectOpenHashMap<LongArrayList> removeList = new LongObjectOpenHashMap<LongArrayList>();
        J_UTIL.columnsAdded("addRemoveColumns", newColumns, removeList, null);
        if (newColumns.keys() == null)
            fail("New data to be inserted not present.");
        final Iterator<LongCursor> modifiedRows = newColumns.keys().iterator();

        while (modifiedRows.hasNext()) {
            final long key = modifiedRows.next().value;
            final LongArrayList removeIDs = removeList.get(key);
            table.set(key, newColumns.get(key), removeIDs);
        }
        validateAllTestRows();
    }

    /** Test BETable.set(long row, Columns cols,
     * LongObjectOpenHashMap<LongArrayList> removeList) Test data contains
     * existing column values to be modified and List of column ids to be
     * removed for multiple rows. */
    @Test
    public void testSetLongColumnsLongArrayList2() throws Exception {
        final LongObjectOpenHashMap<Columns> newColumns = new LongObjectOpenHashMap<Columns>();
        final LongObjectOpenHashMap<LongArrayList> removeList = new LongObjectOpenHashMap<LongArrayList>();
        J_UTIL.columnsModified("updateRemoveColumns", newColumns, removeList,
                null);
        if (newColumns.keys() == null)
            fail("New data to be inserted not present.");
        final Iterator<LongCursor> modifiedRows = newColumns.keys().iterator();
        while (modifiedRows.hasNext()) {
            final long key = modifiedRows.next().value;
            final LongArrayList removeIDs = removeList.get(key);
            table.set(key, newColumns.get(key), removeIDs);
        }
        validateAllTestRows();
    }

    /** Test BETable.set(long row, Columns cols, Range remove) Test data contains
     * columns to be added to existing rows and Range of column ids to be
     * removed for multiple rows. */
    @Test
    public void testSetLongColumnsRange1() throws Exception {
        final LongObjectOpenHashMap<Columns> newColumns = new LongObjectOpenHashMap<Columns>();
        final LongObjectOpenHashMap<Range> removeRange = new LongObjectOpenHashMap<Range>();
        J_UTIL.columnsAdded("addRemoveColumns1", newColumns, null, removeRange);
        if (newColumns.keys() == null)
            fail("New data to be inserted not present.");
        final Iterator<LongCursor> modifiedRows = newColumns.keys().iterator();
        while (modifiedRows.hasNext()) {
            final long key = modifiedRows.next().value;
            table.set(key, newColumns.get(key), removeRange.get(key));
        }
        validateAllTestRows();
    }

    /** Test BETable.set(long row, Columns cols, Range remove) Test data contains
     * exiting columns modified and Range of column ids to be removed for
     * multiple rows. */
    @Test
    public void testSetLongColumnsRange2() throws Exception {
        final LongObjectOpenHashMap<Columns> newColumns = new LongObjectOpenHashMap<Columns>();
        final LongObjectOpenHashMap<Range> removeRange = new LongObjectOpenHashMap<Range>();
        J_UTIL.columnsModified("updateRemoveColumns1", newColumns, null,
                removeRange);
        if (newColumns.keys() == null)
            fail("New data to be inserted not present.");
        final Iterator<LongCursor> modifiedRows = newColumns.keys().iterator();
        while (modifiedRows.hasNext()) {
            final long key = modifiedRows.next().value;
            table.set(key, newColumns.get(key), removeRange.get(key));
        }
        validateAllTestRows();
    }

    /** Test size. */
    @Test
    public void testSize() throws Exception {
        assertEquals("Row size mismatch ", J_UTIL.getRowIDs().size(),
                table.size());
    }

}
// CHECKSTYLE resume magic number check
