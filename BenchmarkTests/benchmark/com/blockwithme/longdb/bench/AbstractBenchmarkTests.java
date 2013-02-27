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
// $codepro.audit.disable debuggingCode, noAbstractMethods, assignmentToNonFinalStatic, useBufferedIO, missingAssertInTestMethod, disallowSleepUsage, pluralizeCollectionNames
//CHECKSTYLE stop magic number check
package com.blockwithme.longdb.bench;

import static com.blockwithme.longdb.bench.constants.BenchmarkTestingConstants.DB_NAME;
import static com.blockwithme.longdb.bench.constants.BenchmarkTestingConstants.INTERNAL_ITERATIONS;
import static com.blockwithme.longdb.bench.constants.BenchmarkTestingConstants.TEST_TABLE_NAME;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.blockwithme.longdb.BEDatabase;
import com.blockwithme.longdb.BETable;
import com.blockwithme.longdb.BETableProfile;
import com.blockwithme.longdb.Backend;
import com.blockwithme.longdb.Columns;
import com.blockwithme.longdb.bench.constants.BenchmarkTestingConstants;
import com.blockwithme.longdb.bench.util.ConfigBean;
import com.blockwithme.longdb.bench.util.OrderedRunner;
import com.blockwithme.longdb.bench.util.Util;
import com.blockwithme.longdb.entities.Base36;
import com.blockwithme.longdb.entities.Bytes;
import com.blockwithme.longdb.entities.LongHolder;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;

/** This abstract class provides Test methods for performance benchmark test
 * cases. This is extended by other classes to provide the concrete Backend
 * implementation. */
@RunWith(OrderedRunner.class)
public abstract class AbstractBenchmarkTests extends AbstractBenchmark {

    /** The backend handle */
    private static Backend BACKEND;
    /** The BLOBS LIST 100K. */
    private static final List<LongObjectOpenHashMap<Bytes>> BLOBSARRAY100K = new ArrayList<LongObjectOpenHashMap<Bytes>>();
    /** The BLOBS LIST 10K. */
    private static final List<LongObjectOpenHashMap<Bytes>> BLOBSARRAY10K = new ArrayList<LongObjectOpenHashMap<Bytes>>();
    /** The Constant BLOBS LIST 1K. */
    private static final List<LongObjectOpenHashMap<Bytes>> BLOBSARRAY1K = new ArrayList<LongObjectOpenHashMap<Bytes>>();
    /** The database handle */
    private static BEDatabase DATABASE;
    /** The execution counter. */
    private static long EXECUTIONCOUNTER;
    /** The init done flag. */
    private static boolean INIT_DONE;
    /** The pre-populated RANDOM ROW IDS. */
    private static final long[] RANDOM_ROW_IDS = new long[BenchmarkTestingConstants.ROW_ID_COUNT];
    /** The row counter */
    private static long ROW_COUNTER;

    /** The table. */
    private static BETable TABLE;

    /** Initialization */
    private static void initBlobs() {
        // generate random rowIds and keep them in array.
        for (int i = 0; i < BenchmarkTestingConstants.TEST_BLOB_COUNT; i++) {
            BLOBSARRAY1K
                    .add(Util
                            .generateRandomByteArray(BenchmarkTestingConstants.TEST_BLOB_SIZE1));
            BLOBSARRAY10K
                    .add(Util
                            .generateRandomByteArray(BenchmarkTestingConstants.TEST_BLOB_SIZE2));
            BLOBSARRAY100K
                    .add(Util
                            .generateRandomByteArray(BenchmarkTestingConstants.TEST_BLOB_SIZE3));
        }
        // generate random rowIds and keep them in array.
        for (int i = 0; i < BenchmarkTestingConstants.ROW_ID_COUNT; i++) {
            RANDOM_ROW_IDS[i] = Util.randomNumberBetween(
                    BenchmarkTestingConstants.MIN_ROW_ID,
                    BenchmarkTestingConstants.MIN_ROW_ID);
        }
        INIT_DONE = true;
    }

    /** Configurations to be injected in sub classes. */
    public static void doInit(final ConfigBean theBean) throws Exception {

        final Properties props = new Properties();
        FileInputStream fis = null;
        final Base36 tbl = Base36.get(TEST_TABLE_NAME);
        try {
            fis = new FileInputStream(theBean.logConfig());
            props.load(fis);
            PropertyConfigurator.configure(props);

            System.setProperty("jub.consumers", theBean.jubConsumers());
            System.setProperty("jub.db.file", theBean.jubDbFile());
            System.setProperty("jub.charts.dir", Util.resultsFolderName());

            BACKEND = theBean.backend();

            DATABASE = BACKEND.openDatabase(DB_NAME);
            if (DATABASE == null)
                DATABASE = BACKEND.createDatabase(DB_NAME);

            final BETableProfile profile = new BETableProfile();
            if (DATABASE.get(tbl) != null)
                DATABASE.drop(tbl);
            DATABASE.create(tbl, profile);
            TABLE = DATABASE.get(tbl);

        } catch (final UnsupportedOperationException e) {
            TABLE = DATABASE.get(tbl);
            if (TABLE != null) {
                final Iterator<LongHolder> itr = TABLE.keys();
                while (itr.hasNext()) {
                    TABLE.remove(itr.next().value());
                }
            }
        } finally {
            if (fis != null) {
                fis.close();
            }
        }
        if (!INIT_DONE)
            initBlobs();
    }

    /** Set Up before class **/
    @BeforeClass
    public static void setUpBeforeClass() {
        // NOP
    }

    /** Tear down after class.
     * 
     * @throws Exception */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        System.out.println("Shutting Down Database.");
        try {
            DATABASE.drop(TABLE.table());
        } catch (final UnsupportedOperationException e) {
            // ignore
        } finally {
            DATABASE.close();
        }
        BACKEND.shutdown();
        // wait before server shuts down.
        try {
            Thread.sleep(10000);
        } catch (final InterruptedException e) {
            // ignore
        }
    }

    /** Actually inserts column values.
     * 
     * @param theColumns the columns */
    private void insert(final LongObjectOpenHashMap<Bytes> theColumns) {
        final long key1 = ROW_COUNTER;
        TABLE.set(key1, new Columns(theColumns, false));
        ROW_COUNTER++;
    }

    /** actually performs insert and update operation.
     * 
     * @param theColumns the columns */
    private void insertUpdate(final LongObjectOpenHashMap<Bytes> theColumns) {
        final long key = RANDOM_ROW_IDS[(int) ROW_COUNTER
                % BenchmarkTestingConstants.ROW_ID_COUNT];
        TABLE.get(key);
        insert(theColumns);
    }

    /** Set up */
    @Before
    public void setUp() {
        // NOP
    }

    /** Tear down */
    @After
    public void tearDown() {
        // NOP
    }

    /** Test method for write operations. 1K blob size */
    @BenchmarkOptions(benchmarkRounds = 100, warmupRounds = 5)
    @Test
    public void testAWrites1K() {

        for (int i = 0; i < INTERNAL_ITERATIONS; i++) {
            insert(BLOBSARRAY1K.get((int) ROW_COUNTER
                    % BenchmarkTestingConstants.TEST_BLOB_COUNT));
        }
    }

    /** Test method for write operations. 10K blob size */
    @BenchmarkOptions(benchmarkRounds = 100, warmupRounds = 5)
    @Test
    public void testBWrites10K() {
        for (int i = 0; i < INTERNAL_ITERATIONS; i++) {
            insert(BLOBSARRAY10K.get((int) ROW_COUNTER
                    % BenchmarkTestingConstants.TEST_BLOB_COUNT));
        }
    }

    /** Test method for write operations. 100K blob size */
    @BenchmarkOptions(benchmarkRounds = 100, warmupRounds = 5)
    @Test
    public void testCWrites100K() {

        for (int i = 0; i < INTERNAL_ITERATIONS; i++) {
            insert(BLOBSARRAY100K.get((int) ROW_COUNTER
                    % BenchmarkTestingConstants.TEST_BLOB_COUNT));
        }
    }

    /** Test method for read operations, performs reads using using row ids
     * present in RANDOM_ROW_IDS */
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
    @BenchmarkOptions(benchmarkRounds = 100, warmupRounds = 5)
    @Test
    public void testDRead() {
        for (int i = 0; i < INTERNAL_ITERATIONS; i++) {

            final long key = RANDOM_ROW_IDS[(int) EXECUTIONCOUNTER
                    % BenchmarkTestingConstants.ROW_ID_COUNT];
            TABLE.get(key);
            EXECUTIONCOUNTER++;
        }
    }

    /** Test method for read-write operation with 1k blob size */
    @Test
    @BenchmarkOptions(benchmarkRounds = 100, warmupRounds = 5)
    public void testEReadWrite1K() {
        for (int i = 0; i < INTERNAL_ITERATIONS; i++) {
            insertUpdate(BLOBSARRAY1K.get((int) ROW_COUNTER
                    % BenchmarkTestingConstants.TEST_BLOB_COUNT));
        }
    }

    /** Test method for read-write operation with 10k blob size */
    @BenchmarkOptions(benchmarkRounds = 100, warmupRounds = 5)
    @Test
    public void testFReadWrite10K() {
        for (int i = 0; i < INTERNAL_ITERATIONS; i++) {
            insertUpdate(BLOBSARRAY10K.get((int) ROW_COUNTER
                    % BenchmarkTestingConstants.TEST_BLOB_COUNT));
        }
    }

    /** Test method for read-write operation with 100k blob size */
    @BenchmarkOptions(benchmarkRounds = 100, warmupRounds = 5)
    @Test
    public void testGReadWrite100K() {
        for (int i = 0; i < INTERNAL_ITERATIONS; i++) {
            insertUpdate(BLOBSARRAY100K.get((int) ROW_COUNTER
                    % BenchmarkTestingConstants.TEST_BLOB_COUNT));
        }
    }
}
// CHECKSTYLE resume magic number check
