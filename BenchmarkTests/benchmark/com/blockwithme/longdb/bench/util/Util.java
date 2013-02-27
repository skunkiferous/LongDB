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
package com.blockwithme.longdb.bench.util;

import static com.blockwithme.longdb.bench.constants.BenchmarkTestingConstants.DB_NAME;
import static com.blockwithme.longdb.bench.constants.BenchmarkTestingConstants.TEST_TABLE_NAME;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.log4j.PropertyConfigurator;

import com.blockwithme.longdb.BEDatabase;
import com.blockwithme.longdb.BETableProfile;
import com.blockwithme.longdb.Backend;
import com.blockwithme.longdb.discovery.BackendServiceLoader;
import com.blockwithme.longdb.entities.Base36;
import com.blockwithme.longdb.entities.Bytes;
import com.carrotsearch.hppc.LongObjectOpenHashMap;

/** Class provides common static methods, used for performance benchmarks
 * testing. */
public final class Util {

    /** The folder name. */
    private static String FOLDER_NAME;

    /** The Constant RND. */
    private static final Random RND = new Random();

    /** Creates the test data base.
     * 
     * @param theBackendType
     *        the backendType type
     * @param theInstanceName
     *        the instance name */
    public static void createTestDataBase(final String theBackendType,
            final String theInstanceName) {
        final Backend backend = BackendServiceLoader.getInstance()
                .loadInstance(theBackendType, theInstanceName);
        final BEDatabase database = backend.createDatabase(DB_NAME);
        final Base36 tbl = Base36.get(TEST_TABLE_NAME);
        final BETableProfile profile = new BETableProfile();
        database.create(tbl, profile);
        database.close(); // $codepro.audit.disable closeInFinally
    }

    /** Creates Byte array of length 'blobsize' with random data.
     * 
     * @param theBlobsize
     *        the blobsize
     * @return the long object open hash map */
    public static LongObjectOpenHashMap<Bytes> generateRandomByteArray(
            final int theBlobsize) {
        final byte[] bytesBuffer = new byte[theBlobsize];
        RND.nextBytes(bytesBuffer);
        final long colId = RND.nextLong();
        final LongObjectOpenHashMap<Bytes> col = new LongObjectOpenHashMap<Bytes>();
        col.put(colId, new Bytes(bytesBuffer));
        return col;
    }

    /** Random number between.
     * 
     * @param theMaximum
     *        the maximum
     * @param theMinimum
     *        the minimum
     * @return the long */
    public static long randomNumberBetween(final long theMaximum,
            final long theMinimum) {
        final long n = theMaximum - theMinimum + 1;
        final long i = RND.nextLong() % n;
        return theMinimum + i;
    }

    /** Results folder name.
     * 
     * @return the string
     * @throws IOException
     *         Signals that an I/O exception has occurred. */
    public static String resultsFolderName() throws IOException {
        if (FOLDER_NAME == null) {
            // CHECKSTYLE IGNORE FOR NEXT 2 LINES
            final SimpleDateFormat datefrmt = new SimpleDateFormat( // NOPMD
                    "MMM_dd_yyyy_HH_mm_ss");
            FOLDER_NAME = "benchmark_results/" + datefrmt.format(new Date());
        }
        return FOLDER_NAME;
    }

    /** Sets the configuration.
     * 
     * @throws IOException
     *         Signals that an I/O exception has occurred. */
    public static void setConfiguration() throws IOException {
        System.setProperty("backend.impl",
                "com.blockwithme.longdb.backend.impl.cassandra.CassandraBackend");
        final Properties props = new Properties();
        props.load(Util.class.getResourceAsStream("/log4j-server.properties"));
        PropertyConfigurator.configure(props);
    }

    /** Hide utility class constructor. */
    private Util() {
    }

}
