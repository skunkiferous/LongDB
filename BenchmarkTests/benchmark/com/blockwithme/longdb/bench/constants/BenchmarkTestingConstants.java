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
package com.blockwithme.longdb.bench.constants;

/** The Interface BenchmarkTestingConstants. */
public interface BenchmarkTestingConstants {

    /** database name used for all the operations */
    String DB_NAME = "default";

    /** number of iteration to be performed per test operation */
    int INTERNAL_ITERATIONS = 50;

    /** Read operation uses a random row id between MIN_ROW_ID and MAX_ROW_ID */
    long MAX_ROW_ID = 10000;

    /** Read operation uses a random row id between MIN_ROW_ID and MAX_ROW_ID */
    long MIN_ROW_ID = 0;

    /** The project home. */
    String PROJECT_NAME = "BenchmarkTests";

    /** number of pre generated row ids */
    int ROW_ID_COUNT = 500;

    /** number of random byte arrays (blobs) to be created */
    int TEST_BLOB_COUNT = 50;

    /** blob size 1024 */
    int TEST_BLOB_SIZE1 = 1024;

    /** blob size 1024*10 */
    int TEST_BLOB_SIZE2 = 10240;

    /** blob size 1024*100 */
    int TEST_BLOB_SIZE3 = 102400;

    /** table name used for all the operations */
    String TEST_TABLE_NAME = "DefaultTable";

}
