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
package com.blockwithme.longdb.test;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.blockwithme.longdb.test.util.Util;

/** Combines test cases for client-server database implementations only, includes
 * : <br>
 * 1. Cassandra Hector Implementation.<br>
 * 2. VoltDb JUnit.<br> */
// CHECKSTYLE IGNORE FOR NEXT 1 LINES
@RunWith(Suite.class)
@SuiteClasses({ HectorJUnit.class, VoltDBJUnit.class })
public class ClientServerDBTestSuite {

    /** Clean up. */
    @AfterClass
    public static void cleanUp() {
        // NOP
    }

    /** Sets the up class. */
    @BeforeClass
    public static void setUpClass() {
        Util.setLogConfig();
    }
}
