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
package com.blockwithme.longdb;

import com.blockwithme.longdb.BEDatabase;
import com.blockwithme.longdb.Backend;
import com.blockwithme.longdb.discovery.BackendServiceLoader;

/** Creates or opened a database. */
public class Factory {
    /** The backend. */
    private final Backend backend;

    /** Creates a Factory.
     * 
     * @param theBackendType
     *        the backend type
     * @param theInstanceName
     *        the instance name */
    public Factory(final String theBackendType, final String theInstanceName) {
        backend = BackendServiceLoader.getInstance().loadInstance(
                theBackendType, theInstanceName);
    }

    /** Creates or opened a database.
     * 
     * @param theDB
     *        the database
     * @return the database */
    public Database open(final String theDB) {
        BEDatabase db = backend.openDatabase(theDB);
        if (db == null) {
            db = backend.createDatabase(theDB);
        }
        return new Database(db);
    }
}
