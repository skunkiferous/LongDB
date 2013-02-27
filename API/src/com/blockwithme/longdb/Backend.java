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

import java.util.Collection;

import javax.annotation.CheckForNull;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.NotThreadSafe;

/** Represents a backend implementation. A backend can support multiple databases
 * concurrently, but resource restrictions apply. WARNING: Backends are NOT
 * thread-safe! TODO: Add a method saying if the backend supports dynamic
 * creation/destruction of databases. */
@NotThreadSafe
@ParametersAreNonnullByDefault
public interface Backend {

    /** Are we closed?.
     * 
     * @return true, if closed */
    boolean closed();

    /** Creates a new database. Not available on the server.
     * 
     * @param theDatabase
     *        the database name
     * @return the newly created database instance */
    BEDatabase createDatabase(String theDatabase);

    /** Lists the known databases.
     * 
     * @return the collection of databases */
    Collection<String> databases();

    /** Drops the Database and all the tables.
     * 
     * @param theDatabase
     *        the database to be dropped.
     * @return true, if successful */
    boolean dropDatabase(String theDatabase);

    /** Opens an existing database. Returns null if unknown.
     * 
     * @param theDatabase
     *        the name of the database to be opened.
     * @return the database instance */
    @CheckForNull
    BEDatabase openDatabase(String theDatabase);

    /** Shuts down the backend. All opened databases are closed first. */
    void shutdown();

}
