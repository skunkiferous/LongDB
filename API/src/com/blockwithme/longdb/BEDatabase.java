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

import com.blockwithme.longdb.entities.Base36;

/** Represents a backend database implementation. This is a thin layer over some
 * other real database. This thin layer allows the client and the server to use
 * different database engines, while keeping the same API. WARNING: Databases
 * are NOT thread-safe! TODO: Add a method to return the BETableProfile of an
 * existing table. This probably requires a separate table per database for that
 * info. TODO: Add a method saying if the database supports dynamic
 * creation/destruction of tables. TODO: Add an optional method to rename
 * tables. Optional because it might not be supported by all API. Also, it is
 * probably not supported while the table is open. TODO: Add a method saying if
 * the database supports rename of tables. */
@ParametersAreNonnullByDefault
public interface BEDatabase {

    /** Returns the Backend of this database.
     * 
     * @return the backend object */
    Backend backend();

    /** Closes the database and all associated tables. No DB operation on this
     * database is possible after this call. */
    void close();

    /** Are we closed?.
     * 
     * @return true, if closed */
    boolean closed();

    /** Creates a new table in the database, using the *optional* table profile.
     * Not available on the server.
     * 
     * @param theTable
     *        the table name
     * @param theProfile
     *        the table profile
     * @return the newly created table instance */
    BETable create(final Base36 theTable, final BETableProfile theProfile);

    /** Returns the database name.
     * 
     * @return the database name */
    String database();

    /** Deletes a table in the database, if it exists. drop() is defined here,
     * instead of in BETable, so that a table can be dropped without opening it
     * first. Not available on the server. Returns true on success.
     * 
     * @param theTable
     *        the table name
     * @return true, if successful */
    boolean drop(final Base36 theTable);

    /** Returns a table from the database. If no table with this name exists, it
     * will return null.
     * 
     * @param theTable
     *        the table name
     * @return the backend table */
    @CheckForNull
    BETable get(final Base36 theTable);

    /** Lists the known tables in the database.
     * 
     * @return the collection of table names */
    Collection<Base36> tables();

}
