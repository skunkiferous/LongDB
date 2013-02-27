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
package com.blockwithme.longdb.base;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.ParametersAreNonnullByDefault;

import com.blockwithme.longdb.Backend;
import com.blockwithme.longdb.exception.DBException;
import com.google.common.base.Preconditions;

/** Abstract Backend implementation.
 * 
 * @param <B>
 *        the type of Backend
 * @param <D>
 *        the type of Database
 * @param <T>
 *        the type of Table */
@ParametersAreNonnullByDefault
public abstract class AbstractBackend<B extends AbstractBackend<B, D, T>, D extends AbstractDatabase<B, D, T>, T extends AbstractTable<B, D, T>>
        implements Backend {

    /** Are we closed? */
    private boolean closed;

    /** The databases */
    private final Map<String, D> databases = new HashMap<String, D>();

    /** Are we opened? */
    private boolean open;

    /** Check that we are opened. */
    private void checkNotClosed() {
        if (closed) {
            throw new DBException("Closed!");
        }
    }

    /** Opens the backend, if it is not opened yet. */
    private void open() {
        checkNotClosed();
        if (!open) {
            open = true;
            openInternal(databases);
        }
    }

    /** Really create the database.
     * 
     * @param theDB
     *        the database name
     * @return the Database instance */
    protected abstract D createDatabaseInternal(final String theDB);

    /** Actually drops the database and performs cleanup.
     * 
     * @param theDB
     *        the database to be dropped */
    protected abstract void dropInternal(final D theDB);

    /** Actually opens the Backend, and scan for existing databases.
     * 
     * @param theDatabases
     *        the databases map */
    protected abstract void openInternal(final Map<String, D> theDatabases);

    /** Really shutdown the database. */
    protected abstract void shutdownInternal();

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.Backend#closed()
     */
    @Override
    public final boolean closed() {
        return closed;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.Backend#createDatabase(java.lang.String)
     */
    @Override
    public final D createDatabase(String theDB) {
        Preconditions.checkNotNull(theDB, "database is null");
        theDB = theDB.toLowerCase();
        D result = openDatabase(theDB);
        if (result == null) {
            result = createDatabaseInternal(theDB);
            databases.put(theDB.toLowerCase(), result);
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.Backend#databases()
     */
    @Override
    public Collection<String> databases() {
        open();
        Preconditions.checkNotNull(databases, "databases is null");
        return Collections.unmodifiableCollection(databases.keySet());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.Backend#dropDatabase(java.lang.String)
     */
    @Override
    public boolean dropDatabase(String theDB) {
        Preconditions.checkNotNull(theDB, "database is null");
        theDB = theDB.toLowerCase();
        open();
        final D dropped = databases.remove(theDB);
        if (dropped != null) {
            if (!dropped.closed()) {
                dropped.close(); // $codepro.audit.disable closeInFinally
            }
            dropInternal(dropped);
            return true;
        }
        return false;

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.Backend#openDatabase(java.lang.String)
     */
    @Override
    @CheckForNull
    public final D openDatabase(final String theDB) {
        Preconditions.checkNotNull(theDB, "database is null");
        open();
        final D result = databases.get(theDB.toLowerCase());
        if (result == null) {
            return null;
        }
        result.open();
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.Backend#shutdown()
     */
    @Override
    public final void shutdown() {
        if (!closed) {
            closed = true;
            if (open) {
                open = false;
                for (final D db : databases.values()) {
                    db.close(); // $codepro.audit.disable closeInFinally
                }
            }
        }
        shutdownInternal();
    }

}
