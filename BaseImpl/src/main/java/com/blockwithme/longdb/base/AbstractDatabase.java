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

import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blockwithme.longdb.BEDatabase;
import com.blockwithme.longdb.BETableProfile;
import com.blockwithme.longdb.entities.Base36;
import com.google.common.base.Preconditions;

// TODO: Auto-generated Javadoc
/** Implementation of a memory-backed BEDatabase. Useful for testing.
 * 
 * @param <B> the type of Backend
 * @param <D> the type of Database
 * @param <T> the type of Table */
@ParametersAreNonnullByDefault
public abstract class AbstractDatabase<B extends AbstractBackend<B, D, T>, D extends AbstractDatabase<B, D, T>, T extends AbstractTable<B, D, T>>
        implements BEDatabase {

    /** The Logger. */
    private static final Logger LOG = LoggerFactory
            .getLogger(AbstractDatabase.class);

    /** The backend. */
    protected final B backend;

    /** Are we closed?. */
    protected boolean closed;

    /** Our database name. */
    protected final String database;

    /** Are we opened?. */
    protected boolean open;

    /** The tables. */
    protected final Map<Base36, T> tables = new HashMap<Base36, T>();

    /** The table ids. */
    protected final Collection<Base36> tablesIDs;

    /** Protected Constructor.
     * 
     * @param theBackend the backend instance
     * @param theDatabase the database name */
    protected AbstractDatabase(final B theBackend, final String theDatabase) {
        Preconditions.checkNotNull(theBackend, "backend is null");
        Preconditions.checkNotNull(theDatabase, "database is null");
        Preconditions.checkNotNull(tables, "tables is null");
        this.backend = theBackend;
        this.database = theDatabase;
        tablesIDs = Collections.unmodifiableCollection(tables.keySet());
    }

    /** Check that we are opened. */
    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("Closed!");
        }
    }

    /** The Specific implementation of {@link AbstractDatabase#close()} */
    protected abstract void closeInternal();

    /** Actually creates a table. The specific implementation of
     * {@link AbstractDatabase#create(Base36, BETableProfile)}
     * 
     * @param theTable the table name
     * @param theProfile the table profile
     * @return the table instance */
    protected abstract T createInternal(final Base36 theTable,
            final BETableProfile theProfile);

    /** Actually drop a table. Specific implementation of
     * {@link AbstractDatabase#drop(Base36)}
     * 
     * @param theTable the table to be dropped */
    protected abstract void dropInternal(final T theTable);

    /** Actually opens the database, and scans for existing tables. Specific
     * implementation of {@link AbstractDatabase#open()}
     * 
     * @param theTables the map of tables */
    protected abstract void openInternal(final Map<Base36, T> theTables);

    /* (non-Javadoc)
     * @see com.blockwithme.longdb.BEDatabase#backend() */
    @Override
    public final B backend() {
        checkNotClosed();
        return backend;
    }

    /* (non-Javadoc)
     * @see com.blockwithme.longdb.BEDatabase#close() */
    @Override
    public final void close() {
        if (!closed) {
            closed = true;
            if (open) {
                open = false;
                for (final T table : tables.values()) {
                    final String name = table.toString();
                    try {
                        table.close(); // $codepro.audit.disable closeInFinally
                    } catch (final Throwable t) {
                        LOG.error("Error closing: " + name, t);
                    }
                }
                closeInternal();
            }
        }
    }

    /* (non-Javadoc)
     * @see com.blockwithme.longdb.BEDatabase#closed() */
    @Override
    public final boolean closed() {
        return closed;
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.BEDatabase#create(com.blockwithme.longdb.entities
     * .Base36, com.blockwithme.longdb.BETableProfile) */
    @Override
    public final T create(final Base36 theTable, final BETableProfile theProfile) {
        open();
        // Optional profile is ignored.
        T result = tables.get(theTable);
        if (result != null) {
            throw new IllegalArgumentException("BETable " + theTable
                    + " already exists!");
        }
        result = createInternal(theTable, theProfile);
        tables.put(theTable, result);
        return result;
    }

    /* (non-Javadoc)
     * @see com.blockwithme.longdb.BEDatabase#database() */
    @Override
    public final String database() {
        // checkNotClosed();
        return database;
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.BEDatabase#drop(com.blockwithme.longdb.entities
     * .Base36) */
    @Override
    public final boolean drop(final Base36 theTable) {
        open();
        final T dropped = tables.get(theTable);
        if (dropped != null) {
            dropInternal(dropped);
            if (dropped.opened) {
                dropped.close(); // $codepro.audit.disable closeInFinally
            }
            tables.remove(theTable);
            return true;
        }
        return false;
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.BEDatabase#get(com.blockwithme.longdb.entities
     * .Base36) */
    @Override
    public final T get(final Base36 theTable) {
        open();
        final T result = tables.get(theTable);
        if (result != null)
            result.open();
        return result;
    }

    /** Opens the database, if it wasn't opened already. */
    public final void open() {
        checkNotClosed();
        if (!open) {
            open = true;
            openInternal(tables);
        }
    }

    /* (non-Javadoc)
     * @see com.blockwithme.longdb.BEDatabase#tables() */
    @Override
    public final Collection<Base36> tables() {
        open();
        return tablesIDs;
    }
}
