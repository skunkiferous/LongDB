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
// $codepro.audit.disable handleNumericParsingErrors
package com.blockwithme.longdb.client.internal;

import java.util.Collection;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blockwithme.longdb.BEDatabase;
import com.blockwithme.longdb.BETable;
import com.blockwithme.longdb.BETableProfile;
import com.blockwithme.longdb.Backend;
import com.blockwithme.longdb.client.entities.RowFilter;
import com.blockwithme.longdb.client.io.DataInput;
import com.blockwithme.longdb.client.io.DataOutput;
import com.blockwithme.longdb.client.io.db.TableDataInput;
import com.blockwithme.longdb.client.io.db.TableDataOutput;
import com.blockwithme.longdb.discovery.BackendServiceLoader;
import com.blockwithme.longdb.entities.Base36;
import com.carrotsearch.hppc.LongArrayList;

/* (non-Javadoc) TODO: Try to work-out specific use cases, to decide if this is
 * good enough, or if we need some kind of connection pooling for more
 * efficiency. - Tarun: - I think if we need any connection pooling I guess
 * XXXBackendImp classes are better place to implement the same. This class is
 * designed as Stateless class in order to perform a db operation and close the
 * connection*/

/** Class holds Configuration Parameters for each Backend Implementation, and
 * provides methods to actually perform a set of DB operations This is a
 * Stateless object, Doesn't hold db connections etc. All the DB operations
 * methods will create a new connection and perform the operations and close the
 * connection(s). */
@ParametersAreNonnullByDefault
public class BackendInstanceWrapper {
    /** The Logger. */
    private static final Logger LOG = LoggerFactory
            .getLogger(BackendInstanceWrapper.class);

    /**  */
    private static final Base36[] ZERO_BASE36S = new Base36[0];

    /**  */
    private static final String[] ZERO_STRINGS = new String[0];

    /** name of database to be selected. */
    // database is an optional field, hence non final.
    private String database;

    /** description of backend instance */
    private final String description;

    /** name of backend instance */
    private final String instanceName;

    /** name of backend */
    private final String name;

    /** Utility method closes the backend connection quitely */
    private static void close(final Backend theBackend) {
        try {
            if (theBackend != null) {
                theBackend.shutdown();
            }
        } catch (final Exception e) {
            LOG.error("Error closing backend: " + theBackend, e);
        }
    }

    /** Utility method closes the database quitely */
    private static void close(final BEDatabase theDB) {
        try {
            if (theDB != null) {
                theDB.close(); // $codepro.audit.disable closeInFinally
            }
        } catch (final Exception e) {
            LOG.error("Error closing DB: " + theDB, e);
        }
    }

    /** Instantiates a new backend configuration holder.
     * 
     * @param theName
     *        backend database instance name.
     * @param theDescription
     *        backend database instance description
     * @param theInstanceName
     *        the instance name
     * @param theDatabase
     *        the database name (optional) pass null if new database needs
     *        to be created after the instantiation of this class. */
    public BackendInstanceWrapper(final String theName,
            final String theDescription, final String theInstanceName,
            @Nullable final String theDatabase) {
        this.name = theName;
        this.description = theDescription;
        this.instanceName = theInstanceName;
        this.database = theDatabase;
    }

    /** Utility method Check tables exist. */
    private void checkTablesExist(final boolean theMergeTable,
            final BEDatabase theDB1, final BEDatabase theDB2) {
        if (!theMergeTable) {
            for (final Base36 table : theDB1.tables()) {
                final BETable toTable = theDB2.get(table);
                if (toTable != null)
                    throw new IllegalArgumentException(
                            "Target table already exists, use 'merge' switch.");
            }
        }
    }

    /** Initiates backend connection and returns an instance. */
    private Backend connect() throws Exception {
        return BackendServiceLoader.getInstance().loadInstance(name,
                instanceName);
    }

    /** Creates the table */
    private BETable create(final BEDatabase theDB2, final Base36 theTable,
            final BETable theFromtable) {
        final BETableProfile profile = new BETableProfile();
        profile.reverseColumnsOrder(theFromtable.reverse());
        theDB2.create(theTable, profile);
        return theDB2.get(theTable);
    }

    /** Opens the db. */
    private BEDatabase openDB(final String theDBName, final Backend theBackend) {
        final BEDatabase db = theBackend.openDatabase(theDBName);
        if (db == null)
            throw new NullPointerException("Database " + theDBName
                    + " doesn't exist.");
        return db;
    }

    /** Row filter. */
    private RowFilter rowFilter(final String theRowFilter) {
        RowFilter filter = null;
        if (theRowFilter != null) {
            final LongArrayList inList = new LongArrayList();
            inList.add(Long.valueOf(theRowFilter));
            filter = new RowFilter(inList);
        }
        return filter;
    }

    /** Get Table. by name */
    private BETable table(final BEDatabase theDatabase,
            final String theTableName, final boolean theShouldExist) {
        if (theDatabase == null)
            throw new NullPointerException("Database doesn't exist");
        final BETable table = theDatabase.get(Base36.get(theTableName));
        if (table == null && theShouldExist)
            throw new IllegalArgumentException("Table " + theTableName
                    + " Not found");
        return table;
    }

    /** Transfers full table data */
    private void transfer(final BETable theSourceTable,
            final BETable theTargetTable) {
        final TableDataInput in = new TableDataInput(theSourceTable);
        final TableDataOutput out = new TableDataOutput(theTargetTable);
        out.dump(in);
    }

    /** Connects to the Backend database and fetches column counts for a
     * particular row in a particular table.
     * 
     * @param theTableName
     *        the table name
     * @param theRowId
     *        the row id
     * @return column count */
    public long columnCount(final String theTableName, final long theRowId)
            throws Exception {
        Backend be = null;
        BEDatabase dbInstance = null;
        try {
            be = connect();
            dbInstance = openDB(this.database, be);
            final BETable table = table(dbInstance, theTableName, true);
            return table.columnsCount(theRowId);
        } finally {
            // TODO Do we need to close the table too?
            close(dbInstance);
            close(be);
        }
    }

    /** Creates a new BEDatabase inside the Backend instance.
     * 
     * @param theDBName
     *        the database name */
    public BackendInstanceWrapper createDatabase(final String theDBName)
            throws Exception {
        Backend be = null;
        try {
            be = connect();
            final Collection<String> dblist = be.databases();
            if (dblist != null && dblist.contains(theDBName))
                throw new IllegalArgumentException("Database " + theDBName
                        + " Already Exists in " + be);
            be.createDatabase(theDBName);
            return this;
        } finally {
            close(be);
        }
    }

    /** Creates a new table inside a database.
     * 
     * @param theTableName
     *        the table name */
    public BackendInstanceWrapper createTable(final String theTableName)
            throws Exception {
        Backend be = null;
        BEDatabase dbInstance = null;
        try {
            be = connect();
            dbInstance = openDB(this.database, be);
            dbInstance.create(Base36.get(theTableName), new BETableProfile());
            return this;
        } finally {
            close(dbInstance);
            close(be);
        }
    }

    /** @return the database name. */
    @CheckForNull
    public String database() {
        return database;
    }

    /** sets the database name. */
    public BackendInstanceWrapper database(final String theDatabase) {
        this.database = theDatabase;
        return this;
    }

    /** @return details of backend */
    public String description() {
        return description;
    }

    /** Drop an existing database from a Backend instance.
     * 
     * @param theDBName
     *        the database name */
    public BackendInstanceWrapper dropDatabase(final String theDBName)
            throws Exception {
        Backend be = null;
        try {
            be = connect();
            final Collection<String> dblist = be.databases();
            if (dblist != null && !dblist.contains(theDBName.toLowerCase()))
                throw new IllegalArgumentException("Database " + theDBName
                        + " doesn't exists in " + be);
            be.dropDatabase(theDBName);
            return this;
        } finally {
            close(be);
        }
    }

    /** Drop an existing table from a Backend database.
     * 
     * @param theTableName
     *        the table name */
    public BackendInstanceWrapper dropTable(final String theTableName)
            throws Exception {
        Backend be = null;
        BEDatabase dbInstance = null;
        try {
            be = connect();
            dbInstance = openDB(this.database, be);
            dbInstance.drop(Base36.get(theTableName));
            return this;
        } finally {
            close(dbInstance);
            close(be);
        }
    }

    /** @return the database instance name. */
    public String instanceName() {
        return instanceName;
    }

    /** Returns Databases available inside a backend instance. */
    @CheckForNull
    public String[] listDB() throws Exception {
        Backend be = null;
        try {
            be = connect();
            final Collection<String> dblist = be.databases();
            if (dblist == null)
                return null;
            return dblist.toArray(ZERO_STRINGS);
        } finally {
            close(be);
        }
    }

    /** Returns the existing tables from present in a database.
     * 
     * @return base36[] of table names. */
    @CheckForNull
    public Base36[] listTables() throws Exception {
        Backend be = null;
        BEDatabase dbInstance = null;
        try {
            be = connect();
            dbInstance = openDB(this.database, be);
            if (dbInstance == null)
                throw new IllegalArgumentException("Database " + this.database
                        + " doesn't exists in " + be);
            final Collection<Base36> tables = dbInstance.tables();
            if (tables == null)
                return null;
            return tables.toArray(ZERO_BASE36S);
        } finally {
            close(dbInstance);
            close(be);
        }
    }

    /** @return name of the backend */
    public String name() {
        return name;
    }

    /** Reads data from DataInput interface and loads in a table represented by
     * 'tableName'
     * 
     * @param theTableName
     *        the table name
     * @param theDataInput
     *        data input interface, wraps an input stream of data to be
     *        loaded in the database table.
     * @param theMergeTables
     *        this method throws and exception if this flag is false and the
     *        table already exists, in order to avoid the already existing
     *        data being overwritten. */
    public BackendInstanceWrapper read(final String theTableName,
            final DataInput theDataInput, final boolean theMergeTables)
            throws Exception {
        Backend be = null;
        BEDatabase dbInstance = null;
        try {
            be = connect();
            dbInstance = openDB(this.database, be);
            final Base36 tn = Base36.get(theTableName);
            if (dbInstance == null)
                throw new NullPointerException("Database doesn't exist");
            BETable table = dbInstance.get(tn);
            if (table == null) {
                dbInstance.create(tn, new BETableProfile());
                table = dbInstance.get(tn);
            } else if (!theMergeTables)
                throw new IllegalArgumentException("Table " + theTableName
                        + " already exists use -mergeTables "
                        + "switch to import data into existing table.");
            if (table == null)
                throw new IllegalArgumentException("Table " + theTableName
                        + " couldn't be created/found");
            final TableDataOutput tout = new TableDataOutput(table);
            tout.dump(theDataInput);
            return this;
        } finally {
            close(dbInstance);
            close(be);
        }
    }

    /** Returns Number of rows in a table.
     * 
     * @param theTableName
     *        the table name
     * @return Number of rows in a table */
    public long tableRowCount(final String theTableName) throws Exception {
        Backend be = null;
        BEDatabase dbInstance = null;
        try {
            be = connect();
            dbInstance = openDB(this.database, be);
            final BETable table = table(dbInstance, theTableName, true);
            return table.size();
        } finally {
            // TODO Do we need to close the table?
            close(dbInstance);
            close(be);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Backend [name=" + name + "]";
    }

    /** This method transfers the data from one table to another table using the
     * Datastore API.
     * 
     * @param theTableName
     *        the source table name
     * @param theInsertInto
     *        instance of BackendConfigurationHolder class representing the
     *        destination database.
     * @param theDestTable
     *        the destination table name
     * @param theVerboseFlag
     *        the verbose
     * @param theMergeTable
     *        this method throws and exception if this flag is false and the
     *        destination table already exists, in order to avoid the
     *        already existing data being overwritten. */
    public void transfer(@Nullable final String theTableName,
            final BackendInstanceWrapper theInsertInto,
            @Nullable final String theDestTable, final boolean theVerboseFlag,
            final boolean theMergeTable) throws Exception {

        Backend b1 = null;
        BEDatabase d1 = null;
        Backend b2 = null;
        BEDatabase d2 = null;
        try {
            b1 = connect();
            d1 = openDB(database, b1);
            b2 = theInsertInto.connect();
            d2 = theInsertInto.openDB(theInsertInto.database, b2);
            if (theTableName != null && theDestTable != null) {
                final BETable fromtable = table(d1, theTableName, true);
                BETable totable = theInsertInto.table(d2, theDestTable, false);
                if (!theMergeTable && totable != null)
                    throw new IllegalArgumentException("Target table "
                            + theDestTable
                            + " already exists, use 'merge' switch.");
                if (totable == null)
                    totable = theInsertInto.create(d2,
                            Base36.get(theDestTable), fromtable);

                transfer(fromtable, totable);
            } else if (theTableName == null && theDestTable == null) {
                checkTablesExist(theMergeTable, d1, d2);
                for (final Base36 table : d1.tables()) {
                    final BETable fromtable = d1.get(table);
                    BETable toTable = d2.get(table);
                    if (toTable == null)
                        toTable = create(d2, table, fromtable);
                    assert (fromtable != null && toTable != null);
                    transfer(fromtable, toTable);
                }
            } else
                throw new IllegalArgumentException("From-table ("
                        + theTableName + ") and To-table (" + theDestTable
                        + ") both should be valid table names, or blank");
        } finally {
            // TODO Don't we need to close the tables?
            close(d1);
            close(b1);
            close(d2);
            close(b2);
        }
    }

    /** Exports data from a particular table to a output stream wrapped inside
     * DataOutput interface passed to this method.
     * 
     * @param theTableName
     *        the table name
     * @param theDataOut
     *        DataOutput interface which wraps the destination output stream
     * @param theRowFilter
     *        row id in case only a single row needs to be exported, to
     *        export full table pass null. */
    public void writeTable(final String theTableName,
            final DataOutput theDataOut, @Nullable final String theRowFilter)
            throws Exception {
        Backend be = null;
        BEDatabase dbInstance = null;
        try {
            be = connect();
            dbInstance = openDB(this.database, be);
            final BETable table = table(dbInstance, theTableName, true);
            final TableDataInput tdi = new TableDataInput(table,
                    rowFilter(theRowFilter));
            theDataOut.dump(tdi);
        } finally {
            // TODO Don't we need to close the table?
            close(dbInstance);
            close(be);
        }
    }
}
