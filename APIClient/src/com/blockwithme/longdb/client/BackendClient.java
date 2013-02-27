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
package com.blockwithme.longdb.client;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import com.blockwithme.longdb.client.internal.BackendInstanceWrapper;
import com.blockwithme.longdb.client.io.DataInput;
import com.blockwithme.longdb.client.io.DataOutput;
import com.blockwithme.longdb.entities.Base36;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import edu.umd.cs.findbugs.annotations.CheckForNull;

/** This class acts as a client for the Datastore (Backend) API. Provides methods
 * to perform database operations like export/import. This class is used by the
 * DBTool. */
@ParametersAreNonnullByDefault
public class BackendClient {

    /** The data input (source). */
    @CheckForNull
    private DataInput dIn;

    /** The data output (sink) */
    @CheckForNull
    private DataOutput dOut;

    /** The source db info. */
    private final BackendInstanceWrapper sourceDB;

    /** The target db info */
    @CheckForNull
    private BackendInstanceWrapper targetDB;

    /** The verbose flag. */
    private boolean verbose;

    /** Protected Constructor with so that this class can be loaded only through
     * the related classes i.e. @{link ClientLoader}.
     * 
     * @param theSourceDB
     *        the source db wrapper object to be injected. */
    @Inject
    protected BackendClient(
            @Named("source") final BackendInstanceWrapper theSourceDB) {
        this.sourceDB = theSourceDB;
    }

    /** @param theDataInput
     *        the DataInput object to be injected. */
    @Inject
    void dataInput(@Nullable final DataInput theDataInput) {
        this.dIn = theDataInput;
    }

    /** @param theDataOut
     *        the DataOutput to be injected */
    @Inject
    void dataOutput(@Nullable final DataOutput theDataOut) {
        this.dOut = theDataOut;
    }

    /** @param theTargetDB
     *        the target db wrapper object to be injected. */
    @Inject
    void targetDB(
            @Nullable @Named("target") final BackendInstanceWrapper theTargetDB) {
        this.targetDB = theTargetDB;
    }

    /** Returns approximate column count in a row count for a particular table.
     * 
     * @param theTableName
     *        the table name
     * @param theRowId
     *        the row id
     * @return the number of columns */
    public long columnCount(final String theTableName, final long theRowId)
            throws Exception {
        Preconditions.checkNotNull(theTableName, "tableName is null");
        return sourceDB.columnCount(theTableName, theRowId);
    }

    /** Creates a new Database instance - Some DB implementations e.g. VoltDB do
     * not support this operation
     * 
     * @param theDBName
     *        the database name */
    public BackendClient createDatabase(final String theDBName)
            throws Exception {
        Preconditions.checkNotNull(theDBName, "databaseName is null");
        sourceDB.createDatabase(theDBName);
        return this;
    }

    /** Creates a new Table in the Database Specified by ConnectionSettings -Some
     * DB implementations e.g. VoltDB do not support this operation
     * 
     * @param theTableName
     *        the table name */
    public BackendClient createTable(final String theTableName)
            throws Exception {
        Preconditions.checkNotNull(theTableName, "tableName is null");
        sourceDB.createTable(theTableName);
        return this;
    }

    /** Returns String array of database names for the given connection settings.
     * 
     * @return the database names */
    public String[] databases() throws Exception {
        return sourceDB.listDB();
    }

    /** Drops a Database represented by settings.database()
     * 
     * @param theDBName
     *        the database name
     * @throws Exception
     *         throws an exception if fails to drop database. */
    public BackendClient dropDatabase(final String theDBName) throws Exception {
        Preconditions.checkNotNull(theDBName, "databaseName is null");
        sourceDB.dropDatabase(theDBName);
        return this;
    }

    /** Drops a Table represented by tableName in Database specified by
     * ConnectionSettings.
     * 
     * @param theTableName
     *        the table name */
    public BackendClient dropTable(final String theTableName) throws Exception {
        Preconditions.checkNotNull(theTableName, "tableName is null");
        sourceDB.dropTable(theTableName);
        return this;
    }

    /** Exports Data to external destination like File/OutputStreams.
     * 
     * @param theTableName
     *        Table name of source DB.
     * @param theRowFilter
     *        Filter criteria if any. */
    public BackendClient export(final String theTableName,
            @Nullable final String theRowFilter) throws Exception {
        Preconditions.checkNotNull(theTableName, "tableName is null");
        assert (dOut != null);
        sourceDB.writeTable(theTableName, dOut, theRowFilter);
        return this;
    }

    /** Imports data from DataInput (External Source like File/InputStream) to a
     * Table.
     * 
     * @param theTableName
     *        Table name where data will be imported
     * @param theMergeTables
     *        indicates that its okay if Table already exists, data will be
     *        overwritten */
    public BackendClient importData(final String theTableName,
            final boolean theMergeTables) throws Exception {
        Preconditions.checkNotNull(theTableName, "tableName is null");
        assert (dIn != null);
        sourceDB.read(theTableName, dIn, theMergeTables);
        return this;
    }

    /** Returns approximate row count for a particular table.
     * 
     * @param theTableName
     *        Table name
     * @return number of rows in the table. */
    public long tableRowCount(final String theTableName) throws Exception {
        Preconditions.checkNotNull(theTableName, "tableName is null");
        return sourceDB.tableRowCount(theTableName);
    }

    /** Returns Array of Table names.
     * 
     * @return the table names */
    public Base36[] tables() throws Exception {
        return sourceDB.listTables();
    }

    /** Transfers(Copies) Data from one Database/table to another Database/table.
     * 
     * @param theSourceTable
     *        source table - should be null if full DB is being copied.
     * @param theTargetTable
     *        Destination table - should be null if full DB is being copied.
     * @param theMergeTables
     *        indicates that its okay if target table(s) already exist, data
     *        will be overwritten */
    public BackendClient transferData(@Nullable final String theSourceTable,
            @Nullable final String theTargetTable, final boolean theMergeTables)
            throws Exception {
        assert (targetDB != null);
        sourceDB.transfer(theSourceTable, targetDB, theTargetTable, verbose,
                theMergeTables);
        return this;
    }
}
