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
// $codepro.audit.disable debuggingCode, noExplicitExit, largeNumberOfParameters
package com.blockwithme.longdb.tools.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;

import com.blockwithme.longdb.BEDatabase;
import com.blockwithme.longdb.BETable;
import com.blockwithme.longdb.BETableProfile;
import com.blockwithme.longdb.Backend;
import com.blockwithme.longdb.bdb.BDBConstants;
import com.blockwithme.longdb.cassandra.embedded.CassandraEmbConstants;
import com.blockwithme.longdb.client.entities.Row;
import com.blockwithme.longdb.client.io.DataInput;
import com.blockwithme.longdb.client.io.json.JSONDataInput;
import com.blockwithme.longdb.discovery.BackendServiceLoader;
import com.blockwithme.longdb.entities.Base36;
import com.blockwithme.longdb.h2.H2Constants;
import com.blockwithme.longdb.leveldb.LevelDBConstants;
import com.blockwithme.longdb.test.util.JSONUtil;
import com.blockwithme.longdb.tools.DBTool;

/** The class DBToolUnitTest. */
// CHECKSTYLE IGNORE FOR NEXT 1 LINES
public final class DBToolTest {

    /** The Constant DB_NAME. */
    private static final String DB_NAME = "DBToolTest";

    /** The Constant TABLE1. */
    private static final String TABLE1 = "DBToolTable1";

    /** The Constant TABLE2. */
    private static final String TABLE2 = "DBToolTable2";

    /** The Constant TABLE3. */
    private static final String TABLE3 = "DBToolTable3";

    /** The Constant TABLE4. */
    private static final String TABLE4 = "DBToolTable4";

    /** Column count.
     * 
     * @param theType the type
     * @param theInstance the instance
     * @param theDBName the db name
     * @param theTableName the table name
     * @param theRowID the row id */
    private static void columnCount(final String theType,
            final String theInstance, final String theDBName,
            final String theTableName, final String theRowID) {
        final String[] argsStrings = { "-type", theType, "-instance",
                theInstance, "-d", theDBName, "-t", theTableName, "-r",
                theRowID, "columncount" };
        runCommand(argsStrings);
    }

    /** Test all operations.
     * 
     * @param theType the type
     * @param theInstance the instance
     * @throws Exception the exception */
    private static void testAllOps(final String theType,
            final String theInstance) throws Exception {

        System.out.println("Database Type :" + theType + " instance name "
                + theInstance);
        System.out.println("showdatabases command ");
        showDatabase(theType, theInstance);

        System.out.println("Creating database :  DBToolTest");
        createDatabase(theType, theInstance, DB_NAME);

        System.out.println("showdatabases command ");
        showDatabase(theType, theInstance);

        System.out.println("Creating first table");
        createTable(theType, theInstance, DB_NAME, TABLE1);

        System.out.println("Loading data into first table ");
        loadDB(theType, theInstance, DB_NAME, TABLE1);

        System.out.println("Row count first table");
        rowCount(theType, theInstance, DB_NAME, TABLE1);

        System.out.println("Column count in row id 1 first table");
        columnCount(theType, theInstance, DB_NAME, TABLE1, "1");

        System.out.println("Exporting in json format uncompressed.");
        export(theType, theInstance, DB_NAME, TABLE1, "json", false);

        System.out.println("Exporting in json format compressed.");
        export(theType, theInstance, DB_NAME, TABLE1, "json", true);

        System.out.println("Exporting in csv uncompressed.");
        export(theType, theInstance, DB_NAME, TABLE1, "csv", false);

        System.out.println("Exporting in csv compressed.");
        export(theType, theInstance, DB_NAME, TABLE1, "csv", true);

        System.out.println("Creating a second table");
        createTable(theType, theInstance, DB_NAME, TABLE2);

        System.out.println("Importing into second table csv");
        importData(theType, theInstance, DB_NAME, TABLE2, "csv", true);

        System.out.println("Row count second table.");
        rowCount(theType, theInstance, DB_NAME, TABLE2);

        System.out.println("Creating a third table");
        createTable(theType, theInstance, DB_NAME, TABLE3);

        System.out.println("Importing into third table (json)");
        importData(theType, theInstance, DB_NAME, TABLE3, "json", true);

        System.out.println("Row count third table");
        rowCount(theType, theInstance, DB_NAME, TABLE3);

        System.out.println("Copying data from first table to fourth table "
                + "(automatically created by the tool)");
        copy(theType, theInstance, DB_NAME, TABLE1, theType, theInstance,
                DB_NAME, TABLE4);

        System.out.println("Row count fourth table.");
        rowCount(theType, theInstance, DB_NAME, TABLE4);

        // Test drop table.
        System.out.println("Dropping fourth table.");
        dropTable(theType, theInstance, DB_NAME, TABLE4);

        System.out.println("Show tables command :");
        showTables(theType, theInstance, DB_NAME);

        // Test drop database.
        System.out.println("Dropping database");
        dropDatabase(theType, theInstance, DB_NAME);
    }

    /** @param theType the type
     * @param theInstance the instance
     * @param theDBName the db name
     * @param theTableName the table name
     * @param theType2 the type2
     * @param theInstance2 the instance2
     * @param theDBName2 the db name2
     * @param theTableName2 the table name2 */
    public static void copy(final String theType, final String theInstance,
            final String theDBName, final String theTableName,
            final String theType2, final String theInstance2,
            final String theDBName2, final String theTableName2) {
        final String[] argsStrings = { "-type", theType, "-instance",
                theInstance, "-d", theDBName, "-t", theTableName, "-dtype",
                theType2, "-dinstance", theInstance2, "-dd", theDBName2, "-dt",
                theTableName2, "copy" };
        runCommand(argsStrings);
    }

    /** Creates the database.
     * 
     * @param theType the type
     * @param theInstance the instance
     * @param theDBName the db name
     * @throws Exception the exception */
    public static void createDatabase(final String theType,
            final String theInstance, final String theDBName) throws Exception {
        final String[] argsStrings = { "-type", theType, "-instance",
                theInstance, "-d", theDBName, "createdatabase" };
        runCommand(argsStrings);
    }

    /** Creates the table.
     * 
     * @param theType the type
     * @param theInstance the instance
     * @param theDBName the db name
     * @param theTableName the table name
     * @throws Exception the exception */
    public static void createTable(final String theType,
            final String theInstance, final String theDBName,
            final String theTableName) throws Exception {
        final String[] argsStrings = { "-type", theType, "-instance",
                theInstance, "-d", theDBName, "-t", theTableName, "createtable" };
        runCommand(argsStrings);
    }

    /** Drop database.
     * 
     * @param theType the type
     * @param theInstance the instance
     * @param theDBName the db name
     * @throws Exception the exception */
    public static void dropDatabase(final String theType,
            final String theInstance, final String theDBName) throws Exception {
        final String[] argsStrings = { "-type", theType, "-instance",
                theInstance, "-d", theDBName, "dropdatabase" };
        runCommand(argsStrings);
    }

    /** Drop table.
     * 
     * @param theType the type
     * @param theInstance the instance
     * @param theDBName the db name
     * @param theTableName the table name
     * @throws Exception the exception */
    public static void dropTable(final String theType,
            final String theInstance, final String theDBName,
            final String theTableName) throws Exception {
        final String[] argsStrings = { "-type", theType, "-instance",
                theInstance, "-d", theDBName, "-t", theTableName, "droptable" };
        runCommand(argsStrings);
    }

    /** Export.
     * 
     * @param theType the type
     * @param theInstance the instance
     * @param theDBName the db name
     * @param theTableName the table name
     * @param theFormat the format
     * @param isCompressed the compressed */
    public static void export(final String theType, final String theInstance,
            final String theDBName, final String theTableName,
            final String theFormat, final boolean isCompressed) {
        final String[] argsStrings;
        if (!isCompressed) {
            final String[] options = { "-type", theType, "-instance",
                    theInstance, "-d", theDBName, "-t", theTableName, "-o",
                    ".\\data", "-f", theFormat, "export" };
            argsStrings = options;
        } else {
            final String[] options = { "-type", theType, "-instance",
                    theInstance, "-d", theDBName, "-t", theTableName, "-o",
                    ".\\data", "-f", theFormat, "-z", "export" };
            argsStrings = options;
        }
        runCommand(argsStrings);
    }

    /** From json.
     * 
     * @throws Exception the exception */
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "DM_DEFAULT_ENCODING")
    public static void fromJSON() throws Exception {
        @SuppressWarnings("resource")
        final DataInput output = new JSONDataInput(new BufferedReader(
                new FileReader("jsongenerator.txt")));
        while (output.hasNext()) {
            final Row r = output.next();
            if (r != null)
                System.out.println(r.rowId() + " "
                        + Arrays.toString(r.columns().columns()));
        }
    }

    /** Import data.
     * 
     * @param theType the type
     * @param theInstance the instance
     * @param theDBName the db name
     * @param theTableName the table name
     * @param theFormat the format
     * @param isCompressed the compressed */
    public static void importData(final String theType,
            final String theInstance, final String theDBName,
            final String theTableName, final String theFormat,
            final boolean isCompressed) {
        final String[] argsStrings = { "-type", theType, "-instance",
                theInstance, "-d", theDBName, "-t", theTableName, "-i",
                ".\\data", "-f", theFormat, "-mergeTables", "import" };
        runCommand(argsStrings);
    }

    /** Load db.
     * 
     * @param theType the type
     * @param theInstance the instance
     * @param theDBName the db name
     * @param theTableName the table name
     * @throws Exception the exception */
    public static void loadDB(final String theType, final String theInstance,
            final String theDBName, final String theTableName) throws Exception {

        final JSONUtil jsonUtil = new JSONUtil("/InputJSON.txt");
        final Backend backend = BackendServiceLoader.getInstance()
                .loadInstance(theType, theInstance);

        BEDatabase db = backend.openDatabase(theDBName);
        if (db == null) {
            backend.createDatabase(theDBName);
            db = backend.openDatabase(theDBName);
            if (db == null) {
                throw new IllegalArgumentException("Database " + theDBName
                        + " not found ");
            }
        }
        BETable table = db.get(Base36.get(theTableName));
        if (table == null) {
            db.create(Base36.get(theTableName), new BETableProfile());
            table = db.get(Base36.get(theTableName));
        }
        if (table != null)
            jsonUtil.loadTableFromJSONFile(table);
        backend.shutdown();
    }

    /** The main test method.
     * 
     * @param theArgs the arguments
     * @throws Exception the exception */
    public static void main(final String[] theArgs) throws Exception { // $codepro.audit.disable
                                                                       // illegalMainMethod

        String root = System.getProperty("PB_ROOT");
        if (root == null) {
            root = System.getenv("PB_ROOT");
        }
        if (root == null) {
            System.err.println("System property 'PB_ROOT' not found.");
            System.exit(-1);
        }
        final File fl = new File(root);
        if (!fl.exists()) {
            System.err.println("'PB_ROOT' folder " + root + " not found.");
            System.exit(-1);
        }
        if (!root.endsWith(File.separator)) {
            root += File.separator;
        }

        final String logProp = root + "conf" + File.separator
                + "log4j-test.properties";
        System.setProperty("log4j.configuration", "file:///" + logProp);

        System.out.println("Show help :");
        showHelp();

        System.out.println("Show backends :");
        show();

        final String instance = "default";
        testAllOps(H2Constants.PROJECT_NAME, instance);
        testAllOps(BDBConstants.PROJECT_NAME, instance);
        testAllOps(LevelDBConstants.PROJECT_NAME, instance);
        testAllOps(CassandraEmbConstants.PROJECT_NAME, instance);
        System.err.println("WARNING : Delete all the contents of :"
                + (new File(".\\data")).getAbsolutePath()
                + " before re-running the tests.");
        System.exit(0);
    }

    /** Row count.
     * 
     * @param theType the type
     * @param theInstance the instance
     * @param theDBName the db name
     * @param theTableName the table name */
    public static void rowCount(final String theType, final String theInstance,
            final String theDBName, final String theTableName) {
        final String[] argsStrings = { "-type", theType, "-instance",
                theInstance, "-d", theDBName, "-t", theTableName, "rowcount" };
        runCommand(argsStrings);
    }

    /** Run command.
     * 
     * @param theArgsStrings the args strings */
    public static void runCommand(final String[] theArgsStrings) {
        System.out.println(Arrays.toString(theArgsStrings));
        DBTool.main(theArgsStrings);
    }

    /** Show help. */
    public static void show() {
        final String[] argsStrings = { "show" };
        runCommand(argsStrings);
    }

    /** Show database.
     * 
     * @param theType the type
     * @param theInstance the instance
     * @throws Exception the exception */
    public static void showDatabase(final String theType,
            final String theInstance) throws Exception {
        final String[] argsStrings = { "-type", theType, "-instance",
                theInstance, "showdatabases" };
        runCommand(argsStrings);
    }

    /** Show help. */
    public static void showHelp() {
        final String[] argsStrings = { "-help" };
        runCommand(argsStrings);
    }

    /** Show tables.
     * 
     * @param theType the type
     * @param theInstance the instance
     * @param theDBName the db name
     * @throws Exception the exception */
    public static void showTables(final String theType,
            final String theInstance, final String theDBName) throws Exception {
        final String[] argsStrings = { "-type", theType, "-instance",
                theInstance, "-d", theDBName, "showtables" };
        runCommand(argsStrings);
    }
}
