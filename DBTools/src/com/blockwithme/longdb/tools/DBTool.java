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
// $codepro.audit.disable debuggingCode, methodChainLength, useBufferedIO
package com.blockwithme.longdb.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;
import java.util.zip.GZIPInputStream;

import javax.annotation.CheckForNull;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;

import com.blockwithme.longdb.client.BackendClient;
import com.blockwithme.longdb.client.ClientLoader;
import com.blockwithme.longdb.client.DBInfo;
import com.blockwithme.longdb.discovery.BackendInstance;
import com.blockwithme.longdb.discovery.BackendServiceLoader;
import com.blockwithme.longdb.discovery.BackendType;
import com.blockwithme.longdb.entities.Base36;
import com.blockwithme.longdb.util.DBUtils;

/** TODO: Could we somehow reuse the configure & setup code and allow
 * "user-defined" commands to be executed by this tool? I mean, we define some
 * interface with a perform method that receives all required objects/data, plus
 * maybe some generic parameter, and allow the user to add a user-defined class
 * that implement this interface to the classpath, and have the user specify the
 * name of the class implementing this interface as a command, with optional
 * parameter. The idea would be to make it easy to perform some kind of one-shot
 * data manipulation, without having to create a full-blown app with
 * initialization code and all. */
@ParametersAreNonnullByDefault
public final class DBTool {

    /** args passed to main method. */
    private static String[] ARGS;

    /** current Command line. */
    private static CommandLine CMD_LN;

    /** compress option '-z' */
    private static Option COMPRESS_OPT;

    /** all possible command options */
    private static Options CURR_OPTS;

    /** export/import data format '-f' option. */
    private static Option DATA_FORMAT;

    /** database name option */
    private static Option DB_DATABASE_NAME_OPT;

    /** backend instance name option */
    private static Option DB_INSTANCE_OPT;

    /** backend type option */
    private static Option DB_TYPE_OPT;

    /** destination database name option */
    private static Option DESTINATION_DATABASE_NAME_OPT;

    /** destination backend instance name option */
    private static Option DESTINATION_DB_INSTANCE_OPT;

    /** destination backend type option */
    private static Option DESTINATION_DB_TYPE_OPT;

    /** destination table name option */
    private static Option DESTINATION_TABLE_OPT;

    /** constant used to boolean value false */
    private static final boolean FALSE = false;

    /** used as file name prefix while creating file to export table data. */
    private static final String FILENAME_PREFIX = "export_";

    /** help option */
    private static Option HELP_OPTS;

    /** input file name option */
    private static Option INPUTFILE_OPT;

    /** merge tables option */
    private static Option MERGE_TABLES;

    /** character used for line feed. */
    private static final String NEW_LINE = System.getProperty("line.separator");

    /** output file option */
    private static Option OUTPUTFILE_OPT;

    /** command line parser object */
    private static PosixParser PARSER;

    /* Different command options */
    /** row id option */
    private static Option ROW_OPT;

    /** stdIn option flag that indicate if input needs to be read from stdIn */
    private static Option STD_IN;

    /** stdOut option flag that indicate if output needs to be written to stdOut */
    private static Option STD_OUT;

    /** table name option */
    private static Option TABLE_OPT;

    /** constant used to boolean value true */
    private static final boolean TRUE = true;

    /** Data Formats supported for Import/Exports TODO: Should not be hardwired. */
    private static final String[] VALIDFORMATS = { "csv", "json" };

    /** verbose flag */
    private static boolean VERBOSE;

    /** verbose option */
    private static Option VERBOSE_OPT;

    /** DB info. */
    private static DBInfo dBInfo(final boolean theDBName) throws Exception {
        return new DBInfo(dBType(DB_TYPE_OPT), getDBInstance(DB_INSTANCE_OPT),
                getDB(DB_DATABASE_NAME_OPT, theDBName));
    }

    /** DB type. */
    private static String dBType(final Option theDBtype) throws Exception {
        return optionVal(theDBtype);
    }

    /** Destination DB info. */
    private static DBInfo destinationDBInfo(final boolean theDBName)
            throws Exception {
        return new DBInfo(dBType(DESTINATION_DB_TYPE_OPT),
                getDBInstance(DESTINATION_DB_INSTANCE_OPT), getDB(
                        DESTINATION_DATABASE_NAME_OPT, theDBName));
    }

    /** Export to stream. */
    // TODO: theStdout is kind of superfluous here, as you could just say that a
    // null theFilePath means that
    @CheckForNull
    private static void exportToStream(final String theFilePath,
            final String theFormat, final String theTable,
            final boolean theStdout) throws Exception {
        final boolean compress = CMD_LN.hasOption(COMPRESS_OPT.getOpt());
        OutputStream os = null;
        final DBInfo info = dBInfo(FALSE);
        try {
            if (!theStdout)
                os = new FileOutputStream(theFilePath);
            else
                os = System.out;
            CRC32 checksum = null;
            OutputStream chesumStr;
            if (compress)
                chesumStr = new CRCGZipOutputStream(os);
            else if (theStdout)
                chesumStr = os;
            else {
                checksum = new CRC32();
                chesumStr = new CheckedOutputStream(os, checksum);
            }
            final PrintWriter ouputWriter = new PrintWriter(
                    new OutputStreamWriter(chesumStr, "UTF-8"));

            final ClientLoader loader = ClientLoader.newInstance(info)
                    .outputFormat(theFormat).outWriter(ouputWriter)
                    .verbose(VERBOSE).verboseStream(System.err);

            final BackendClient client = loader.loadClient();
            client.export(theTable, getRowFilter());

            if (!theStdout)
                writeCRC(theFilePath, chesumStr, compress, checksum);

        } finally {
            DBUtils.closeQuitely(os);
        }
    }

    /** Loads BackendClient using ClientLoader */
    private static BackendClient getClient(final DBInfo theInfo) {
        final ClientLoader loader = ClientLoader.newInstance(theInfo)
                .verbose(VERBOSE).verboseStream(System.err);
        return loader.loadClient();
    }

    /** Gets the DB name from command options */
    private static String getDB(final Option theDB, final boolean theDBName)
            throws Exception {
        return optionVal(theDB, theDBName);
    }

    /** Gets the DB instance name from command options */
    private static String getDBInstance(final Option theDBInstance)
            throws Exception {
        return optionVal(theDBInstance);
    }

    /** Performs CRC checksum, checks if file is compressed, created file input
     * stream */
    @SuppressWarnings("resource")
    private static InputStream getFileInputStream(final File theInfile)
            throws Exception {
        final boolean isCompressed = Utils.compressed(theInfile);
        if (!Utils.checkSum(theInfile, isCompressed))
            showWarn("CRC-32 check is not being perfromed on input file: "
                    + theInfile.getAbsolutePath()
                    + " either .crc32 file doesn't exist or its invalid");
        return isCompressed ? new GZIPInputStream(
                new FileInputStream(theInfile))
                : new FileInputStream(theInfile);
    }

    /** Returns format of files to be imported by import command. If format
     * option is not specified and input source is a 'file' then format is read
     * from file extension. Formats should be one of the elements in
     * VALIDFORMATS array. */
    private static String getFormat(final String theFile) {
        String format = optionVal(DATA_FORMAT, true);
        if (format == null) {
            final File fl = new File(theFile); // $codepro.audit.disable
                                               // com.instantiations.assist.eclipse.analysis.pathManipulation
            if (fl.exists() && !fl.isFile()) {
                final int mid = theFile.lastIndexOf('.');
                format = theFile.substring(mid + 1, theFile.length());
            }
        }
        if (format == null)
            invalidCommand("File format not specified. Use -f option for file format");
        if (!ArrayUtils.contains(VALIDFORMATS, format))
            invalidCommand("Valid file formats are :"
                    + Arrays.toString(VALIDFORMATS));
        return format;
    }

    /** Returns String specified for input files in the command line. Valid
     * values are 'stdIn' or existing folder name where files to be imported are
     * present */
    private static String getInputOption() {
        String outFileStr = optionVal(INPUTFILE_OPT, true);
        if (outFileStr == null)
            outFileStr = CMD_LN.hasOption(STD_IN.getOpt()) ? "stdin" : null;
        else {
            final File fl = new File(outFileStr); // $codepro.audit.disable
                                                  // com.instantiations.assist.eclipse.analysis.pathManipulation
            if (!fl.exists()) {
                invalidCommand("Invalid file " + fl.getAbsolutePath());
            }
        }
        if (outFileStr == null)
            errorAbort("Either -o or -stdout options are needed to export data");
        return outFileStr;
    }

    /** Gets the output option for export command, from command line options */
    private static String getOutputOption() {
        String outFileStr = optionVal(OUTPUTFILE_OPT, true);
        if (outFileStr == null)
            outFileStr = CMD_LN.hasOption(STD_OUT.getOpt()) ? "stdout" : null;
        if (outFileStr == null)
            errorAbort("Either -o or -stdout options are needed to export data");
        return outFileStr;
    }

    /** Gets the row filter, for copy command */
    private static String getRowFilter() {
        return optionVal(ROW_OPT, true);
    }

    /** Import from stream. */
    private static void importFromStream(final InputStream theInputStream,
            final String theTableName, final String theFormat) throws Exception {

        final boolean mergeTables = CMD_LN.hasOption(MERGE_TABLES.getOpt());
        final InputStreamReader reader = new InputStreamReader(theInputStream,
                "UTF-8");
        final ClientLoader loader = ClientLoader.newInstance(dBInfo(FALSE))
                .inputFormat(theFormat).inReader(reader).verbose(VERBOSE)
                .verboseStream(System.err);
        final BackendClient client = loader.loadClient();
        client.importData(theTableName, mergeTables);
    }

    /** Initialization done in this method. */
    @SuppressWarnings("static-access")
    private static void init() throws Exception {
        PARSER = new PosixParser();
        CURR_OPTS = new Options();

        HELP_OPTS = new Option("help", "print help message");

        VERBOSE_OPT = new Option("v", "verbose");

        DB_TYPE_OPT = OptionBuilder
                .withArgName("dbtype")
                .hasArg()
                .withDescription(
                        "database type of the database to be connected, use -show "
                                + "option to see the list of available database types and instances")
                .create("type");

        DB_INSTANCE_OPT = OptionBuilder
                .withArgName("instance")
                .hasArg()
                .withDescription(
                        "database instance of the database to be connected, use -show "
                                + "option to see the list of available database types and instances")
                .create("instance");

        DB_DATABASE_NAME_OPT = OptionBuilder.withArgName("database_name")
                .hasArg().withDescription("select table by table_name")
                .create("d");

        OUTPUTFILE_OPT = OptionBuilder.withArgName("file").hasArg()
                .withDescription("use given file for export").create("o");

        DATA_FORMAT = OptionBuilder.withArgName("format").hasArg()
                .withDescription("supported output file format json/csv")
                .create("f");

        INPUTFILE_OPT = OptionBuilder.withArgName("file").hasArg()
                .withDescription("use given file for import").create("i");

        TABLE_OPT = OptionBuilder.withArgName("table_name").hasArg()
                .withDescription("select table by table_name").create("t");

        ROW_OPT = OptionBuilder.withArgName("row_id").hasArg()
                .withDescription("row identifier").create("r");

        COMPRESS_OPT = OptionBuilder.withDescription(
                "compress the output file.").create("z");

        DESTINATION_DB_TYPE_OPT = OptionBuilder
                .withArgName("dest_dbtype")
                .hasArg()
                .withDescription(
                        "dbtype of the destination database, use -show "
                                + "option to see the list of available database types and instances")
                .create("dtype");

        DESTINATION_DB_INSTANCE_OPT = OptionBuilder
                .withArgName("dest_dbtype")
                .hasArg()
                .withDescription(
                        "instance of the destination database, use -show "
                                + "option to see the list of available database types and instances")
                .create("dinstance");

        DESTINATION_TABLE_OPT = OptionBuilder.withArgName("table_name")
                .hasArg().withDescription("select destination table name ")
                .create("dt");

        DESTINATION_DATABASE_NAME_OPT = OptionBuilder
                .withArgName("database_name").hasArg()
                .withDescription("select destination database name ")
                .create("dd");

        STD_OUT = OptionBuilder.withDescription(
                "Exported data will be written to 'Stdout'").create("stdout");

        STD_IN = OptionBuilder.withDescription(
                "Imported data will be read from 'Stdin'").create("stdin");

        MERGE_TABLES = OptionBuilder.withDescription(
                "Use 'mergeTables' to import/copy data into existing tables.")
                .create("mergeTables");

        CURR_OPTS
                .addOption(
                        "",
                        true,
                        "valid arguments - help, showtables, showdatabases, export, import, rowcount, "
                                + "columncount, dropdatabase, droptable "
                                + "createdatabase, createtable copy "
                                + NEW_LINE
                                + "example 1: 'DBTool -u cassandra:@localhost:9160:mydb showtables' "
                                + NEW_LINE
                                + "example 2: 'DBTool -u cassandra:embedded:@abc_cluster: showdatabases' "
                                + NEW_LINE
                                + "example 3: 'DBTool -u voltdb:@localhost:mydb -t mytable rowcount' "
                                + NEW_LINE);

        CURR_OPTS.addOption(HELP_OPTS);
        CURR_OPTS.addOption(VERBOSE_OPT);
        CURR_OPTS.addOption(DB_TYPE_OPT);
        CURR_OPTS.addOption(DB_INSTANCE_OPT);
        CURR_OPTS.addOption(DB_DATABASE_NAME_OPT);
        CURR_OPTS.addOption(OUTPUTFILE_OPT);
        CURR_OPTS.addOption(INPUTFILE_OPT);
        CURR_OPTS.addOption(TABLE_OPT);
        CURR_OPTS.addOption(ROW_OPT);
        CURR_OPTS.addOption(DESTINATION_DB_TYPE_OPT);
        CURR_OPTS.addOption(DESTINATION_DB_INSTANCE_OPT);
        CURR_OPTS.addOption(DESTINATION_DATABASE_NAME_OPT);
        CURR_OPTS.addOption(DESTINATION_TABLE_OPT);
        CURR_OPTS.addOption(COMPRESS_OPT);
        CURR_OPTS.addOption(DATA_FORMAT);
        CURR_OPTS.addOption(STD_OUT);
        CURR_OPTS.addOption(STD_IN);
        CURR_OPTS.addOption(MERGE_TABLES);
    }

    /** Throw exception for Invalid command message */
    private static void invalidCommand() {
        throw new IllegalArgumentException(
                "Invalid command, use -help for full list of options");
    }

    /** Throw exception for Invalid command message */
    private static void invalidCommand(final String theMessage) {
        throw new IllegalArgumentException(theMessage
                + " use -help for full list of options");
    }

    /** Retrieve value for specified option from command line. throws
     * IllegalArgumentException if option is missing. */
    private static String optionVal(final Option theOption) {
        return optionVal(theOption, false);
    }

    /** Retrieve value for specified option from command line. return null if
     * optional is true */
    @CheckForNull
    private static String optionVal(final Option theOptions,
            final boolean isOptional) {
        if (!isOptional && !CMD_LN.hasOption(theOptions.getOpt()))
            invalidCommand("Required option -" + theOptions.getOpt()
                    + " is missing.");
        return CMD_LN.getOptionValue(theOptions.getOpt());
    }

    /** Identify which command is being invoked and send the request to the
     * corresponding method. */
    private static void perform(final CommandType theCmdType) throws Exception {
        switch (theCmdType) {
        case HELP:
            showHelp();
            break;
        case SHOW:
            show();
            break;
        case SHOWDATABASES:
            processShowDatabase();
            break;
        case SHOWTABLES:
            processShowTables();
            break;
        case ROWCOUNT:
            processTableRowCount();
            break;
        case COLUMNCOUNT:
            processColumnCount();
            break;
        case CREATEDATABASE:
            processCreateDatabase();
            break;
        case CREATETABLE:
            processCreateTable();
            break;
        case DROPDATABASE:
            processDropDatabase();
            break;
        case DROPTABLE:
            processDropTable();
            break;
        case EXPORT:
            processExport();
            break;
        case IMPORT:
            processImport();
            break;
        case COPY:
            processTransfer();
            break;
        default:
            invalidCommand();
        }
    }

    /** Retrieve column count for a row id */
    private static void processColumnCount() throws Exception {

        final ClientLoader loader = ClientLoader.newInstance(dBInfo(FALSE))
                .verbose(VERBOSE).verboseStream(System.err);
        final BackendClient client = loader.loadClient();
        final long rowCount = client.columnCount(optionVal(TABLE_OPT),
                Long.valueOf(optionVal(ROW_OPT))); // $codepro.audit.disable
                                                   // handleNumericParsingErrors
        System.out.println(rowCount);

    }

    /** Parsing and Validation of command line args, and delegation to perform()
     * method for further processing */
    private static void processCommand(final String[] theArgs) throws Exception {
        CMD_LN = PARSER.parse(CURR_OPTS, theArgs);
        if (CMD_LN.hasOption(HELP_OPTS.getOpt())) {
            showHelp();
            return;
        }
        VERBOSE = CMD_LN.hasOption(VERBOSE_OPT.getOpt());
        if ((CMD_LN.getArgs() == null || CMD_LN.getArgs().length == 0)) {
            invalidCommand();
            return;
        }

        try {
            final String commandName = CMD_LN.getArgs()[0];
            final CommandType cmdType = CommandType.valueOf(commandName
                    .toUpperCase());
            perform(cmdType);
        } catch (final Exception e) {
            invalidCommand(e.getMessage());
        }
    }

    /** Processes create database command */
    private static void processCreateDatabase() throws Exception {
        final DBInfo info = dBInfo(FALSE);
        final BackendClient client = getClient(info);
        final String dbname = info.dbName();
        assert (dbname != null);
        client.createDatabase(dbname);
        if (VERBOSE)
            System.err.println("Database " + info.dbName() + " Created.");
    }

    /** Processes create table command */
    private static void processCreateTable() throws Exception {
        final DBInfo info = dBInfo(FALSE);
        final BackendClient client = getClient(info);
        client.createTable(optionVal(TABLE_OPT));
        if (VERBOSE)
            System.err.println("Table " + info.dbName() + " Created.");
    }

    /** Processes drop database command */
    private static void processDropDatabase() throws Exception {
        final DBInfo info = dBInfo(FALSE);
        final BackendClient client = getClient(info);
        final String dbName = info.dbName();
        assert (dbName != null);
        client.dropDatabase(dbName);
        if (VERBOSE)
            System.err.println("Database " + info.dbName() + " Dropped.");
    }

    /** Processes drop table command */
    private static void processDropTable() throws Exception {

        final DBInfo info = dBInfo(FALSE);
        final BackendClient client = getClient(info);
        client.dropTable(optionVal(TABLE_OPT));
        if (VERBOSE)
            System.err.println("Table " + optionVal(TABLE_OPT) + " Dropped.");
    }

    /** Export single row or entire table to a file. */
    private static void processExport() throws Exception {
        final String outFileStr = getOutputOption();
        final String format = optionVal(DATA_FORMAT);
        final String tableOpt = optionVal(TABLE_OPT, true);
        final boolean stdout = "stdout".equalsIgnoreCase(outFileStr);

        if (tableOpt == null && stdout)
            invalidCommand("Required option -t is missing. "
                    + "All tables cannot be written to stdout");

        if (stdout) {
            exportToStream(null, format, tableOpt, true);
        } else {
            final Map<String, String> map = tableMap(outFileStr, tableOpt,
                    format);
            for (final Entry<String, String> tableEntry : map.entrySet()) {
                exportToStream(tableEntry.getValue(), format,
                        tableEntry.getKey(), false);
            }
        }
    }

    /** Processes import command. */
    private static void processImport() throws Exception {

        final String infile = getInputOption();
        final String format = getFormat(infile);
        // table name is optional parameter for import command
        // if table name is not passed, its read from the file name
        // file name is of the format 'export_tableName.extn'
        String tableName = optionVal(TABLE_OPT, true);

        if ("stdin".equalsIgnoreCase(infile)) {
            importFromStream(System.in, optionVal(TABLE_OPT), format);
        } else {
            final File fl = new File(infile); // $codepro.audit.disable
                                              // com.instantiations.assist.eclipse.analysis.pathManipulation
            if (fl.isDirectory()) {
                final Collection<File> files = FileUtils.listFiles(fl,
                        new String[] { format }, false);
                if (files.isEmpty()) {
                    showWarn("No files to be imported");
                }
                for (final File file : files) {
                    if (tableName == null)
                        tableName = tableFromFileName(file, format);
                    if (tableName != null) // checks if fileName is valid.
                        importFromStream(getFileInputStream(file), tableName,
                                format);
                }
            } else {
                importFromStream(getFileInputStream(fl), optionVal(TABLE_OPT),
                        format);
            }
        }
    }

    /** show available databases. */
    private static void processShowDatabase() throws Exception {
        final DBInfo info = dBInfo(TRUE);
        final BackendClient client = getClient(info);
        final String[] dbNames = client.databases();
        if (dbNames == null || dbNames.length == 0) {
            return;
        }
        for (final String dbName : dbNames) {
            System.out.println(dbName);
        }
    }

    /** show tables in a database. */
    private static void processShowTables() throws Exception {

        final DBInfo info = dBInfo(FALSE);
        final BackendClient client = getClient(info);

        final Base36[] tables = client.tables();
        if (tables == null || tables.length == 0) {
            return;
        }
        for (final Base36 tableName : tables) {
            System.out.println(tableName.toString());
        }
    }

    /** Retrieve row count for a table */
    private static void processTableRowCount() throws Exception {
        final DBInfo info = dBInfo(FALSE);
        final BackendClient client = getClient(info);
        final long rowCount = client.tableRowCount(optionVal(TABLE_OPT));
        System.out.println(rowCount);
    }

    /** Processes copy command */
    private static void processTransfer() throws Exception {
        final DBInfo sourceInfo = dBInfo(FALSE);
        final DBInfo targetInfo = destinationDBInfo(FALSE);
        final ClientLoader loader = ClientLoader.newInstance(sourceInfo)
                .targetDB(targetInfo).verbose(VERBOSE)
                .verboseStream(System.err);
        final BackendClient client = loader.loadClient();

        client.transferData(optionVal(TABLE_OPT, true),
                optionVal(DESTINATION_TABLE_OPT, true),
                CMD_LN.hasOption(MERGE_TABLES.getOpt()));
    }

    /** Show command prints available Backend configurations. */
    private static void show() {
        final Iterator<BackendType> itr = BackendServiceLoader.getInstance()
                .availableBackends();
        if (!itr.hasNext()) {
            System.out
                    .println("No Backend databases are configured currently.");
            return;
        }
        System.out
                .println("Following Backend databases are configured currently :");
        while (itr.hasNext()) {
            final BackendType backendType = itr.next();
            System.out.println("type: " + backendType.name()
                    + " has following instances: ");
            final List<BackendInstance> instances = backendType.instances();
            for (final BackendInstance backendInstance : instances) {
                System.out.println("\t" + backendInstance.name() + " - "
                        + backendInstance.description());
            }
        }
    }

    // CHECKSTYLE stop magic number check
    /** Show help. */
    private static void showHelp() {
        final HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(300);
        formatter.printHelp("DBTool", CURR_OPTS);
    }

    // CHECKSTYLE resume magic number check

    /** Show warn. */
    private static void showWarn(final String theWarning) {
        System.err.println(theWarning);
    }

    /** If file name is of the format export_tableName.extn extracts the table
     * name from this file name, else returns null */
    @CheckForNull
    private static String tableFromFileName(final File theFile,
            final String theFormat) {
        final String fileName = theFile.getName();
        String tName = null;
        try {
            if (fileName.startsWith(FILENAME_PREFIX)
                    && fileName.endsWith(theFormat)) {
                final String tmp = fileName.substring(FILENAME_PREFIX.length(),
                        fileName.indexOf("." + theFormat));
                Base36.get(tmp); // validate if the name is Base36 convertible.
                tName = tmp;
            }
        } catch (final Exception e) {
            // ignore the exception and return null
            // if null is returned file will not be considered to be imported.
        }
        return tName; // returns null if fileName is not valid
    }

    /** Creates a map of Tables names and File names for each table. File name is
     * of the format export_tableName.extn */
    private static Map<String, String> tableMap(final String theOutFile,
            final String theTableOption, final String theFormat)
            throws Exception {

        final Map<String, String> map = new HashMap<String, String>();
        final File outputDir = new File(theOutFile); // $codepro.audit.disable
                                                     // com.instantiations.assist.eclipse.analysis.pathManipulation
        if (!outputDir.exists() || !outputDir.isDirectory())
            invalidCommand(outputDir.getAbsolutePath()
                    + " must be an existing directory.");

        Base36[] tables;
        final DBInfo info = dBInfo(FALSE);
        final BackendClient client = getClient(info);

        if (theTableOption == null)
            tables = client.tables();
        else
            tables = new Base36[] { Base36.get(theTableOption) };

        String fileDir = outputDir.getAbsolutePath();
        fileDir = fileDir.endsWith(File.separator) ? fileDir : fileDir
                + File.separator;

        for (final Base36 base36 : tables) {
            map.put(base36.toString(),
                    fileDir + FILENAME_PREFIX + base36.toString() + "."
                            + theFormat);
        }
        return map;
    }

    /** Write CRC value in the meta-data file */
    private static void writeCRC(final String theOutFile,
            final OutputStream theBOStrm, final boolean isCompressed,
            final CRC32 theChecksum) throws IOException {
        final Long chksum = isCompressed ? ((CRCGZipOutputStream) theBOStrm)
                .getCRC().getValue() : theChecksum.getValue();
        final File crcFile = new File((new File(theOutFile)).getAbsolutePath() // $codepro.audit.disable
                                                                               // com.instantiations.assist.eclipse.analysis.pathManipulation
                + ".crc32");
        FileUtils.writeStringToFile(crcFile, Long.toHexString(chksum));
    }

    /** Throw exception to print Error abort message */
    static void errorAbort(final String theMessage) {
        throw new RuntimeException(theMessage);
    }

    /** DBTool main method performs initialization and delegates to
     * processCommand() NB: I prefer the main() to always be at the end...
     * 
     * @param theArgs
     *        the arguments */
    public static void main(final String[] theArgs) { // $codepro.audit.disable
                                                      // illegalMainMethod
        try {
            init();
            ARGS = Arrays.copyOf(theArgs, theArgs.length);
            if (ARGS == null || ARGS.length == 0
                    || ARGS[0].trim().length() == 0) {
                invalidCommand();
                return;
            }
            processCommand(ARGS);
        } catch (final Exception e) {
            System.err.println(e.getMessage());
            // System.exit(-1);
        }
        // System.exit(0);
    }

    /** Private constructor only main method of this class invoked externally */
    private DBTool() {
    }
}
