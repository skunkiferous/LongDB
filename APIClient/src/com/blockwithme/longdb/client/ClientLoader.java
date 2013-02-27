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
// $codepro.audit.disable anonymousClassMemberVisibility
package com.blockwithme.longdb.client;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.Iterator;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import com.blockwithme.longdb.client.internal.BackendInstanceWrapper;
import com.blockwithme.longdb.client.io.DataInput;
import com.blockwithme.longdb.client.io.DataOutput;
import com.blockwithme.longdb.client.io.csv.CSVDataInput;
import com.blockwithme.longdb.client.io.csv.CSVDataOutput;
import com.blockwithme.longdb.client.io.json.JSONDataInput;
import com.blockwithme.longdb.client.io.json.JSONDataOutput;
import com.blockwithme.longdb.discovery.BackendInstance;
import com.blockwithme.longdb.discovery.BackendServiceLoader;
import com.blockwithme.longdb.discovery.BackendType;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

import edu.umd.cs.findbugs.annotations.CheckForNull;

/** The Class ClientLoader provides a mechanism to create an instance of
 * BackendClient object with the required dependency settings. */
@ParametersAreNonnullByDefault
public final class ClientLoader {

    /** The input format. */
    @CheckForNull
    private String inputFormat;

    /** The in reader. */
    @CheckForNull
    private Reader inReader;

    /** The output format. */
    @CheckForNull
    private String outputFormat;

    /** The out writer. */
    @CheckForNull
    private PrintWriter outWriter;

    /** The source db. */
    private DBInfo sourceDB;

    /** The target db. */
    @CheckForNull
    private DBInfo targetDB;

    /** The verbose. */
    private boolean verbose;

    /** The verbose stream. */
    private PrintStream verboseStream;

    /** Uses Backend service loader to lookup an appropriate backend type and
     * instance, if found it creates a BackendWrapper object and returns. Else
     * throws IllegalArgumentException
     * 
     * @param theSettings
     *        the settings
     * @return the backend instance wrapper */
    private static BackendInstanceWrapper backendInstanceWrapper(
            final DBInfo theSettings) {
        final Iterator<BackendType> beItr = BackendServiceLoader.getInstance()
                .availableBackends();
        while (beItr.hasNext()) {
            final BackendType betype = beItr.next();
            if (betype.name().equals(theSettings.backendType())) {
                for (final BackendInstance ins : betype.instances()) {
                    if (ins.name().equals(theSettings.instanceName()))
                        return new BackendInstanceWrapper(betype.name(),
                                ins.description(), ins.name(),
                                theSettings.dbName());
                }
            }
        }
        throw new IllegalArgumentException("Either Database type :"
                + theSettings.backendType() + " or Instance :"
                + theSettings.instanceName() + " not found");
    }

    /** Creates a new instance.
     * 
     * @param theSourceDB
     *        the source database settings
     * @return the ClientLoader new instance */
    public static ClientLoader newInstance(final DBInfo theSourceDB) {
        final ClientLoader instance = new ClientLoader();
        instance.sourceDB = checkNotNull(theSourceDB, "sourceDB is null");
        return instance;
    }

    /** Instantiates a new client loader. */
    private ClientLoader() {
    }

    /** @return the input data format used for importing data
     * @see BackendClient#importData(String, boolean) */
    @CheckForNull
    public String inputFormat() {
        return inputFormat;
    }

    /** Format of data being imported.
     * 
     * @param theInputFormat
     *        the input format, possible values are 'csv', 'json' etc.
     * @return the modified ClientLoader instance
     * @see BackendClient#importData(String, boolean) */
    public ClientLoader inputFormat(final String theInputFormat) {
        this.inputFormat = checkNotNull(theInputFormat, "inputFormat is null");
        return this;
    }

    /** @return the data reader object, used for importing data
     * @see BackendClient#importData(String, boolean) */
    @CheckForNull
    public Reader inReader() {
        return inReader;
    }

    /** Input Reader object to read the data for importing.
     * 
     * @param theReader
     *        the input reader object.
     * @return the modified ClientLoader instance
     * @see BackendClient#importData(String, boolean) */
    public ClientLoader inReader(final Reader theReader) {
        this.inReader = checkNotNull(theReader, "inReader is null");
        return this;
    }

    /** Loads BackendClient instance with required settings.
     * 
     * @return the BackendClient object */
    public BackendClient loadClient() {
        final Injector inject = Guice.createInjector(new AbstractModule() {

            /*
             * (non-Javadoc)
             * 
             * @see com.google.inject.AbstractModule#configure()
             */
            @Override
            protected void configure() {
                try {

                    bind(BackendClient.class);
                    if (verboseStream != null) {
                        bind(PrintStream.class).annotatedWith(
                                Names.named("verboseStream")).toInstance(
                                verboseStream);
                    }
                    bind(Boolean.class).annotatedWith(
                            Names.named("verboseflag")).toInstance(verbose);

                } catch (final Exception e) {
                    e.printStackTrace();
                    throw new IllegalStateException(e.getMessage(), e);
                }
            }

            // TODO: remove the if-else logic and make this more dynamic
            /** Provide input reader. */
            @Provides
            public DataInput provideInputReader() {
                DataInput input = null;
                final Reader inRdr = inReader;
                if ((inRdr != null) && (inputFormat != null)) {
                    if ("csv".equalsIgnoreCase(inputFormat))
                        input = new CSVDataInput(inRdr);
                    else if ("json".equalsIgnoreCase(inputFormat))
                        input = new JSONDataInput(inRdr);
                    else
                        throw new IllegalArgumentException("Input format "
                                + inputFormat + " not supported."
                                + " Valid types are csv, json");
                    input.verboseOptions(verboseStream, verbose);
                }
                return input;
            }

            // TODO: remove the if-else logic and make this more dynamic
            @Provides
            public DataOutput provideOutputWriter() {
                DataOutput out = null;
                final PrintWriter oWriter = outWriter;
                if ((oWriter != null) && (outputFormat != null)) {
                    if ("csv".equalsIgnoreCase(outputFormat)) {
                        out = new CSVDataOutput(oWriter);
                    } else if ("json".equalsIgnoreCase(outputFormat)) {
                        out = new JSONDataOutput(oWriter);
                    } else
                        throw new IllegalArgumentException("Output format "
                                + outputFormat + " not supported."
                                + " Valid types are csv, json");
                    out.verboseOptions(verboseStream, verbose);
                }
                return out;
            }

            /** Provides BackendInstanceWrapper instance to be injected where
             * 
             * @see BackendClient
             * @return the backend instance wrapper */
            @Provides
            @Named("source")
            public BackendInstanceWrapper provideSourceWrapper() {
                return backendInstanceWrapper(sourceDB);
            }

            @Provides
            @Named("target")
            @Nullable
            @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
            public BackendInstanceWrapper provideTargetWrapper() {
                if (targetDB != null) {
                    return backendInstanceWrapper(targetDB);
                } else
                    return null;
            }

        });
        return inject.getInstance(BackendClient.class);
    }

    /** @return the data format exported data.
     * @see BackendClient#export(String, String) */
    @CheckForNull
    public String outputFormat() {
        return outputFormat;
    }

    /** Format of the data being exported.
     * 
     * @param theOutputFormat
     *        the output format, possible values are 'csv', 'json' etc.
     * @return the modified ClientLoader instance
     * @see BackendClient#export(String, String) */
    public ClientLoader outputFormat(final String theOutputFormat) {
        this.outputFormat = checkNotNull(theOutputFormat,
                "outputFormat is null");
        return this;
    }

    /** @return the output writer object for exporting data.
     * @see BackendClient#export(String, String) */
    @CheckForNull
    public PrintWriter outWriter() {
        return outWriter;
    }

    /** Out writer object for exporting data.
     * 
     * @param theOutWriter
     *        the out writer object
     * @return the modified ClientLoader instance
     * @see BackendClient#export(String, String) */
    public ClientLoader outWriter(final PrintWriter theOutWriter) {
        this.outWriter = checkNotNull(theOutWriter, "outWriter is null");
        return this;
    }

    /** @return the source dB info */
    public DBInfo sourceDB() {
        return sourceDB;
    }

    /** @return the target dB info */
    @CheckForNull
    public DBInfo targetDB() {
        return targetDB;
    }

    /** Adds Target db settings to the loader, used by.
     * 
     * @param theTargetDB
     *        the target db settings
     * @return the modified ClientLoader instance
     * @see BackendClient#transferData(String, String, boolean) */
    public ClientLoader targetDB(final DBInfo theTargetDB) {
        this.targetDB = checkNotNull(theTargetDB, "targetDB is null");
        return this;
    }

    /** @return verbose flag */
    public boolean verbose() {
        return verbose;
    }

    /** Whether to print verbose information.
     * 
     * @param theVerbose
     *        the verbose flag
     * @return the modified client loader instance */
    public ClientLoader verbose(final boolean theVerbose) {
        this.verbose = theVerbose;
        return this;
    }

    /** @return stream to print verbose information. (System.out/System.err etc) */
    @CheckForNull
    public PrintStream verboseStream() {
        return verboseStream;
    }

    /** Stream to print verbose information. (System.out/System.err etc)
     * 
     * @param theVerboseStream
     *        the verbose stream
     * @return the modified ClientLoader instance */
    public ClientLoader verboseStream(final PrintStream theVerboseStream) {
        this.verboseStream = theVerboseStream;
        return this;
    }
}
