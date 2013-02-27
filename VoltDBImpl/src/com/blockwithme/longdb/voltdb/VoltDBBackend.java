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
package com.blockwithme.longdb.voltdb;

import java.util.Map;

import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;

import com.blockwithme.longdb.base.AbstractBackend;
import com.blockwithme.longdb.exception.DBException;
import com.google.inject.Inject;

/** Implementation of a VoltDB-backed backend. */
@ParametersAreNonnullByDefault
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "CD_CIRCULAR_DEPENDENCY", justification = "This is our base class design")
public class VoltDBBackend extends
        AbstractBackend<VoltDBBackend, VoltDatabase, VoltDBTable> {

    /** Logger for this class */
    private static final Logger LOG = LoggerFactory
            .getLogger(VoltDBBackend.class);

    /** The client. */
    private org.voltdb.client.Client client;

    /** The config. */
    private final VoltDBConfig config;

    /** Creates a Voltdb Backend instance with url and properties supplied as
     * parameters. Properties supplied as parameters will overwrite the
     * properties loaded from VoltConfig.properties configuration file
     * 
     * @param theConfig
     *        configuration bean injected by Guice. */
    @Inject
    public VoltDBBackend(final VoltDBConfig theConfig) {
        this.config = theConfig;
        connect();
    }

    /** establish connection using the voltdb Configurations */
    private void connect() {
        try {

            final String hostname = config.hostName();
            final ClientConfig voltConfg = new ClientConfig();

            LOG.debug("Starting voltdb client.");
            LOG.debug("Auto tune - " + config.autoTune());
            LOG.debug("Connection timeout - "
                    + config.connectionResponseTimeout());
            LOG.debug("Transaction per second - "
                    + config.maxTransactionsPerSecond());
            LOG.debug("Max outstanding txns - " + config.maxOutstandingTxns());
            LOG.debug("Procedure call timeout - "
                    + config.procedureCallTimeout());

            voltConfg.setHeavyweight(config.heavyweight());

            if (config.autoTune())
                voltConfg.enableAutoTune();
            voltConfg.setConnectionResponseTimeout(config
                    .connectionResponseTimeout());
            voltConfg.setMaxTransactionsPerSecond(config
                    .maxTransactionsPerSecond());
            voltConfg.setMaxOutstandingTxns(config.maxOutstandingTxns());
            voltConfg.setProcedureCallTimeout(config.procedureCallTimeout());

            client = ClientFactory.createClient(voltConfg);
            client.createConnection(hostname);
            client.configureBlocking(true);

        } catch (final Exception e) {
            LOG.error("Exception Occurred in - connect()", e);
            throw new DBException("Error creating connection.", e);
        }
    }

    /** Client Handle to be used by related classes.
     * 
     * @return the client */
    protected Client client() {
        return client;
    }

    /** Throws UnsupportedOperationException, Voltdb implementation currently
     * doesn't support dynamic creation on schemas and tables.
     * 
     * @param theDBName
     *        the database name
     * @return the voltdb database */
    @Override
    protected VoltDatabase createDatabaseInternal(final String theDBName) {
        throw new UnsupportedOperationException(
                "This operation is not supported for VoltDB");
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.blockwithme.longdb.base.AbstractBackend#dropInternal(com.paintedboxes
     * .db.base.AbstractDatabase)
     */
    @Override
    protected void dropInternal(final VoltDatabase theDB) {
        throw new UnsupportedOperationException(
                "This operation is not supported for VoltDB");

    }

    /** The 'databases' map is initialized with 'default' database. There is only
     * one pre-created 'database' in this implementation Voltdb Implementation
     * currently doesn't support dynamic creation on schemas and tables.
     * 
     * @param theDatabase
     *        the databases map */
    @Override
    protected void openInternal(final Map<String, VoltDatabase> theDatabase) {
        theDatabase.clear();
        final VoltDatabase db = new VoltDatabase(this);
        theDatabase.put("default", db);
    }

    /** Closes Backend client connection. */
    @Override
    protected void shutdownInternal() {
        // try {
        // client.close();
        // } catch (final InterruptedException e) {
        // throwThis(e, "Error closing client connection.");
        // }
    }

}
