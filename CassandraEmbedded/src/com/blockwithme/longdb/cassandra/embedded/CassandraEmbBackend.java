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
package com.blockwithme.longdb.cassandra.embedded;

import java.io.File;
import java.util.List;
import java.util.Map;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.cassandra.thrift.CassandraServer;
import org.apache.cassandra.thrift.KsDef;

import com.blockwithme.longdb.base.AbstractBackend;
import com.blockwithme.longdb.cassandra.util.CassandraDeamonService;
import com.blockwithme.longdb.exception.DBException;
import com.blockwithme.longdb.util.ConfigUtil;
import com.google.inject.Inject;

/** Implementation for Cassandra Embedded Backend. This implementation bypasses
 * the Socket communication and directly uses the APIs available in
 * org.apache.cassandra.thrift.CassandraServer class. A Daemon Cassandra service
 * runs inside the current JVM. */
@ParametersAreNonnullByDefault
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "CD_CIRCULAR_DEPENDENCY", justification = "This is our base class design")
public class CassandraEmbBackend
        extends
        AbstractBackend<CassandraEmbBackend, CassandraEmbDatabase, CassandraEmbTable> {

    /** The embedded cassandra configuration object. */
    private final CassandraEmbConfig config;

    /** The server server instance represents connection to the embedded
     * cassandra */
    private final CassandraServer server;

    /** Embedded Cassandra Daemon services will be launched in the current JVM. */
    private CassandraDeamonService service;

    /** Internal constructor, creates a cassandra backend instance with url and
     * properties supplied as parameters. Properties supplied as parameters will
     * overwrite the properties loaded from CassandraConfig.properties
     * configuration file
     * 
     * @param theConfig
     *        the configuration bean */
    @Inject
    protected CassandraEmbBackend(final CassandraEmbConfig theConfig) {
        this.config = theConfig;
        startCluster();
        server = new CassandraServer();
    }

    /** Start cluster. */
    private void startCluster() {
        // get path for cassandra yaml file.
        final String confPath = ConfigUtil.rootFolderPath() + File.separator
                + "conf" + File.separator + "cassandra.yaml";
        try {
            // launch Cassandra daemon service, inside the current JVM.
            service = CassandraDeamonService.getSingletonObject(confPath);
        } catch (final Exception e) {
            throw new DBException("Error Starting Embeded Cassandra: "
                    + confPath, e);
        }
    }

    /** Creates a new keyspace in the current cassandra cluster.
     * 
     * @param theDB
     *        the database
     * @return the cassandra embedded database */
    @Override
    protected CassandraEmbDatabase createDatabaseInternal(final String theDB) {
        return new CassandraEmbDatabase(this, theDB);
    }

    /** Drops the Keyspace corresponding to the database instance.
     * 
     * @param theDB
     *        the dropped */
    @Override
    protected void dropInternal(final CassandraEmbDatabase theDB) {

        try {
            server.system_drop_keyspace(theDB.database());
        } catch (final Exception e) {
            throw new DBException("Error While dropping Database: " + theDB, e);
        }
    }

    /** Finds all the Keyspaces (keyspace maps to a BEDatabase) in the current
     * Cassandra cluster, then method populates 'databases' map.
     * 
     * @param theDatabase
     *        the databases */
    @Override
    protected void openInternal(
            final Map<String, CassandraEmbDatabase> theDatabase) {
        // empty the table and query complete list of keyspaces from DB.
        theDatabase.clear();
        final List<KsDef> ksDefs;
        try {
            ksDefs = server.describe_keyspaces();
            for (final KsDef keyspaceDefinition : ksDefs) {
                final CassandraEmbDatabase db = new CassandraEmbDatabase(this,
                        keyspaceDefinition);
                theDatabase.put(keyspaceDefinition.getName(), db);
            }
        } catch (final Exception e) {
            throw new DBException("Error Opening Database.", e);
        }
    }

    /** @return the Cassandra server instance */
    protected CassandraServer server() {
        return server;
    }

    /** Stops Cassandra Daemon service. */
    @Override
    protected void shutdownInternal() {
        service.stop();
    }

    /** @return Configuration Bean. */
    public CassandraEmbConfig config() {
        return config;
    }

}
