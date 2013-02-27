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
// $codepro.audit.disable noPublicMembers
package com.blockwithme.longdb.cassandra;

import java.util.List;
import java.util.Map;

import javax.annotation.ParametersAreNonnullByDefault;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import com.blockwithme.longdb.base.AbstractBackend;
import com.google.inject.Inject;

/** Implementation of a Cassandra Backend, this implementation uses Hector API
 * for communication with Cassandra Database. */
@ParametersAreNonnullByDefault
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "CD_CIRCULAR_DEPENDENCY", justification = "This is our base class design")
public class CassandraBackend extends
        AbstractBackend<CassandraBackend, CassandraDatabase, CassandraTable> {

    /** The cassandra cluster object. */
    private Cluster cluster;

    /** The cassandra configuration object. */
    private final CassandraConfig config;

    /** Creates a cassandra backend instance with url and properties supplied as
     * parameters. Properties supplied as parameters are will overwrite the
     * properties loaded from CassandraConfig.properties configuration file
     * 
     * @param theConfig
     *        configuration bean injected by Guice */
    @Inject
    CassandraBackend(final CassandraConfig theConfig) {
        this.config = theConfig;
        startCluster();
    }

    /** Start cluster. */
    private void startCluster() {
        cluster = HFactory.getOrCreateCluster(config.clusterName(),
                config.dbUrl());
    }

    /** returns cassandra cluster object to be referenced by related classes.
     * 
     * @return the cluster */
    protected Cluster cluster() {
        return cluster;
    }

    /** current cassandra configuration, to be referenced by related classes.
     * 
     * @return the configuration bean */
    protected CassandraConfig config() {
        return config;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.blockwithme.longdb.base.AbstractBackend#createDatabaseInternal(java.
     * lang.String)
     */
    @Override
    protected CassandraDatabase createDatabaseInternal(final String theDBName) {
        return new CassandraDatabase(this, theDBName);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.blockwithme.longdb.base.AbstractBackend#dropInternal(com.paintedboxes
     * .db.base.AbstractDatabase)
     */
    @Override
    protected void dropInternal(final CassandraDatabase theDB) {
        cluster.dropKeyspace(theDB.database(), true);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractBackend#openInternal(java.util.Map)
     */
    @Override
    protected void openInternal(final Map<String, CassandraDatabase> theDatabase) {
        // empty the table and query complete list of keyspaces from DB.
        theDatabase.clear();
        final List<KeyspaceDefinition> ksDefs = cluster.describeKeyspaces();
        for (final KeyspaceDefinition keyspaceDefinition : ksDefs) {
            final CassandraDatabase db = new CassandraDatabase(this,
                    keyspaceDefinition);
            theDatabase.put(keyspaceDefinition.getName(), db);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractBackend#shutdownInternal()
     */
    @Override
    protected void shutdownInternal() {
        // NOP
    }
}
