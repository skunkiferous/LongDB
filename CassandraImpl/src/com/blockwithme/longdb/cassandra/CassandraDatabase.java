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
package com.blockwithme.longdb.cassandra;

import static com.blockwithme.longdb.cassandra.CassandraConstants.REVERSE_COLUMN_COMPARATOR;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.ParametersAreNonnullByDefault;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.service.FailoverPolicy;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import com.blockwithme.longdb.BETableProfile;
import com.blockwithme.longdb.base.AbstractDatabase;
import com.blockwithme.longdb.entities.Base36;

// TODO: Auto-generated Javadoc
/** Implementation of a Cassandra BEDatabase. */
@ParametersAreNonnullByDefault
public class CassandraDatabase extends
        AbstractDatabase<CassandraBackend, CassandraDatabase, CassandraTable> {

    /** The key space definition */
    private final KeyspaceDefinition ks;

    /** This constructor is called for initializing existing keyspace
     * 
     * @param theBackend the backend instance
     * @param theKeySpace the corresponding keyspace */
    protected CassandraDatabase(final CassandraBackend theBackend,
            final KeyspaceDefinition theKeySpace) {
        super(theBackend, theKeySpace.getName());
        this.ks = theKeySpace;
    }

    /** This constructor is called for creating new KeySpace
     * 
     * @param theBackend the backend instance
     * @param theDBName the database instance */
    protected CassandraDatabase(final CassandraBackend theBackend,
            final String theDBName) {

        super(theBackend, theDBName);
        final KeyspaceDefinition keyspaceDefinition = createKSDefinition(
                theBackend, theDBName);
        theBackend.cluster().addKeyspace(keyspaceDefinition);
        final ConfigurableConsistencyLevel consistencyLevelPolicy = createConsistencyPolicy();
        final FailoverPolicy failoverPolicy = createFailoverPolicy();
        final Map<String, String> credentials = createCredentials();

        HFactory.createKeyspace(theDBName, theBackend.cluster(),
                consistencyLevelPolicy, failoverPolicy, credentials);
        ks = theBackend.cluster().describeKeyspace(theDBName);
    }

    /** Returns the cassandra config */
    private CassandraConfig config() {
        return backend.config();
    }

    /** Creates the consistency policy.
     * 
     * @return the configurable consistency level */
    private ConfigurableConsistencyLevel createConsistencyPolicy() {
        final ConfigurableConsistencyLevel consistencyLevelPolicy = new ConfigurableConsistencyLevel();
        final HConsistencyLevel readConsistencyLevel = HConsistencyLevel
                .valueOf(config().readConsistencyLevel());
        final HConsistencyLevel writeConsistencyLevel = HConsistencyLevel
                .valueOf(config().writeConsistencyLevel());
        consistencyLevelPolicy
                .setDefaultReadConsistencyLevel(readConsistencyLevel);
        consistencyLevelPolicy
                .setDefaultWriteConsistencyLevel(writeConsistencyLevel);
        return consistencyLevelPolicy;
    }

    /** Creates the credentials. */
    @CheckForNull
    private Map<String, String> createCredentials() {
        Map<String, String> credentials = null;
        if (config().userName() != null && config().password() != null) {
            credentials = new HashMap<String, String>();
            credentials.put(config().userName(), config().password());
        }
        return credentials;
    }

    /** Creates the failover policy.
     * 
     * @return the failover policy */
    private FailoverPolicy createFailoverPolicy() {
        final FailoverPolicy failoverPolicy = new FailoverPolicy(config()
                .numRetries(), config().sleepBwHostsMilli());
        return failoverPolicy;
    }

    /** Creates the key space definition.
     * 
     * @param theBackend the backend
     * @param theDatabase the database
     * @return the keyspace definition */
    private KeyspaceDefinition createKSDefinition(
            final CassandraBackend theBackend, final String theDatabase) {
        final KeyspaceDefinition keyspaceDefinition = HFactory
                .createKeyspaceDefinition(theDatabase,
                        config().strategyClass(), config().replicationFactor(),
                        null);
        return keyspaceDefinition;
    }

    /* (non-Javadoc)
     * @see com.blockwithme.longdb.base.AbstractDatabase#closeInternal() */
    @Override
    protected void closeInternal() {
        // NOP
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.base.AbstractDatabase#createInternal(com.blockwithme
     * .longdb.entities.Base36, com.blockwithme.longdb.BETableProfile) */
    @Override
    protected CassandraTable createInternal(final Base36 theTable,
            final BETableProfile theProfile) {
        return new CassandraTable(this, theTable,
                theProfile.reverseColumnsOrder(), false);
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.base.AbstractDatabase#dropInternal(com.blockwithme
     * .longdb.base.AbstractTable) */
    @Override
    protected void dropInternal(final CassandraTable theTable) {
        theTable.dropped();
    }

    /* Gets the list of column families in the current keyspace and populate
     * 'tables' map (non-Javadoc)
     * @see
     * com.blockwithme.longdb.base.AbstractDatabase#openInternal(java.util.Map) */
    @Override
    protected void openInternal(final Map<Base36, CassandraTable> theTables) {
        theTables.clear();
        final KeyspaceDefinition keyspaceDef = backend.cluster()
                .describeKeyspace(ks.getName());
        final List<ColumnFamilyDefinition> cfDefs = keyspaceDef.getCfDefs();
        for (final ColumnFamilyDefinition def : cfDefs) {
            // TODO value of detectCollisions is hardcoded.
            boolean reverse = false;
            final String cmp = def.getComparatorType().getClass().getName();
            if (cmp.equals(REVERSE_COLUMN_COMPARATOR))
                reverse = true;
            final CassandraTable table = new CassandraTable(this, def, reverse,
                    false);
            theTables.put(Base36.get(def.getName()), table);
        }
    }
}
