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

import static com.blockwithme.longdb.cassandra.embedded.CassandraEmbConstants.REVERSE_COLUMN_COMPARATOR;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;

import com.blockwithme.longdb.BETableProfile;
import com.blockwithme.longdb.base.AbstractDatabase;
import com.blockwithme.longdb.entities.Base36;
import com.blockwithme.longdb.exception.DBException;

// TODO: Auto-generated Javadoc
/** Implementation of a Cassandra BEDatabase. */
@ParametersAreNonnullByDefault
public class CassandraEmbDatabase
        extends
        AbstractDatabase<CassandraEmbBackend, CassandraEmbDatabase, CassandraEmbTable> {
    /** The cassandra keyspace definition. */
    private final KsDef ks;

    /** Instantiates a new cassandra embedded database.
     * 
     * @param theBackend the backend object
     * @param theKSDef the keyspace definition */
    protected CassandraEmbDatabase(final CassandraEmbBackend theBackend,
            final KsDef theKSDef) {
        super(theBackend, theKSDef.getName());
        ks = theKSDef;
    }

    /** Instantiates a new cassandra embedded database.
     * 
     * @param theBackend the backend object
     * @param theDBName the database name */
    protected CassandraEmbDatabase(final CassandraEmbBackend theBackend,
            final String theDBName) {

        super(theBackend, theDBName);

        try {
            final KsDef keyspaceDefinition = createKSDefinition(theBackend,
                    theDBName);
            // TODO This API throws a null pointer exception if
            // we create a blank keyspace (without any Column Family.)
            // Added a dummy column family for to 'make it work'
            // Need to find what should be the long term resolution.

            final CfDef cfDef = new CfDef(theDBName, "DummyTable");
            final List<CfDef> cfdefs = new ArrayList<CfDef>();
            cfdefs.add(cfDef);
            keyspaceDefinition.setCf_defs(cfdefs);
            theBackend.server().system_add_keyspace(keyspaceDefinition);
            ks = theBackend.server().describe_keyspace(theDBName);
        } catch (final Exception e) {
            throw new DBException("Error Creating Database: " + theDBName, e);
        }
    }

    /** Creates the key space definition.
     * 
     * @param theBackend the backend object
     * @param theDBName the database name
     * @return the key space definition */
    @SuppressWarnings("deprecation")
    private KsDef createKSDefinition(final CassandraEmbBackend theBackend,
            final String theDBName) {

        try {
            final int replicationFactor = Integer.valueOf(theBackend.config()
                    .replicationFactor());
            final KsDef keyspaceDefinition = new KsDef();
            keyspaceDefinition.setName(theDBName);
            keyspaceDefinition.setReplication_factor(replicationFactor); // $codepro.audit.disable
                                                                         // deprecatedMethod
            keyspaceDefinition.setStrategy_class(theBackend.config()
                    .strategyClass());
            return keyspaceDefinition;
        } catch (final NumberFormatException e) {
            throw new DBException("Error Creating Database: " + theDBName, e);
        }
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
    protected CassandraEmbTable createInternal(final Base36 theTable,
            final BETableProfile theProfile) {
        return new CassandraEmbTable(this, theTable,
                theProfile.reverseColumnsOrder(), false);
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.base.AbstractDatabase#dropInternal(com.blockwithme
     * .longdb.base.AbstractTable) */
    @Override
    protected void dropInternal(final CassandraEmbTable theTable) {
        theTable.dropped();
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.base.AbstractDatabase#openInternal(java.util.Map) */
    @Override
    protected void openInternal(final Map<Base36, CassandraEmbTable> theTables) {
        theTables.clear();
        try {
            backend.server().set_keyspace(ks.getName());
            final KsDef keyspaceDef = backend.server().describe_keyspace(
                    ks.getName());
            final Iterator<CfDef> cfItr = keyspaceDef.getCf_defsIterator();
            if (cfItr != null) {
                while (cfItr.hasNext()) {
                    final CfDef def = cfItr.next();
                    // TODO value of detectCollisions is hardcoded.
                    boolean reverse = false;
                    final String cmp = def.getComparator_type();
                    if (cmp.equals(REVERSE_COLUMN_COMPARATOR))
                        reverse = true;
                    final CassandraEmbTable table = new CassandraEmbTable(this,
                            def, reverse, false);
                    theTables.put(Base36.get(def.getName()), table);
                }
            }
        } catch (final Exception e) {
            throw new DBException("Error While loading Database: " + ks, e);
        }
    }
}
