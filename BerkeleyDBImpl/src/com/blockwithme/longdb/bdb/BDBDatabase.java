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
package com.blockwithme.longdb.bdb;

import java.util.List;
import java.util.Map;

import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blockwithme.longdb.BETableProfile;
import com.blockwithme.longdb.base.AbstractDatabase;
import com.blockwithme.longdb.entities.Base36;
import com.blockwithme.longdb.exception.DBException;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;

// TODO: Auto-generated Javadoc
/** The BEDatabase implementation for BerkeleyDB. */
@ParametersAreNonnullByDefault
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "CD_CIRCULAR_DEPENDENCY")
public class BDBDatabase extends
        AbstractDatabase<BDBBackend, BDBDatabase, BDBTable> {
    /** Logger for this class */
    private static final Logger LOG = LoggerFactory
            .getLogger(BDBDatabase.class);

    /** DB Environment Represents the Database entity */
    private final Environment dbEnvironment;

    /** Instantiates a new bDB database.
     * 
     * @param theBackend the Backend instance
     * @param theDBEnv the Environment object
     * @param theDBName the database name */
    protected BDBDatabase(final BDBBackend theBackend,
            final Environment theDBEnv, final String theDBName) {
        super(theBackend, theDBName);
        dbEnvironment = theDBEnv;
    }

    /** BDB environment object handle represent an open connection to BDB
     * database, to be used by related classes.
     * 
     * @return the environment object */
    Environment env() {
        return dbEnvironment;
    }

    /** Closes all the tables and the DB environment. */
    @Override
    protected void closeInternal() {
        try {
            for (final BDBTable t : this.tables.values()) {
                try {
                    t.close();
                } catch (final Exception e) {
                    LOG.warn("exception ignored - closeInternal()", e);
                }
            }
        } finally {
            try {
                dbEnvironment.close();
            } catch (final Exception e) {
                LOG.warn("exception ignored - closeInternal()", e);
                // ignore.
            }
        }
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.base.AbstractDatabase#createInternal(com.blockwithme
     * .longdb.entities.Base36, com.blockwithme.longdb.BETableProfile) */
    @Override
    protected BDBTable createInternal(final Base36 theTable,
            final BETableProfile theProfile) {
        try {
            /* TODO Setting ReverseKeyComparator is allowed, The issue is while
             * fetching Table Meta-data BDB doesn't tell which key comparator
             * was set - workaround could be to find any other meta-data field
             * where this info could be stored.
             * (profile.reverseColumnsOrder)config.setBtreeComparator(
             * ReverseKeyComparator.class); */
            final BDBTable t = new BDBTable(this, theTable.toFixedString(),
                    theProfile.reverseColumnsOrder());
            t.open();
            return t;
        } catch (final Exception e) {
            LOG.error(
                    "Exception Occurred in - createInternal(Base36 table="
                            + theTable + ", BETableProfile profile="
                            + theProfile + ")", e);
            throw new DBException("Error creating tabe.", e);
        }
    }

    /** Db config.
     * 
     * @return the database config */
    protected DatabaseConfig dbConfig() {
        final DatabaseConfig dbConf = new DatabaseConfig();
        dbConf.setTransactional(backend.config().transactional());
        dbConf.setAllowCreate(true);
        dbConf.setBtreeComparator(KeyComparator.class);
        dbConf.setOverrideBtreeComparator(true);
        dbConf.setSortedDuplicates(true);
        return dbConf;
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.base.AbstractDatabase#dropInternal(com.blockwithme
     * .longdb.base.AbstractTable) */
    @Override
    protected void dropInternal(final BDBTable theTable) {
        theTable.dropped();
        final Transaction txn = env().beginTransaction(null,
                TransactionConfig.DEFAULT);
        env().truncateDatabase(txn, theTable.table().toFixedString(), false);
        txn.commit();
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.base.AbstractDatabase#openInternal(java.util.Map) */
    @Override
    protected void openInternal(final Map<Base36, BDBTable> theTables) {
        theTables.clear();
        final List<String> dbNames = dbEnvironment.getDatabaseNames();
        for (final String dbn : dbNames) {
            theTables.put(Base36.get(dbn), new BDBTable(this, dbn, false));
        }
    }
}
