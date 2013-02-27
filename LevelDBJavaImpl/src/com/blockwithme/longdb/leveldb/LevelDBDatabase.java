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
// $codepro.audit.disable com.instantiations.eclipse.analysis.audit.security.incompatibleTypesStoredInACollection
package com.blockwithme.longdb.leveldb;

import java.io.File;
import java.util.Map;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blockwithme.longdb.BETableProfile;
import com.blockwithme.longdb.base.AbstractDatabase;
import com.blockwithme.longdb.entities.Base36;
import com.blockwithme.longdb.exception.DBException;

// TODO: Auto-generated Javadoc
/** The Class LevelDBDatabase. */
@ParametersAreNonnullByDefault
public class LevelDBDatabase extends
        AbstractDatabase<LevelDBBackend, LevelDBDatabase, LevelDBTable> {
    /** Logger for this class */
    private static final Logger LOG = LoggerFactory
            .getLogger(LevelDBDatabase.class);

    /** The db folder. */
    private final File dbFolder;

    /** Instantiates a new level db database.
     * 
     * @param theBackend the backend
     * @param theDBFolder the db folder
     * @param theDBName the db name */
    protected LevelDBDatabase(final LevelDBBackend theBackend,
            final File theDBFolder, final String theDBName) {
        super(theBackend, theDBName);
        this.dbFolder = theDBFolder;
    }

    /** Gets the dir.
     * 
     * @param theName the name
     * @return the dir */
    private File getDir(final String theName) {
        final File tableDir = new File(dbFolder.getAbsolutePath() // $codepro.audit.disable
                                                                  // com.instantiations.assist.eclipse.analysis.pathManipulation
                + File.separator + theName);
        return tableDir;
    }

    /* (non-Javadoc)
     * @see com.blockwithme.longdb.base.AbstractDatabase#closeInternal() */
    @Override
    protected void closeInternal() {
        for (final Base36 tableName : tables.keySet()) {
            final LevelDBTable table = tables.get(tableName);
            table.closeInternal();
        }
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.base.AbstractDatabase#createInternal(com.blockwithme
     * .longdb.entities.Base36, com.blockwithme.longdb.BETableProfile) */
    @Override
    protected LevelDBTable createInternal(final Base36 theTable,
            final BETableProfile theProfile) {
        try {
            final File databaseDir = getDir(theTable.toFixedString());
            FileUtils.forceMkdir(databaseDir);
            final LevelDBTable tableInstance = new LevelDBTable(this,
                    databaseDir, theTable, false);
            return tableInstance;
        } catch (final Exception e) {
            LOG.error(
                    "Exception Occurred in - createInternal(Base36 table="
                            + theTable + ", BETableProfile profile="
                            + theProfile + ")", e);
            throw new DBException("Error creating table.", e);
        }
    }

    /** Drop all tables. */
    protected void dropAllTables() {
        for (final Base36 tableName : tables.keySet()) {
            final LevelDBTable table = tables.get(tableName);
            dropInternal(table);
        }
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.base.AbstractDatabase#dropInternal(com.blockwithme
     * .longdb.base.AbstractTable) */
    @Override
    protected void dropInternal(final LevelDBTable theTable) {
        try {
            theTable.dropped();
            final File tableDir = getDir(theTable.table().toFixedString());
            if (tableDir.exists())
                Util.forceDrop(tableDir);
            try {
                FileUtils.deleteDirectory(tableDir);
            } catch (final Exception e) {
                LOG.warn("exception ignored - dropInternal(LevelDBTable table="
                        + theTable + ")");
                // ignore.
            }
        } catch (final Exception e) {
            LOG.error(
                    "Exception Occurred in - dropInternal(LevelDBTable table="
                            + theTable + ")", e);
            throw new DBException("Error dropping table files", e);
        }
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.base.AbstractDatabase#openInternal(java.util.Map) */
    @Override
    protected void openInternal(final Map<Base36, LevelDBTable> theTables) {
        theTables.clear();
        final String[] dataDirs = dbFolder.list(DirectoryFileFilter.INSTANCE);

        if (dataDirs != null) {
            for (final String dir : dataDirs) {
                final File path = getDir(dir);
                final LevelDBTable table = new LevelDBTable(this, path,
                        Base36.get(dir), false);
                theTables.put(Base36.get(dir.toLowerCase()), table);
            }
        }
    }

}
