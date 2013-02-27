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
package com.blockwithme.longdb.leveldb;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blockwithme.longdb.base.AbstractBackend;
import com.blockwithme.longdb.exception.DBException;
import com.google.inject.Inject;

// TODO: Auto-generated Javadoc
/** This implementation is based on LevelDB Java implementation.
 * https://github.com/dain/leveldb */
@ParametersAreNonnullByDefault
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "CD_CIRCULAR_DEPENDENCY", justification = "This is our base class design")
public class LevelDBBackend extends
        AbstractBackend<LevelDBBackend, LevelDBDatabase, LevelDBTable> {
    /** Logger for this class */
    private static final Logger LOG = LoggerFactory
            .getLogger(LevelDBBackend.class);

    /** The config. */
    private final LevelDBConfig config;

    /** The root folder. */
    private final String rootFolder;

    /** Creates a Backend instance with url and properties supplied as
     * parameters. Properties supplied as parameters will overwrite the
     * properties loaded from LevelDBConfig.properties file
     * 
     * @param theConfig configuration bean injected by Guice */
    @Inject
    public LevelDBBackend(final LevelDBConfig theConfig) {
        this.config = theConfig;
        rootFolder = theConfig.filePath();
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.base.AbstractBackend#createDatabaseInternal(java.
     * lang.String) */
    @Override
    protected LevelDBDatabase createDatabaseInternal(final String theDBName) {
        try {
            final File dataRoot = new File(rootFolder);
            if (!dataRoot.exists())
                FileUtils.forceMkdir(dataRoot);
            final File databaseDir = new File(dataRoot.getAbsolutePath()
                    + File.separator + theDBName);
            FileUtils.forceMkdir(databaseDir);
            final LevelDBDatabase bdb = new LevelDBDatabase(this, databaseDir,
                    theDBName);
            return bdb;
        } catch (final IOException e) {
            throw new DBException("Error creating connection: " + theDBName, e);
        }
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.base.AbstractBackend#dropInternal(com.blockwithme
     * .longdb.base.AbstractDatabase) */
    @Override
    protected void dropInternal(final LevelDBDatabase theDB) {
        try {
            theDB.close(); // $codepro.audit.disable closeInFinally
            final File databaseDir = new File(rootFolder + File.separator
                    + theDB.database());
            if (databaseDir.exists())
                Util.forceDrop(databaseDir);
            try {
                FileUtils.deleteDirectory(databaseDir);
            } catch (final Exception e) {
                LOG.warn(
                        "exception ignored - dropInternal(LevelDBDatabase dropped="
                                + theDB + ")", e);

                // ignore.
            }
        } catch (final Exception e) {
            throw new DBException("Error creating connection: " + theDB, e);
        }
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.base.AbstractBackend#openInternal(java.util.Map) */
    @Override
    protected void openInternal(final Map<String, LevelDBDatabase> theDatabase) {
        theDatabase.clear();
        try {
            final File dataRoot = new File(rootFolder);
            if (!dataRoot.exists())
                FileUtils.forceMkdir(dataRoot);
            final String[] dataDirs = dataRoot
                    .list(DirectoryFileFilter.INSTANCE);

            if (dataDirs != null && dataDirs.length > 0) {
                for (final String dir : dataDirs) {
                    final String fullPath = dataRoot.getAbsolutePath()
                            + File.separator + dir;
                    final LevelDBDatabase bdb = new LevelDBDatabase(this,
                            new File(fullPath), dir);
                    theDatabase.put(dir.toLowerCase(), bdb);
                }
            }
        } catch (final Exception e) {
            throw new DBException("Error creating connection: " + theDatabase,
                    e);
        }
    }

    /* (non-Javadoc)
     * @see com.blockwithme.longdb.base.AbstractBackend#shutdownInternal() */
    @Override
    protected void shutdownInternal() {
        // NOP
    }

    /** Returns the Current configurations.
     * 
     * @return the configuration bean */
    public LevelDBConfig config() {
        return config;
    }
}
