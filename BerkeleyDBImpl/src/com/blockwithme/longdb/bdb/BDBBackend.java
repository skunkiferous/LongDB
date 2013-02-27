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
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

// TODO: Auto-generated Javadoc
/** BerkeleyDB Backend Implementation */
@ParametersAreNonnullByDefault
public class BDBBackend extends
        AbstractBackend<BDBBackend, BDBDatabase, BDBTable> {
    /** Logger for this class */
    private static final Logger LOG = LoggerFactory.getLogger(BDBBackend.class);

    /** The config bean. */
    private final BDBConfig config;

    /** Internal constructor, creates Backend instance, configurations are read
     * from BDBConfig.properties
     * 
     * @param theConfig Configuration bean class that provides all the
     *        configuration properties for the instantiation */
    @Inject
    protected BDBBackend(final BDBConfig theConfig) {
        this.config = theConfig;
    }

    /** creates connect to BDB database. */
    private Environment connect(final String theFileName) {
        try {
            String filePath = config.filePath();
            if (!filePath.endsWith(File.separator))
                filePath += File.separator;
            filePath += theFileName;

            final EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setAllowCreate(true);
            envConfig.setTransactional(config.transactional());
            envConfig.setCacheSize(config.cacheSize());
            envConfig.setLocking(config.locking());
            // TODO following method works with BDB 5.0.X.
            // un-comment following line when the jar is available in any of the
            // maven repos
            // envConfig.setCacheMode(CM);
            final Environment dbEnvironment = new Environment(
                    new File(filePath), envConfig);
            return dbEnvironment;

        } catch (final Exception e) {
            LOG.error("Exception Occurred in - " + "connect(String fileName="
                    + theFileName + ")", e);
            throw new DBException("Error creating connection.", e);
        }
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.base.AbstractBackend#createDatabaseInternal(java.
     * lang.String) */
    @Override
    protected BDBDatabase createDatabaseInternal(final String theDB) {
        // TODO - validate that database name should not contain any special
        // chars.
        try {
            final File dataRoot = new File(config.filePath());
            if (!dataRoot.exists())
                FileUtils.forceMkdir(dataRoot);
            final File databaseDir = new File(dataRoot.getPath()
                    + File.separator + theDB);
            if (databaseDir.exists())
                throw new DBException("Database already exists");

            FileUtils.forceMkdir(databaseDir);
            final BDBDatabase bdb = new BDBDatabase(this, this.connect(theDB),
                    theDB);
            return bdb;
        } catch (final IOException e) {
            LOG.error("Exception Occurred in - "
                    + "createDatabaseInternal(String database=" + theDB + ")",
                    e);
            throw new DBException("Error creating connection.", e);
        }
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.base.AbstractBackend#dropInternal(com.blockwithme
     * .longdb.base.AbstractDatabase) */
    @Override
    protected void dropInternal(final BDBDatabase theDB) {
        try {
            theDB.closeInternal();
            final String rootFolder = config.filePath();
            final File databaseDir = new File(rootFolder + File.separator
                    + theDB.database());
            if (!databaseDir.exists())
                LOG.warn("Database " + theDB + " does't exist");
            else
                FileUtils.forceDelete(databaseDir);
        } catch (final IOException e) {
            LOG.error("Exception Occurred in "
                    + "- dropInternal(BDBDatabase dropped=" + theDB + ")", e);
            throw new DBException(
                    "Error dropping connection:" + e.getMessage(), e);
        }
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.base.AbstractBackend#openInternal(java.util.Map) */
    @Override
    protected void openInternal(final Map<String, BDBDatabase> theDatabase) {
        theDatabase.clear();
        try {
            final File dataRoot = new File(config.filePath());
            if (!dataRoot.exists())
                FileUtils.forceMkdir(dataRoot);
            final String[] dataDirectories = dataRoot
                    .list(DirectoryFileFilter.INSTANCE);
            for (final String dir : dataDirectories) {
                final BDBDatabase bdb = new BDBDatabase(this,
                        this.connect(dir), dir);
                theDatabase.put(dir.toLowerCase(), bdb);
            }
        } catch (final Exception e) {
            LOG.error("Exception Occurred in - "
                    + "openInternal(Map<String,BDBDatabase> databases="
                    + theDatabase + ")", e);

            throw new DBException("Error creating connection.", e);
        }
    }

    /* (non-Javadoc)
     * @see com.blockwithme.longdb.base.AbstractBackend#shutdownInternal() */
    @Override
    protected void shutdownInternal() {
        // TODO Really? Nothing to do?
        // NOP.
    }

    /** Returns configurations currently applicable.
     * 
     * @return current configuration settings. */
    public BDBConfig config() {
        return config;
    }

}
