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
package com.blockwithme.longdb.h2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import javax.annotation.ParametersAreNonnullByDefault;

import com.blockwithme.longdb.base.AbstractBackend;
import com.blockwithme.longdb.exception.DBException;
import com.blockwithme.longdb.h2.config.SQLUtil;
import com.google.inject.Inject;

// TODO: Auto-generated Javadoc
/** Implementation of a VoltDB-backed backend. */
@ParametersAreNonnullByDefault
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "CD_CIRCULAR_DEPENDENCY", justification = "This is our base class design")
public class H2Backend extends AbstractBackend<H2Backend, H2Database, H2Table> {
    /** The config. */
    private final H2Config config;

    /** The connection object. */
    private Connection conn;

    /** Creates a H2 backend instance with url and properties supplied as
     * parameters. Properties supplied as parameters will overwrite the
     * properties loaded from H2Config.properties configuration file
     * 
     * @param theConfig configuration bean. */
    @Inject
    public H2Backend(final H2Config theConfig) {
        this.config = theConfig;
        connect();
    }

    /** Creates JDBC connection */
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "DMI_EMPTY_DB_PASSWORD")
    private void connect() {
        try {
            Class.forName("org.h2.Driver");
            conn = DriverManager.getConnection("jdbc:h2:" + config.dbFile(),
                    "", ""); // $codepro.audit.disable
                             // com.instantiations.assist.eclipse.analysis.hardcodedPassword
        } catch (final Exception e) {
            throw new DBException("Error creating connection: "
                    + config.dbFile(), e);
        }
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.base.AbstractBackend#createDatabaseInternal(java.
     * lang.String) */
    @Override
    protected H2Database createDatabaseInternal(final String theDBName) {
        SQLUtil.executeStatement("CREATE SCHEMA " + theDBName, conn);
        return new H2Database(this, theDBName);
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.base.AbstractBackend#dropInternal(com.blockwithme
     * .longdb.base.AbstractDatabase) */
    @Override
    protected void dropInternal(final H2Database theDB) {
        SQLUtil.executeStatement("DROP SCHEMA " + theDB.database(), conn);
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.base.AbstractBackend#openInternal(java.util.Map) */
    @Override
    protected void openInternal(final Map<String, H2Database> theDatabase) {
        theDatabase.clear();
        final List<String> results = SQLUtil.getResultsAsList(
                "SELECT * FROM INFORMATION_SCHEMA.SCHEMATA", conn,
                "SCHEMA_NAME");
        if (results != null)
            for (String sName : results) {
                sName = sName.toLowerCase();
                if ("PUBLIC".equalsIgnoreCase(sName)
                        || "INFORMATION_SCHEMA".equalsIgnoreCase(sName))
                    continue;
                theDatabase.put(sName, new H2Database(this, sName));
            }
    }

    /* (non-Javadoc)
     * @see com.blockwithme.longdb.base.AbstractBackend#shutdownInternal() */
    @Override
    protected void shutdownInternal() {
        try {
            conn.close(); // $codepro.audit.disable closeInFinally
        } catch (final SQLException e) {
            throw new DBException("Error closing client connection.", e);
        }
    }

    /** Connection.
     * 
     * @return the connection */
    public Connection connection() {
        return conn;
    }

}
