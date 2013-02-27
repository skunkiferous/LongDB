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
// $codepro.audit.disable methodChainLength
package com.blockwithme.longdb.h2;

import static com.blockwithme.longdb.h2.H2Constants.VBINARY_MAX_SIZE;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

import javax.annotation.ParametersAreNonnullByDefault;

import com.blockwithme.longdb.BETableProfile;
import com.blockwithme.longdb.base.AbstractDatabase;
import com.blockwithme.longdb.entities.Base36;
import com.blockwithme.longdb.h2.config.SQLUtil;

/** Implementation of a VoltDB BEDatabase. */
@ParametersAreNonnullByDefault
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "CD_CIRCULAR_DEPENDENCY", justification = "This is our base class design")
public class H2Database extends
        AbstractDatabase<H2Backend, H2Database, H2Table> {

    /** Instantiates a new h2 database.
     * 
     * @param theBackend
     *        the backend
     * @param theDBName
     *        the database name */
    protected H2Database(final H2Backend theBackend, final String theDBName) {
        super(theBackend, theDBName);
    }

    /** Connection.
     * 
     * @return the connection */
    private Connection connection() {
        return ((H2Backend) backend()).connection();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractDatabase#closeInternal()
     */
    @Override
    protected void closeInternal() {
        // NOP
    }

    /** Executes SQL statement for table creation.
     * 
     * @param theTable
     *        the table name
     * @param theProfile
     *        the profile
     * @return the newly created table instance */
    @Override
    protected H2Table createInternal(final Base36 theTable,
            final BETableProfile theProfile) {
        final StringBuilder strBuf = new StringBuilder(); // $codepro.audit.disable
                                                          // unusedStringBuilder
        final String createTable = strBuf.append("CREATE TABLE ")
                .append(this.database()).append('.')
                .append(theTable.toString())
                .append(" ( ROW_KEY BIGINT  NOT NULL,")
                .append(" COLUMN_KEY BIGINT  NOT NULL,")
                .append(" DATA_BLOB VARBINARY(").append(VBINARY_MAX_SIZE)
                .append("), LAST_MODIFIED TIMESTAMP,")
                .append(" CONSTRAINT PK_").append(theTable.toString())
                .append(" PRIMARY KEY (ROW_KEY, COLUMN_KEY)").append(')')
                .toString();
        SQLUtil.executeStatement(createTable, connection());
        return new H2Table(this, theTable);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.blockwithme.longdb.base.AbstractDatabase#dropInternal(com.paintedboxes
     * .db.base.AbstractTable)
     */
    @Override
    protected void dropInternal(final H2Table theTable) {
        final StringBuilder strBuf = new StringBuilder(); // $codepro.audit.disable
                                                          // unusedStringBuilder
        final String createTable = strBuf.append("DROP TABLE ")
                .append(this.database()).append('.')
                .append(theTable.table().toString()).toString();
        SQLUtil.executeStatement(createTable, connection());
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.blockwithme.longdb.base.AbstractDatabase#openInternal(java.util.Map)
     */
    @Override
    protected void openInternal(final Map<Base36, H2Table> theTables) {
        theTables.clear();

        final List<String> results = SQLUtil.getResultsAsList(
                "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_SCHEMA)=UPPER(\'"
                        + this.database() + "\') ", connection(), "TABLE_NAME");
        if (results != null)
            for (final String sName : results) {
                final Base36 b36 = Base36.get(sName);
                theTables.put(b36, new H2Table(this, b36));
            }
    }
}
