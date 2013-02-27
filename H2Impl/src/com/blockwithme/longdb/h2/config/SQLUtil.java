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
package com.blockwithme.longdb.h2.config;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.CheckForNull;

import com.blockwithme.longdb.Columns;
import com.blockwithme.longdb.entities.Bytes;
import com.blockwithme.longdb.exception.DBException;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongObjectOpenHashMap;

/** SQL Util class */
// TODO: For the whole file: if you have multiple resources to close(),
// each close should be in it's own try/finally, so that an exception
// thrown for the inner-most resource close will not prevent the
// closure of the outer-most resource.
public final class SQLUtil {
    /** Executes a statement where response is not expected.
     * 
     * @param theStatement
     *        statement to be executed
     * @param theConnection
     *        the connection */
    public static void executeStatement(final String theStatement,
            final Connection theConnection) {
        Statement stmt = null;
        try {
            stmt = theConnection.createStatement();
            stmt.execute(theStatement);
        } catch (final SQLException e) {
            throw new DBException("Error executing statement: " + theStatement
                    + ", Connection conn=" + theConnection, e);
        } finally {
            if (stmt != null)
                try {
                    stmt.close();
                } catch (final SQLException e) {
                    // ignore
                }
        }
    }

    /** Gets the results as columns.
     * 
     * @param theStatement
     *        statement to be executed
     * @param theConnection
     *        the connection
     * @return the results as columns */
    @CheckForNull
    public static Columns getResultsAsColumns(final String theStatement,
            final Connection theConnection) {
        try {
            final LongObjectOpenHashMap<Bytes> map = new LongObjectOpenHashMap<Bytes>();
            final Statement stmt = theConnection.createStatement();
            final ResultSet rs = stmt.executeQuery(theStatement);
            try {
                while (rs.next()) {
                    final Blob blob = rs.getBlob("DATA_BLOB");
                    map.put(rs.getLong("COLUMN_KEY"),
                            new Bytes(blob.getBytes(0, (int) blob.length())));
                }
                if (map.size() > 0)
                    return new Columns(map, false);
            } finally {
                rs.close();
                stmt.close();
            }
        } catch (final SQLException e) {
            throw new DBException("Error executing statement: " + theStatement
                    + ", Connection conn=" + theConnection, e);

        }
        return null;
    }

    /** Gets the results as list.
     * 
     * @param theStatement
     *        statement to be executed
     * @param theConnection
     *        the connection object
     * @param theColumnName
     *        the column name for the data to be extracted from result set.
     * @return the results as list */
    public static List<String> getResultsAsList(final String theStatement,
            final Connection theConnection, final String theColumnName) {
        try {
            final Statement stmt = theConnection.createStatement();
            final ResultSet rs = stmt.executeQuery(theStatement);
            try {
                final List<String> res = new ArrayList<String>();
                while (rs.next()) {
                    final String r = rs.getString(theColumnName);
                    res.add(r);
                }
                return res;
            } finally {
                rs.close();
                stmt.close();
            }
        } catch (final SQLException e) {
            throw new DBException("Error executing statement: " + theStatement
                    + ", Connection conn=" + theConnection
                    + ", String colName=" + theColumnName, e);
        }
    }

    /** Gets the results long list.
     * 
     * @param theStatement
     *        statement to be executed
     * @param theConnection
     *        the connection object
     * @param theColumnName
     *        the column name for the data to be extracted.
     * @return the results as LongArrayList */
    @CheckForNull
    public static LongArrayList getResultsLongList(final String theStatement,
            final Connection theConnection, final String theColumnName) {
        try {
            final Statement stmt = theConnection.createStatement();
            final ResultSet rs = stmt.executeQuery(theStatement); // $codepro.audit.disable
            // com.instantiations.assist.eclipse.analysis.sqlInjection
            try {
                LongArrayList res = null;
                while (rs.next()) {
                    if (res == null)
                        res = new LongArrayList();
                    res.add(rs.getLong(theColumnName));
                }
                return res;
            } finally {
                rs.close();
                stmt.close();
            }
        } catch (final SQLException e) {
            throw new DBException("Error executing statement: " + theStatement
                    + ", Connection conn=" + theConnection
                    + ", String colName=" + theColumnName, e);
        }
    }

    /** Gets the single result.
     * 
     * @param theStatement
     *        statement to be executed
     * @param theConnection
     *        the connection object
     * @param theColumnName
     *        the column name for the data to be extracted
     * @return the single result */
    public static Object getSingleResult(final String theStatement,
            final Connection theConnection, final String theColumnName) {
        try {
            final Statement stmt = theConnection.createStatement();
            final ResultSet rs = stmt.executeQuery(theStatement); // NOPMD
            try {
                rs.next();
                return rs.getObject(theColumnName);
            } finally {
                rs.close();
                stmt.close();
            }
        } catch (final SQLException e) {
            throw new DBException("Error executing statement: " + theStatement
                    + ", Connection conn=" + theConnection
                    + ", String colName=" + theColumnName, e);
        }
    }

    /** Hide utility class constructor. */
    private SQLUtil() {
    }
}
