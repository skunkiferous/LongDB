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
// $codepro.audit.disable methodChainLength, unusedStringBuilder
//CHECKSTYLE stop magic number check
package com.blockwithme.longdb.h2;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.sql.rowset.serial.SerialBlob;

import com.blockwithme.longdb.Columns;
import com.blockwithme.longdb.Range;
import com.blockwithme.longdb.base.AbstractTable;
import com.blockwithme.longdb.entities.Base36;
import com.blockwithme.longdb.entities.Bytes;
import com.blockwithme.longdb.entities.LongHolder;
import com.blockwithme.longdb.exception.DBException;
import com.blockwithme.longdb.h2.config.SQLUtil;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.LongCursor;

/** BETable implementation for H2 database. */
@ParametersAreNonnullByDefault
public class H2Table extends AbstractTable<H2Backend, H2Database, H2Table> {
    /** The Constant FULL_RANGE. */
    private static final Range FULL_RANGE = Range.fullRange();

    /** Instantiates a new h2 table.
     * 
     * @param theDB
     *        the database instance
     * @param theTableName
     *        the table name */
    protected H2Table(final H2Database theDB, final Base36 theTableName) {
        super(theDB, theTableName, false, false);
    }

    /** Check existing columns.
     * 
     * @param theKey
     *        the key
     * @param theInsertUpdateColumns
     *        the insert or update
     * @param theInsertList
     *        the insert list
     * @return the long array list */
    // TODO: Purpose of method, and it's parameters, is not obvious.
    // Would need improved comments in description.
    @CheckForNull
    private LongArrayList checkExistingColumns(final long theKey,
            final Columns theInsertUpdateColumns, final List<Long> theInsertList) {
        final LongArrayList existingColIds;
        final StringBuilder s1 = new StringBuilder();
        s1.append("SELECT COLUMN_KEY FROM ").append(this.database().database())
                .append('.').append(this.table().toString())
                .append(" WHERE ROW_KEY= ").append(theKey)
                .append(" AND COLUMN_KEY IN (");

        boolean prepend = false;
        final long[] modifiedCols = theInsertUpdateColumns.columns();
        for (final long longCursor : modifiedCols) {
            if (prepend)
                s1.append(',');
            s1.append(longCursor);
            prepend = true;
        }
        s1.append(')');
        existingColIds = SQLUtil.getResultsLongList(s1.toString(),
                connection(), "COLUMN_KEY");
        for (final long longCursor : modifiedCols) {
            if (existingColIds == null || !existingColIds.contains(longCursor))
                theInsertList.add(longCursor);
        }
        return existingColIds;
    }

    /** Connection.
     * 
     * @return the connection */
    private Connection connection() {
        return database.backend().connection();
    }

    /** Execute insert.
     * 
     * @param theKey
     *        the key
     * @param theInsertUpdateColumns
     *        the insert or update
     * @param theTime
     *        the timestamp
     * @param theInsertList
     *        the insert list */
    private void executeInsert(final long theKey,
            final Columns theInsertUpdateColumns, final Timestamp theTime,
            final List<Long> theInsertList) {
        PreparedStatement pStmt = null;
        try {

            final StringBuilder s2 = new StringBuilder(); // $codepro.audit.disable
                                                          // unusedStringBuilder
            final String insertSql = s2.append("INSERT INTO ")
                    .append(this.database().database()).append('.')
                    .append(this.table().toString())
                    .append(" VALUES (?, ?, ?, ?)").toString();

            pStmt = connection().prepareStatement(insertSql);
            for (final Long insertId : theInsertList) {
                pStmt.setLong(1, theKey);
                pStmt.setLong(2, insertId);
                final Bytes byts = theInsertUpdateColumns.getBytes(insertId);
                assert (byts != null);
                final Blob blob = new SerialBlob(byts.toArray(false));
                pStmt.setBlob(3, blob);
                pStmt.setTimestamp(4, theTime);
                pStmt.addBatch();
            }
            pStmt.executeBatch();

        } catch (final SQLException e) {
            throw new DBException(
                    "Error while executing insert statement(long key=" + theKey
                            + ", Columns insertOrUpdate="
                            + theInsertUpdateColumns + ", Timestamp now="
                            + theTime + ", List<Long> insertList="
                            + theInsertList + ")", e);
        } finally {
            if (pStmt != null) { // $codepro.audit.disable unnecessaryNullCheck
                try {
                    pStmt.close();
                } catch (final SQLException e) {
                    // ignore.
                }
            }
        }
    }

    /** Execute update. */
    private void executeUpdate(final long theKey,
            final Columns theInsertUpdateColumns, final Timestamp theTime,
            final LongArrayList theUpdateList) {
        PreparedStatement pStmt = null;
        try {
            final StringBuilder s3 = new StringBuilder();
            final String updateSql = s3.append("UPDATE ")
                    .append(this.database().database()).append('.')
                    .append(this.table().toString())
                    .append(" SET DATA_BLOB=?, LAST_MODIFIED=? ")
                    .append("WHERE ROW_KEY=? AND COLUMN_KEY=? ").toString();

            pStmt = connection().prepareStatement(updateSql);
            for (final LongCursor insertId : theUpdateList) {
                final Bytes byts = theInsertUpdateColumns
                        .getBytes(insertId.value);
                assert (byts != null);
                final Blob blob = new SerialBlob(byts.toArray(false));
                pStmt.setBlob(1, blob);
                pStmt.setTimestamp(2, theTime);
                pStmt.setLong(3, theKey);
                pStmt.setLong(4, insertId.value);
                pStmt.addBatch();
            }
            pStmt.executeBatch();

        } catch (final SQLException e) {
            throw new DBException(
                    "Error while executing update statement(long key=" + theKey
                            + ", Columns insertOrUpdate="
                            + theInsertUpdateColumns + ", Timestamp now="
                            + theTime + ", LongArrayList updateList="
                            + theUpdateList + ")", e);
        } finally {
            if (pStmt != null) { // $codepro.audit.disable unnecessaryNullCheck
                try {
                    pStmt.close();
                } catch (final SQLException e) {
                    // ignore.
                }
            }
        }
    }

    /** Insert update.
     * 
     * @param theKey
     *        the key
     * @param theInsertUpdateColumns
     *        the insert or update */
    private void insertUpdate(final long theKey,
            final Columns theInsertUpdateColumns) {

        if (theInsertUpdateColumns == null
                || theInsertUpdateColumns.size() == 0)
            return;

        // TODO : discuss and fix below should we use
        // System.currentTimeMillis() ?
        final Timestamp now = new Timestamp(System.currentTimeMillis());
        final List<Long> insertList = new ArrayList<Long>();
        final LongArrayList existingColIds = checkExistingColumns(theKey,
                theInsertUpdateColumns, insertList);

        if (!insertList.isEmpty())
            executeInsert(theKey, theInsertUpdateColumns, now, insertList);

        if (existingColIds != null && existingColIds.size() > 0)
            executeUpdate(theKey, theInsertUpdateColumns, now, existingColIds);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#closeInternal()
     */
    @Override
    protected void closeInternal() {
        // NOP
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#columnsCountInternal(long)
     */
    @Override
    protected long columnsCountInternal(final long theKey) {
        final StringBuilder strbuf = new StringBuilder();
        strbuf.append("SELECT COUNT(*) AS C_COUNT FROM  ")
                .append(this.database().database()).append('.')
                .append(this.table().toString()).append(" WHERE ROW_KEY=")
                .append(theKey);
        final long size = (Long) SQLUtil.getSingleResult(strbuf.toString(),
                connection(), "C_COUNT");
        return size;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#columnsInternal(long)
     */
    @Override
    @CheckForNull
    protected LongArrayList columnsInternal(final long theKey) {
        final StringBuilder strbuf = new StringBuilder();
        strbuf.append("SELECT COLUMN_KEY FROM ")
                .append(this.database().database()).append('.')
                .append(this.table().toString())
                .append(" WHERE ROW_KEY=" + theKey);
        final LongArrayList result = SQLUtil.getResultsLongList(
                strbuf.toString(), connection(), "COLUMN_KEY");
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#columnsIteratorInternal(long)
     */
    @Override
    @CheckForNull
    protected Iterator<LongHolder> columnsIteratorInternal(final long theKey) {
        final LongArrayList cols = columnsInternal(theKey);
        if ((cols != null) && !cols.isEmpty()) {
            final Iterator<LongCursor> iter = cols.iterator();
            final LongHolder holder = new LongHolder();
            return new Iterator<LongHolder>() {
                @Override
                public boolean hasNext() {
                    return iter.hasNext();
                }

                @Override
                public LongHolder next() {
                    holder.setRaw(iter.next().value);
                    return holder;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#getInternal(long,
     * com.carrotsearch.hppc.LongArrayList)
     */
    @Override
    @CheckForNull
    protected Columns getInternal(final long theKey,
            final LongArrayList theColumns) {
        if (theColumns == null || theColumns.size() == 0) {
            return null;
        }
        final StringBuilder strbuf = new StringBuilder();
        // TODO: Why are we returning the row key, when we already know what it
        // is?!?
        strbuf.append(
                "SELECT ROW_KEY, COLUMN_KEY, DATA_BLOB, LAST_MODIFIED FROM ")
                .append(this.database().database()).append('.')
                .append(this.table().toString()).append(" WHERE ROW_KEY=")
                .append(theKey).append(" AND COLUMN_KEY IN (");
        boolean prepend = false;
        for (final LongCursor longCursor : theColumns) {
            if (prepend)
                strbuf.append(',');
            strbuf.append(longCursor.value);
            prepend = true;
        }
        strbuf.append(") ORDER BY ROW_KEY, COLUMN_KEY");
        return SQLUtil.getResultsAsColumns(strbuf.toString(), connection());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#getInternal(long,
     * com.blockwithme.longdb.Range)
     */
    @Override
    @CheckForNull
    protected Columns getInternal(final long theKey, final Range theRange) {
        if (theRange == null || theRange.empty())
            return null;

        long start;
        long end;
        if (theRange.equals(Range.fullRange())) {
            start = Long.MIN_VALUE;
            end = Long.MAX_VALUE;
        } else {
            start = (theRange.start() < theRange.end()) ? theRange.start()
                    : theRange.end();
            end = (theRange.start() < theRange.end()) ? theRange.end()
                    : theRange.start();
        }

        final StringBuilder strbuf = new StringBuilder();
        // TODO: Why are we returning the row key, when we already know what it
        // is?!?
        strbuf.append(
                "SELECT ROW_KEY, COLUMN_KEY, DATA_BLOB, LAST_MODIFIED FROM ")
                .append(this.database().database()).append('.')
                .append(this.table().toString()).append(" WHERE ROW_KEY=")
                .append(theKey).append(" AND COLUMN_KEY >=").append(start)
                .append(" AND COLUMN_KEY <=").append(end);
        strbuf.append(" ORDER BY ROW_KEY, COLUMN_KEY");
        return SQLUtil.getResultsAsColumns(strbuf.toString(), connection());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#keysInternal()
     */
    @Override
    protected Iterator<LongHolder> keysInternal() {
        return new H2KeyIterator(this);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#openInternal()
     */
    @Override
    protected void openInternal() {
        closed = false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#removeInternal(long,
     * com.carrotsearch.hppc.LongArrayList)
     */
    @Override
    protected void removeInternal(final long theKey,
            final LongArrayList theRemoveCols) {

        final StringBuilder strBuf = new StringBuilder();
        strBuf.append("DELETE FROM ").append(this.database().database())
                .append('.').append(this.table().toString())
                .append(" WHERE ROW_KEY=").append(theKey)
                .append(" AND COLUMN_KEY IN (");
        boolean prepend = false;
        for (final LongCursor longCursor : theRemoveCols) {
            if (prepend)
                strBuf.append(',');
            strBuf.append(longCursor.value);
            prepend = true;
        }
        strBuf.append(')');
        SQLUtil.executeStatement(strBuf.toString(), connection());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#removeInternal(long,
     * com.blockwithme.longdb.Range)
     */
    @Override
    protected void removeInternal(final long theKey, final Range theRemoveCols) {

        if (theRemoveCols.empty())
            return;
        long start;
        long end;
        if (theRemoveCols.equals(Range.fullRange())) {
            start = Long.MIN_VALUE;
            end = Long.MAX_VALUE;
        } else {
            start = (theRemoveCols.start() < theRemoveCols.end()) ? theRemoveCols
                    .start() : theRemoveCols.end();
            end = (theRemoveCols.start() < theRemoveCols.end()) ? theRemoveCols
                    .end() : theRemoveCols.start();
        }

        final StringBuilder strBuf = new StringBuilder();
        strBuf.append("DELETE FROM ").append(this.database().database())
                .append('.').append(this.table().toString())
                .append(" WHERE ROW_KEY=").append(theKey)
                .append(" AND COLUMN_KEY >= ").append(start)
                .append(" AND COLUMN_KEY <= ").append(end);
        SQLUtil.executeStatement(strBuf.toString(), connection());
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#setInternal(long,
     * com.blockwithme.longdb.Columns, com.carrotsearch.hppc.LongArrayList)
     */
    @Override
    protected void setInternal(final long theKey,
            final Columns theInsertUpdateColumns,
            final LongArrayList theRemoveCols) {
        removeInternal(theKey, theRemoveCols);
        insertUpdate(theKey, theInsertUpdateColumns);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#setInternal(long,
     * com.blockwithme.longdb.Columns, com.blockwithme.longdb.Range)
     */
    @Override
    protected void setInternal(final long theKey,
            final Columns theInsertUpdateColumns, final Range theRemoveCols) {
        removeInternal(theKey, theRemoveCols);
        insertUpdate(theKey, theInsertUpdateColumns);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#sizeInternal()
     */
    @Override
    protected long sizeInternal() {
        final StringBuilder strbuf = new StringBuilder();
        strbuf.append("SELECT COUNT(DISTINCT ROW_KEY) AS R_COUNT FROM ")
                .append(this.database().database()).append('.')
                .append(this.table().toString());
        final long size = (Long) SQLUtil.getSingleResult(strbuf.toString(),
                connection(), "R_COUNT");
        return size;
    }

    /** Called when the table was dropped. */
    public void dropped() {
        // NOP
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#getLimited(long, int)
     */
    @Override
    public Columns getLimited(final long theKey, final int theCount) {

        if (theCount == 0)
            return null;
        final StringBuilder strBuf = new StringBuilder(); // $codepro.audit.disable
                                                          // unusedStringBuilder
        final String query = strBuf
                .append("SELECT ROW_KEY, COLUMN_KEY, DATA_BLOB, ")
                .append(" LAST_MODIFIED FROM ")
                .append(this.database().database()).append('.')
                .append(this.table().toString()).append(" WHERE ROW_KEY=")
                .append(theKey).append(" ORDER BY ROW_KEY, ")
                .append(" COLUMN_KEY LIMIT ").append(theCount).toString();

        return SQLUtil.getResultsAsColumns(query, connection());

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#reverseSupported()
     */
    @Override
    public boolean reverseSupported() {
        return false;
    }

}
