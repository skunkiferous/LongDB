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
package com.blockwithme.longdb.cassandra;

import static com.blockwithme.longdb.cassandra.CassandraConstants.REVERSE_COLUMN_COMPARATOR;
import static com.blockwithme.longdb.cassandra.CassandraConstants.ROW_SLICE_COUNT;
import static com.blockwithme.longdb.util.DBUtils.toArray;

import java.util.Iterator;

import javax.annotation.CheckForNull;
import javax.annotation.ParametersAreNonnullByDefault;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blockwithme.longdb.Columns;
import com.blockwithme.longdb.Range;
import com.blockwithme.longdb.base.AbstractTable;
import com.blockwithme.longdb.entities.Base36;
import com.blockwithme.longdb.entities.Bytes;
import com.blockwithme.longdb.entities.LongHolder;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.LongCursor;

/** The Class CassandraTable. */
@ParametersAreNonnullByDefault
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "CD_CIRCULAR_DEPENDENCY", justification = "This is our base class design")
public class CassandraTable extends
        AbstractTable<CassandraBackend, CassandraDatabase, CassandraTable> {

    /** The range object with full range. */
    private static final Range FULL_RANGE = Range.fullRange();

    /** The logger. */
    private static final Logger LOG = LoggerFactory
            .getLogger(CassandraTable.class);

    /** The column family definition. */
    private ColumnFamilyDefinition cfDef;

    /** This constructor is used for creating a new Table. */
    protected CassandraTable(final CassandraDatabase theDB,
            final Base36 theTable, final boolean isReverse,
            final boolean theDetectCollisions) {
        super(theDB, theTable, isReverse, theDetectCollisions);
        if (!isReverse) {
            cfDef = new ThriftCfDef(theDB.database(), theTable.toFixedString(),
                    ComparatorType.LONGTYPE);
        } else {
            cfDef = new ThriftCfDef(theDB.database(), theTable.toFixedString(),
                    ComparatorType.getByClassName(REVERSE_COLUMN_COMPARATOR));
        }
        getcluster().addColumnFamily(cfDef);
    }

    /** This constructor is used for loading existing table.
     * 
     * @param theDB
     *        the database
     * @param theColumnFamilyDef
     *        the column family definition
     * @param isReverse
     *        the reverse flag
     * @param theDetectCollisions
     *        the detect collisions flag */
    protected CassandraTable(final CassandraDatabase theDB,
            final ColumnFamilyDefinition theColumnFamilyDef,
            final boolean isReverse, final boolean theDetectCollisions) {
        super(theDB, Base36.get(theColumnFamilyDef.getName()), isReverse,
                theDetectCollisions);
        this.cfDef = theColumnFamilyDef;
    }

    /** Internal method: Checks range if now empty/null/full-range */
    private boolean checkRange(final Range theRange) {
        return ((theRange != null) && !theRange.empty())
                && !theRange.equals(Range.fullRange());
    }

    /** Internal method: Checks reverse. */
    private boolean checkReverse(final Range theRange) {
        return (!reverse && (theRange.start() <= theRange.end()))
                || (reverse && (theRange.end() <= theRange.start()));
    }

    /** Internal method: Construct 'Columns' object with the results from a
     * Cassandra query */
    @CheckForNull
    private Columns constructColumns(
            final OrderedRows<Long, Long, byte[]> theRows,
            final boolean isReverse) {
        final LongObjectOpenHashMap<Bytes> map = new LongObjectOpenHashMap<Bytes>();
        if (theRows != null) {
            final Row<Long, Long, byte[]> row = theRows.iterator().next();
            final ColumnSlice<Long, byte[]> colS = row.getColumnSlice();
            if ((colS == null) || colS.getColumns().size() == 0)
                return null;
            for (final HColumn<Long, byte[]> col : colS.getColumns()) {
                map.put(col.getName(), new Bytes(col.getValue()));
            }
            if (map.size() > 0)
                return new Columns(map, isReverse);
        }
        return null;
    }

    /** Execute column query.
     * 
     * @param theKey
     *        the key
     * @param theRange
     *        the range
     * @param theMaxCount
     *        the max count
     * @param theColumns
     *        the cols
     * @return the ordered rows */
    @CheckForNull
    private OrderedRows<Long, Long, byte[]> executeColumnQuery(
            final long theKey, final Range theRange, final int theMaxCount,
            final LongArrayList theColumns) {

        final Keyspace keyspace = getKeySpace();
        final RangeSlicesQuery<Long, Long, byte[]> rangeSlicesQuery = HFactory
                .createRangeSlicesQuery(keyspace, LongSerializer.get(),
                        LongSerializer.get(), BytesArraySerializer.get());

        rangeSlicesQuery.setColumnFamily(cfDef.getName());
        rangeSlicesQuery.setKeys(theKey, theKey);
        // check if Range is set.
        if (checkRange(theRange)) {
            if (checkReverse(theRange))// check if range is as per the reverse
                // order.
                rangeSlicesQuery.setRange(theRange.start(), theRange.end(),
                        false, theMaxCount);
            else
                rangeSlicesQuery.setRange(theRange.end(), theRange.start(),
                        false, theMaxCount);
        } else
            rangeSlicesQuery.setRange(null, null, false, theMaxCount); // full
        // range

        if (theColumns != null) {
            rangeSlicesQuery.setColumnNames(toArray(theColumns));
        }
        final QueryResult<OrderedRows<Long, Long, byte[]>> result = rangeSlicesQuery
                .execute();
        if (result == null || result.get() == null
                || result.get().getCount() == 0) {
            return null;
        }
        return result.get();
    }

    /** Gets the cluster.
     * 
     * @return the cluster */
    private Cluster getcluster() {
        return ((CassandraBackend) database.backend()).cluster();
    }

    /** Gets the key space.
     * 
     * @return the key space */
    @CheckForNull
    private Keyspace getKeySpace() {
        final Cluster cluster = getcluster();
        if (cluster.describeKeyspace(database.database()) == null) {
            return null; // keyspace doesn't exist.
        }
        final Keyspace keyspace = HFactory.createKeyspace(database.database(),
                cluster);
        return keyspace;
    }

    /** Sets the internal.
     * 
     * @param theKey
     *        the key
     * @param theInsertUpdateColumns
     *        the insert or update */
    private void setInternal(final long theKey,
            final Columns theInsertUpdateColumns) {
        final LongSerializer bytSerializer = new LongSerializer();
        final Keyspace keyspaceOperator = getKeySpace();
        final Mutator<Long> mutator = HFactory.createMutator(keyspaceOperator,
                bytSerializer);
        int columnIndex = 0;
        for (final LongHolder longHolder : theInsertUpdateColumns) {
            final Bytes byts = theInsertUpdateColumns.getBytes(longHolder
                    .value());
            assert (byts != null);
            final String cfName = table.toFixedString();
            final Long columnName = theInsertUpdateColumns.columns()[columnIndex];
            columnIndex++;
            final HColumn<Long, byte[]> hcol = new HColumnImpl<Long, byte[]>(
                    columnName, byts.toArray(false), HFactory.createClock());
            mutator.addInsertion(theKey, cfName, hcol);
        }
        final MutationResult result = mutator.execute();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Column Insertion Result > Execution Time >"
                    + result.getExecutionTimeMicro() + " Host used >"
                    + result.getHostUsed());
        }
    }

    /** Called when the table is closed. */
    @Override
    protected void closeInternal() {
        // NOP
    }

    /** Return number of columns in a row.
     * 
     * @param theKey
     *        the key
     * @return the long */
    @Override
    protected long columnsCountInternal(final long theKey) {
        final Keyspace keyspace = getKeySpace();
        final QueryResult<Integer> qr = HFactory
                .createCountQuery(keyspace, LongSerializer.get(),
                        LongSerializer.get()).setColumnFamily(cfDef.getName())
                .setKey(theKey).setRange(null, null, Integer.MAX_VALUE)
                .execute();
        return qr.get();
    }

    /** Returns all column Ids for a row.
     * 
     * @param theKey
     *        the key
     * @return the long array list */
    @Override
    @CheckForNull
    protected LongArrayList columnsInternal(final long theKey) {
        final OrderedRows<Long, Long, byte[]> rows = executeColumnQuery(theKey,
                null, Integer.MAX_VALUE, null);
        final LongArrayList cols = new LongArrayList();
        if (rows != null) {
            final Row<Long, Long, byte[]> row = rows.iterator().next();
            final ColumnSlice<Long, byte[]> colS = row.getColumnSlice();
            for (final HColumn<Long, byte[]> col : colS.getColumns()) {
                cols.add(col.getName());
            }
            if (cols.size() > 0)
                return cols;
        }
        return null;
    }

    /** Returns Columns Id Iterator.
     * 
     * @param theKey
     *        the key
     * @return the iterator returns null if row not found. */
    @Override
    @CheckForNull
    protected Iterator<LongHolder> columnsIteratorInternal(final long theKey) {
        final Columns cols = getInternal(theKey, FULL_RANGE);
        if (cols != null)
            return cols.iterator();
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
        final OrderedRows<Long, Long, byte[]> cls = executeColumnQuery(theKey,
                null, Integer.MAX_VALUE, theColumns);
        return constructColumns(cls, reverse);
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
        final OrderedRows<Long, Long, byte[]> cols = executeColumnQuery(theKey,
                theRange, Integer.MAX_VALUE, null);

        return constructColumns(cols, reverse);
    }

    /** Returns rowId Iterator.
     * 
     * @return the iterator */
    @Override
    protected Iterator<LongHolder> keysInternal() {
        return new CassandraKeyIterator(this);
    }

    /** Called when the table is opened. */
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
        final LongSerializer bytSerializer = new LongSerializer();
        final Keyspace keyspaceOperator = getKeySpace();
        final Mutator<Long> mutator = HFactory.createMutator(keyspaceOperator,
                bytSerializer);
        final String cfName = table.toFixedString();
        for (final LongCursor longCursor : theRemoveCols) {
            mutator.addDeletion(theKey, cfName, longCursor.value,
                    LongSerializer.get());
        }
        final MutationResult result = mutator.execute();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Column Insertion Result > Execution Time >"
                    + result.getExecutionTimeMicro() + " Host used >"
                    + result.getHostUsed());
        }
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
        final OrderedRows<Long, Long, byte[]> rows = executeColumnQuery(theKey,
                theRemoveCols, Integer.MAX_VALUE, null);
        if (rows == null)
            return;
        final LongArrayList cols = new LongArrayList();
        final Row<Long, Long, byte[]> row = rows.iterator().next();
        final ColumnSlice<Long, byte[]> colS = row.getColumnSlice();
        for (final HColumn<Long, byte[]> col : colS.getColumns()) {
            cols.add(col.getName());
        }
        removeInternal(theKey, cols);
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
        setInternal(theKey, theInsertUpdateColumns);
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
        setInternal(theKey, theInsertUpdateColumns);
    }

    /** Returns approximate number of rows in this table.
     * 
     * @return the long */
    @Override
    protected long sizeInternal() {
        long start = 0;
        long lastEnd = 0;
        long count = 0;
        Long startKey = null;

        /** Following while loop loads around ROW_SLICE_COUNT rows per iteration
         * using RangeSlicesQuery. The Last key found in the previous iteration
         * becomes 'start key' in the current iteration. */
        while (true) { // $codepro.audit.disable constantConditionalExpression
            final Keyspace keyspace = getKeySpace();

            final RangeSlicesQuery<Long, Long, byte[]> query = HFactory
                    .createRangeSlicesQuery(keyspace, LongSerializer.get(),
                            LongSerializer.get(), BytesArraySerializer.get());
            query.setColumnFamily(cfDef.getName()).setKeys(startKey, null);
            query.setReturnKeysOnly().setRowCount(ROW_SLICE_COUNT);
            final OrderedRows<Long, Long, byte[]> rows = query.execute().get();
            final int rowCount = rows.getCount();
            if (rowCount == 0) {
                break;
            } else {
                start = rows.peekLast().getKey();
                if (startKey != null && start == lastEnd) {
                    break;
                }
                count += rowCount - 1; // Key range is inclusive
                lastEnd = start;
                startKey = start;
            }
        }
        if (count > 0) {
            count += 1;
        }
        return count;
    }

    /** Called when the table was dropped. */
    public void dropped() {
        final String tbl = getcluster().dropColumnFamily(database.database(),
                table.toFixedString());
        LOG.debug("Table dropped >" + tbl);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#getLimited(long, int)
     */
    @Override
    @CheckForNull
    public Columns getLimited(final long theKey, final int theCount) {
        final OrderedRows<Long, Long, byte[]> cols = executeColumnQuery(theKey,
                null, theCount, null);
        if (cols == null)
            return null;
        return constructColumns(cols, reverse);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#reverseSupported()
     */
    @Override
    public boolean reverseSupported() {
        return true;
    }

}
