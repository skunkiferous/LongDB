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

import static com.blockwithme.longdb.cassandra.embedded.CassandraEmbConstants.DEFAULT_CONSISTENCY_LEVEL;
import static com.blockwithme.longdb.cassandra.embedded.CassandraEmbConstants.REVERSE_COLUMN_COMPARATOR;
import static com.blockwithme.longdb.cassandra.embedded.CassandraEmbConstants.ROW_SLICE_COUNT;
import static com.blockwithme.longdb.common.constants.ByteConstants.BLANK_STR_BYTES;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.ParametersAreNonnullByDefault;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.cassandra.thrift.CassandraServer;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.Mutation._Fields;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blockwithme.longdb.Columns;
import com.blockwithme.longdb.Range;
import com.blockwithme.longdb.base.AbstractTable;
import com.blockwithme.longdb.entities.Base36;
import com.blockwithme.longdb.entities.Bytes;
import com.blockwithme.longdb.entities.LongHolder;
import com.blockwithme.longdb.exception.DBException;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.LongCursor;

/** The Class Cassandra Embedded Table. */
@ParametersAreNonnullByDefault
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "CD_CIRCULAR_DEPENDENCY", justification = "This is our base class design")
public class CassandraEmbTable
        extends
        AbstractTable<CassandraEmbBackend, CassandraEmbDatabase, CassandraEmbTable> {

    /**  */
    private static final byte[] ZERO_BYTES = new byte[0];

    /** The range object representing full range of long numbers. */
    private static final Range FULL_RANGE = Range.fullRange();

    /** The logger object. */
    private static final Logger LOG = LoggerFactory
            .getLogger(CassandraEmbTable.class);

    /** column family definition */
    private final CfDef cfDef;

    /** This constructor is used to create a new Column family.
     * 
     * @param theDB
     *        the cassandra database object
     * @param theTable
     *        the table name.
     * @param isReverse
     *        the reverse flag
     * @param theDetectCollisions
     *        the detect collisions flag */
    protected CassandraEmbTable(final CassandraEmbDatabase theDB,
            final Base36 theTable, final boolean isReverse,
            final boolean theDetectCollisions) {
        super(theDB, theTable, isReverse, theDetectCollisions);
        try {
            setks();
            cfDef = new CfDef();
            cfDef.setName(theTable.toFixedString());
            cfDef.setKeyspace(theDB.database());
            if (!isReverse) {
                cfDef.setComparator_type(ComparatorType.LONGTYPE.getClassName());
            } else {
                cfDef.setComparator_type(REVERSE_COLUMN_COMPARATOR);
            }
            server().system_add_column_family(cfDef);
        } catch (final Exception e) {
            LOG.error(
                    "Exception Occurred in - "
                            + "CassandraEmbTable(CassandraEmbDatabase database="
                            + theDB + ", Base36 table=" + theTable
                            + ", boolean reverse=" + isReverse
                            + ", boolean detectCollisions="
                            + theDetectCollisions + ")", e);
            throw new DBException("Error While creating Table.", e);
        }
    }

    /** This constructor is used for initializing an existing Table.
     * 
     * @param theDB
     *        the database object
     * @param theColumnFalmilyDef
     *        the column family definition
     * @param isReverse
     *        the reverse flag
     * @param theDetectCollisions
     *        the detect collisions flag */
    protected CassandraEmbTable(final CassandraEmbDatabase theDB,
            final CfDef theColumnFalmilyDef, final boolean isReverse,
            final boolean theDetectCollisions) {
        super(theDB, Base36.get(theColumnFalmilyDef.getName()), isReverse,
                theDetectCollisions);
        this.cfDef = theColumnFalmilyDef;
    }

    /** Internal method: Check range is not full or empty */
    private boolean checkRange(final Range theRange) {
        return theRange != null && !theRange.empty()
                && !theRange.equals(Range.fullRange());
    }

    /** Internal method: Checks range and column name are null */
    private boolean checkRangeAndColumnName(final Range theRange,
            final LongArrayList theColumnName) {
        return (theRange == null && theColumnName == null)
                || (theColumnName == null && theRange != null && theRange
                        .equals(FULL_RANGE));
    }

    /** Internal method: Check reverse column. */
    private boolean checkReverseColumn(final Range theRange) {
        return (!reverse && theRange.start() <= theRange.end())
                || (reverse && theRange.end() <= theRange.start());
    }

    /** Internal method: Construct column name. */
    private List<ByteBuffer> constructColName(final LongArrayList theColumnName) {
        final List<ByteBuffer> list = new ArrayList<ByteBuffer>();
        for (final LongCursor longCursor : theColumnName) {
            list.add(ByteBufferUtil.bytes(longCursor.value));
        }
        return list;
    }

    /** Internal method: Constructs mutations object with the data to be
     * modified. */
    private Mutation constructMutations(final Columns theInsertUpdateColumns,
            final int theColumnIndex, final LongHolder theColumnId) {
        final Bytes byts = theInsertUpdateColumns.getBytes(theColumnId.value());
        assert (byts != null);
        final Long columnName = theInsertUpdateColumns.columns()[theColumnIndex];

        final Mutation m = new Mutation();
        final ColumnOrSuperColumn col = new ColumnOrSuperColumn();
        final Column clmn = new Column(ByteBufferUtil.bytes(columnName));
        clmn.setValue(byts.toArray(false));
        clmn.setTimestamp(HFactory.createClock());
        col.setColumn(clmn);
        m.setFieldValue(_Fields.COLUMN_OR_SUPERCOLUMN, col);
        return m;
    }

    /** Internal method: Constructs range slice object. */
    private SliceRange constructRange(final Integer theCount,
            final Long theMin, final Long theMax, final boolean theKeysOnly) {
        final SliceRange range = new SliceRange();
        if (theKeysOnly)
            range.setCount(0); // to return only keys.
        if (theCount != null)
            range.setCount(theCount);
        if (theMin == null || theMin == Long.MIN_VALUE)
            range.setStart(ByteBuffer.wrap(ZERO_BYTES));
        else
            range.setStart(ByteBufferUtil.bytes(theMin));
        if (theMax == null || theMax == Long.MAX_VALUE)
            range.setFinish(ByteBuffer.wrap(ZERO_BYTES));
        else
            range.setFinish(ByteBufferUtil.bytes(theMax));
        return range;
    }

    /** Internal method: Construct range slice object */
    private SliceRange constructRange(final Range theRange,
            final Integer theMaxCount) {
        SliceRange sRange;
        if (checkRange(theRange)) { // check if range is not full range
            if (checkReverseColumn(theRange))// check reverse column
                sRange = constructRange(theMaxCount, theRange.start(),
                        theRange.end(), false);
            else
                sRange = constructRange(theMaxCount, theRange.end(),
                        theRange.start(), false);
        } else
            sRange = constructRange(theMaxCount, Long.MIN_VALUE,
                    Long.MAX_VALUE, false);
        return sRange;
    }

    /** Internal method: Constructs mutation object with the data to be removed. */
    private Mutation constructRemoveMutations(final LongCursor theLongCur) {
        final Mutation m = new Mutation();
        final Deletion deletion = new Deletion();
        deletion.setTimestamp(HFactory.createClock());
        final SlicePredicate slicePredicate = new SlicePredicate();
        slicePredicate
                .addToColumn_names(ByteBufferUtil.bytes(theLongCur.value));
        deletion.setPredicate(slicePredicate);
        m.setFieldValue(_Fields.DELETION, deletion);
        return m;
    }

    /** Internal method: Creates the columns. */
    @CheckForNull
    private Columns createColumns(final List<ColumnOrSuperColumn> theRowData) {
        Columns colsObj = null;
        if (!theRowData.isEmpty()) {
            final LongObjectOpenHashMap<Bytes> map = new LongObjectOpenHashMap<Bytes>();
            for (final ColumnOrSuperColumn col : theRowData) {
                final Long colName = LongSerializer.get().fromBytes(
                        col.getColumn().getName());
                final byte[] colBlob = col.getColumn().getValue();
                map.put(colName, new Bytes(colBlob));
            }
            colsObj = new Columns(map, reverse);
        }
        return colsObj;
    }

    /** Internal method: Actually executes column query. */
    @CheckForNull
    private List<ColumnOrSuperColumn> executeColumnQuery(final long theKey,
            final Range theRange, final int theMaxCount,
            final LongArrayList theColumnNames) {
        List<ColumnOrSuperColumn> resultArray = null;
        try {
            setks();
            final Integer mCounts = (theMaxCount == Integer.MAX_VALUE) ? null
                    : theMaxCount;
            final SlicePredicate predicate = new SlicePredicate();
            // check if range and column name 'both' are not set.
            if (checkRangeAndColumnName(theRange, theColumnNames)) {
                predicate.setSlice_range(new SliceRange(ByteBuffer
                        .wrap(ZERO_BYTES), ByteBuffer.wrap(ZERO_BYTES), false,
                        theMaxCount));
            } else if (theRange != null && !theRange.equals(FULL_RANGE))
                predicate.setSlice_range(constructRange(theRange, mCounts));

            if (theColumnNames != null)
                predicate.setColumn_names(constructColName(theColumnNames));

            resultArray = server().get_slice(ByteBufferUtil.bytes(theKey),
                    getColumnParent(), predicate, DEFAULT_CONSISTENCY_LEVEL);
            if (resultArray.isEmpty()) {
                return null;
            }
        } catch (final Exception e) {
            LOG.error("Exception Occurred in - executeColumnQuery(long key="
                    + theKey + ", Range range=" + theRange + ", int maxcount="
                    + theMaxCount + ", LongArrayList colNames="
                    + theColumnNames + ")", e);
            throw new DBException("Error performing Column Slice Query.", e);
        }
        return resultArray;
    }

    /** Internal method: Gets the column parent. */
    private ColumnParent getColumnParent() {
        final ColumnParent columnParent = new ColumnParent();
        columnParent.setColumn_family(table.toFixedString());
        return columnParent;
    }

    /** Internal method: Gets the key range. */
    private KeyRange getKeyRange(final Long theStartKey) {
        final KeyRange kRange = new KeyRange();
        if (theStartKey == null)
            kRange.setStart_key(BLANK_STR_BYTES);
        else
            kRange.setStart_key(ByteBufferUtil.bytes(theStartKey));
        kRange.setEnd_key(BLANK_STR_BYTES);
        kRange.setCount(ROW_SLICE_COUNT);
        return kRange;
    }

    /** Internal method: get the Server object */
    private CassandraServer server() {
        return ((CassandraEmbBackend) this.database().backend()).server();
    }

    /** Internal method: Sets the internal. */
    private void setInternal(final long theKey,
            final Columns theInsertUpdateColumns) {

        try {
            setks();
            final Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
            int columnIndex = 0;
            final List<Mutation> mutations = new ArrayList<Mutation>();
            final Map<String, List<Mutation>> subMap = new HashMap<String, List<Mutation>>();
            for (final LongHolder longHolder : theInsertUpdateColumns) {
                final Mutation m = constructMutations(theInsertUpdateColumns,
                        columnIndex, longHolder);
                mutations.add(m);
                columnIndex++;
            }
            subMap.put(table.toFixedString(), mutations);
            mutationMap.put(ByteBufferUtil.bytes(theKey), subMap);
            server().batch_mutate(mutationMap, DEFAULT_CONSISTENCY_LEVEL);

        } catch (final Exception e) {
            LOG.error("Exception Occurred in - setInternal(long key=" + theKey
                    + ", Columns insertOrUpdate=" + theInsertUpdateColumns
                    + ")", e);
            throw new DBException("Error while inserting/updating values", e);
        }
    }

    /** Internal method: Sets key space. */
    private void setks() throws InvalidRequestException, TException {
        server().set_keyspace(this.database().database());
    }

    /** Called when table is closed. */
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
        try {
            setks();
            final SliceRange range = constructRange(Integer.MAX_VALUE,
                    Long.MIN_VALUE, Long.MAX_VALUE, true);
            final SlicePredicate predicate = new SlicePredicate();
            predicate.setSlice_range(range);
            final List<ColumnOrSuperColumn> resultArray = server().get_slice(
                    ByteBufferUtil.bytes(theKey), getColumnParent(), predicate,
                    DEFAULT_CONSISTENCY_LEVEL);
            return resultArray.size();
        } catch (final Exception e) {
            LOG.error("Exception Occurred in - "
                    + "columnsCountInternal(long key=" + theKey + ")", e);
            throw new DBException("Error getting Row size.", e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#columnsInternal(long)
     */
    @Override
    @CheckForNull
    protected LongArrayList columnsInternal(final long theKey) {
        final List<ColumnOrSuperColumn> rowData = executeColumnQuery(theKey,
                FULL_RANGE, Integer.MAX_VALUE, null);
        final LongArrayList cols = new LongArrayList();
        if (rowData != null) {
            for (final ColumnOrSuperColumn col : rowData) {
                final Long colName = LongSerializer.get().fromBytes(
                        col.getColumn().getName());
                cols.add(colName);
            }
            if (cols.size() > 0)
                return cols;
        }
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#columnsIteratorInternal(long)
     */
    @Override
    @CheckForNull
    protected Iterator<LongHolder> columnsIteratorInternal(final long theKey) {
        final Columns cols = getInternal(theKey, FULL_RANGE);
        if (cols == null)
            return null;
        return cols.iterator();
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
        final List<ColumnOrSuperColumn> rowData = executeColumnQuery(theKey,
                FULL_RANGE, Integer.MAX_VALUE, theColumns);
        if (rowData == null)
            return null;
        return createColumns(rowData);
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
        final List<ColumnOrSuperColumn> rowData = executeColumnQuery(theKey,
                theRange, Integer.MAX_VALUE, null);
        if (rowData == null)
            return null;
        return createColumns(rowData);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#keysInternal()
     */
    @Override
    protected Iterator<LongHolder> keysInternal() {
        return new CassandraEmbKeyIterator(this);
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
        try {
            setks();
            final Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
            final List<Mutation> mutations = new ArrayList<Mutation>();
            final Map<String, List<Mutation>> subMap = new HashMap<String, List<Mutation>>();
            for (final LongCursor longCur : theRemoveCols) {
                final Mutation m = constructRemoveMutations(longCur);
                mutations.add(m);
            }
            subMap.put(table.toFixedString(), mutations);
            mutationMap.put(ByteBufferUtil.bytes(theKey), subMap);
            server().batch_mutate(mutationMap, DEFAULT_CONSISTENCY_LEVEL);

        } catch (final Exception e) {
            LOG.error("Exception Occurred in - removeInternal(long key="
                    + theKey + ", LongArrayList remove=" + theRemoveCols + ")",
                    e);
            throw new DBException("Error while removing values", e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#removeInternal(long,
     * com.blockwithme.longdb.Range)
     */
    @Override
    @CheckForNull
    protected void removeInternal(final long theKey, final Range theRemoveCols) {
        if (theRemoveCols.empty())
            return;
        final Columns cols = getInternal(theKey, theRemoveCols);
        if (cols == null)
            return;
        final LongArrayList removeList = new LongArrayList();
        removeList.add(cols.columns(), 0, cols.columns().length);
        removeInternal(theKey, removeList);
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

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractTable#sizeInternal()
     */
    @Override
    protected long sizeInternal() {
        long start = 0;
        long lastEnd = 0;
        long count = 0;
        Long startKey = null;
        try {
            setks();
            /** Following while loop loads around ROW_SLICE_COUNT rows per
             * iteration using get_range_slices query. The Last key found in the
             * previous iteration becomes 'start key' in the current iteration. */
            while (true) { // $codepro.audit.disable
                           // constantConditionalExpression
                final SliceRange range = constructRange(0, Long.MIN_VALUE,
                        Long.MAX_VALUE, true);
                final KeyRange kRange = getKeyRange(startKey);
                final SlicePredicate predicate = new SlicePredicate();
                predicate.setSlice_range(range);
                final List<KeySlice> res = server().get_range_slices(
                        getColumnParent(), predicate, kRange,
                        DEFAULT_CONSISTENCY_LEVEL);
                final int rowCount = res.size();
                if (rowCount == 0) {
                    break;
                } else {
                    final KeySlice last = res.get(res.size() - 1);
                    start = ByteBufferUtil.toLong(last.key);
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
        } catch (final Exception e) {
            LOG.error("Exception Occurred in - sizeInternal()", e);
            throw new DBException("Error getting table size.", e);
        }
        return count;
    }

    /** Called when the table was dropped. */
    public void dropped() {
        try {
            setks();
            final String tbl = server().system_drop_column_family(
                    table.toFixedString());
            LOG.debug("Table dropped >" + tbl);
        } catch (final Exception e) {
            LOG.error("Exception Occurred in - dropped()", e);
            throw new DBException("Error While dropping Table.", e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.BETable#getLimited(long, int)
     */
    @Override
    @CheckForNull
    public Columns getLimited(final long theKey, final int theCount) {
        final List<ColumnOrSuperColumn> rowData = executeColumnQuery(theKey,
                FULL_RANGE, theCount, null);
        if (rowData == null)
            return null;
        return createColumns(rowData);
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
