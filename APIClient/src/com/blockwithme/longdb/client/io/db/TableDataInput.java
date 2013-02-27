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
package com.blockwithme.longdb.client.io.db;

import java.io.PrintStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.annotation.ParametersAreNonnullByDefault;

import com.blockwithme.longdb.BETable;
import com.blockwithme.longdb.Columns;
import com.blockwithme.longdb.Range;
import com.blockwithme.longdb.client.entities.Row;
import com.blockwithme.longdb.client.entities.RowFilter;
import com.blockwithme.longdb.client.io.DataInput;
import com.blockwithme.longdb.entities.LongHolder;
import com.carrotsearch.hppc.cursors.LongCursor;

// TODO: Auto-generated Javadoc
/** Provides DataInput (Row iterator) based on a BETable. Filtered Iterator can
 * be also created by passing RowFilter instance. This class is used in order to
 * export table data to external data streams. */
@ParametersAreNonnullByDefault
public class TableDataInput implements DataInput {

    /** The filter. */
    private RowFilter filter;

    /** The row iterator. */
    private Iterator<LongHolder> rowIterator;

    /** The table. */
    private final BETable table;

    /** The verbose. */
    private boolean verbose;

    /** The verbose stream. */
    private PrintStream verboseStream;

    /** Constructor which provides iterator on all the rows of the table.
     * 
     * @param theTable the table */
    public TableDataInput(final BETable theTable) {
        this.table = theTable;
    }

    /** Constructor which provides filtered iterator based on the filter passed.
     * 
     * @param theTable the table
     * @param theFilter the row filter to be assigned */
    public TableDataInput(final BETable theTable, final RowFilter theFilter) {
        this.table = theTable;
        this.filter = theFilter;
    }

    /** Closes the iterator. */
    @Override
    public void close() {
        table.close(); // $codepro.audit.disable closeInFinally
    }

    /* (non-Javadoc)
     * @see com.blockwithme.longdb.client.io.DataInput#description() */
    @Override
    public String description() {
        return "Input source:" + table.table();
    }

    /** Returns 'Output Stream' used to write verbose information. This is NOT
     * the stream where the data is exported.
     * 
     * @return the log stream */
    @Override
    public PrintStream getLogStream() {
        return verboseStream;
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#hasNext() */
    @SuppressWarnings("unchecked")
    @Override
    public boolean hasNext() {
        // TODO This method is MUCH too long. We have to split it.
        if (rowIterator == null) {

            if (filter == null)
                rowIterator = table.keys(); // All keys in the table.

            else if (filter.inList() != null) {
                /* TODO - remove this workaround when API allows filtering on
                 * rowIds. The following inner class iterates filter.inList()
                 * and for each element in list checks if the table has that
                 * row. */
                rowIterator = new Iterator<LongHolder>() {
                    Iterator<LongCursor> itr;

                    LongHolder nextVal;
                    {
                        // TODO sort this list first.
                        // TODO Isn't the next IF always true, since we checked
                        // it already?
                        if (filter != null && filter.inList() != null) {
                            itr = filter.inList().iterator();
                        } else
                            itr = Collections.EMPTY_SET.iterator();
                    }

                    @Override
                    public boolean hasNext() {
                        if (nextVal != null)
                            return true;
                        while (itr.hasNext()) {
                            final LongHolder v = new LongHolder();
                            v.value(itr.next().value);
                            if (table.exists(v.value())) {
                                nextVal = v;
                                return true;
                            }
                        }
                        return false;
                    }

                    @Override
                    public LongHolder next() {
                        if (!hasNext())
                            throw new NoSuchElementException(
                                    "No more elements.");
                        final LongHolder currVal = nextVal;
                        nextVal = null;
                        return currVal;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException(
                                "Remove not supported!");
                    }
                };
            } else

                /* TODO - remove this workaround when API allows filtering on
                 * rowIds - ranges. The following inner class iterates
                 * tables.keys() and checks rowId is within range. */
                rowIterator = new Iterator<LongHolder>() {
                    boolean endRange;

                    Iterator<LongHolder> itr = table.keys();

                    LongHolder nextVal;

                    Range rng = filter.range();

                    @Override
                    public boolean hasNext() {

                        if (nextVal != null)
                            return true;

                        if (endRange)
                            return false;

                        while (itr.hasNext()) {
                            final LongHolder v = itr.next();
                            if (rng.start() <= v.value()
                                    && rng.end() >= v.value()) {
                                nextVal = v;
                                return true;
                            } else if (v.value() > rng.end()) {
                                endRange = true;
                                break;
                            }
                        }
                        return false;
                    }

                    @Override
                    public LongHolder next() {
                        if (!hasNext())
                            throw new NoSuchElementException(
                                    "No more elements.");
                        final LongHolder currVal = nextVal;
                        nextVal = null;
                        return currVal;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException(
                                "Remove not supported!");
                    }
                };
        }
        return rowIterator.hasNext();
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#next() */
    @Override
    public Row next() {
        if (!hasNext())
            throw new NoSuchElementException("No more rows available.");
        final LongHolder ridHldr = rowIterator.next();
        final Columns cols = table.get(ridHldr.value());
        if (cols == null) {
            // This is the case where rowId without columns exists.
            // this should never happen ideally, but if it happens the row
            // will be ignored
            return next();
        }
        return new Row(ridHldr.value(), cols);
    }

    /** Method not implemented throws exception. */
    @Override
    public void remove() {
        throw new UnsupportedOperationException("Remove not supported");
    }

    /** Returns verbose flag.
     * 
     * @return verbose flag */
    @Override
    public boolean verbose() {
        return verbose;
    }

    /** Sets 'verbose flag' and 'Output Stream' to write verbose information,
     * while exporting data. This is NOT the stream where the data is exported.
     * 
     * @param thePrintStream the printStream
     * @param isVerbose the verbose flag. */
    @Override
    public void verboseOptions(final PrintStream thePrintStream,
            final boolean isVerbose) {
        verboseStream = thePrintStream;
        this.verbose = isVerbose;
    }
}
