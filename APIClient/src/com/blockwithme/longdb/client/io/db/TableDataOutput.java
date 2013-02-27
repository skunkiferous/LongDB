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

import javax.annotation.ParametersAreNonnullByDefault;

import com.blockwithme.longdb.BETable;
import com.blockwithme.longdb.client.entities.Row;
import com.blockwithme.longdb.client.io.DataInput;
import com.blockwithme.longdb.client.io.DataOutput;
import com.blockwithme.longdb.util.DBUtils;

// TODO: Auto-generated Javadoc
/** This class is used to dump data from external source into a Database Table. */
@ParametersAreNonnullByDefault
public class TableDataOutput implements DataOutput {

    /** Table where data will be written. */
    private final BETable table;

    /** verbose flag */
    private boolean verbose;

    /** verbose print stream */
    private PrintStream verboseStream;

    /** Instantiates a new table data output.
     * 
     * @param theTable the table object */
    public TableDataOutput(final BETable theTable) {
        this.table = theTable;
    }

    /** indicates if verbose options are on/off */
    private boolean verboseOpts() {
        return verbose && verboseStream != null;
    }

    /* (non-Javadoc)
     * @see com.blockwithme.longdb.client.io.DataOutput#description() */
    @Override
    public String description() {
        return " Table :" + table.table().toString();
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.client.io.DataOutput#dump(com.blockwithme.longdb
     * .client.io.DataInput) */
    @Override
    public void dump(final DataInput theSource) {
        long rowcount = 0;
        long colcount = 0;

        if (!verboseOpts()) {
            this.verboseOptions(theSource.getLogStream(), theSource.verbose());
        }

        try {
            if (verboseOpts())
                verboseStream
                        .print("Start writing data from "
                                + theSource.description() + " To "
                                + this.description());

            while (theSource.hasNext()) {
                final Row r = theSource.next();
                table.set(r.rowId(), r.columns());
                rowcount++;
                colcount += r.columns().size();
                /* Probably we don't want to write each and every rowid and
                 * column id in verbose stream. if (verboseOpts())
                 * verboseStream.print("Updated row:" + r.rowId() + " columns:"
                 * + Arrays.toString(r.columns().columns())); */
            }
            if (verboseOpts())
                verboseStream.print("Written " + rowcount + " rows and "
                        + colcount + " columns to table:" + table);
        } finally {
            DBUtils.closeQuitely(theSource);
            DBUtils.closeQuitely(table);
        }
    }

    /* (non-Javadoc)
     * @see com.blockwithme.longdb.client.io.DataOutput#getLogStream() */
    @Override
    public PrintStream getLogStream() {
        return verboseStream;
    }

    /* (non-Javadoc)
     * @see com.blockwithme.longdb.client.io.DataOutput#verbose() */
    @Override
    public boolean verbose() {
        return verbose;
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.client.io.DataOutput#verboseOptions(java.io.PrintStream
     * , boolean) */
    @Override
    public void verboseOptions(final PrintStream thePrintStrm,
            final boolean isVerbose) {
        verboseStream = thePrintStrm;
        this.verbose = isVerbose;
    }
}
