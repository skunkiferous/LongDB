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
package com.blockwithme.longdb.client.io.csv;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.commons.codec.binary.Base64;

import com.blockwithme.longdb.Columns;
import com.blockwithme.longdb.client.entities.Row;
import com.blockwithme.longdb.client.io.DataInput;
import com.blockwithme.longdb.client.io.DataOutput;
import com.blockwithme.longdb.entities.Bytes;
import com.blockwithme.longdb.entities.LongHolder;
import com.blockwithme.longdb.util.DBUtils;

// TODO: Auto-generated Javadoc
/** Implementation of DataOutput interface which prints values to an output
 * writer in 'Character Separated Values' format
 * 
 * @see CSVDataInput */
@ParametersAreNonnullByDefault
public class CSVDataOutput implements DataOutput {

    /** The file HEADER. */
    private static final String HEADER = "#row_id\tcol_id\tbase64(blob)\n";

    /** The output writer. */
    private final PrintWriter out;

    /** The verbose flag. */
    private boolean verbose;

    /** The verbose stream. */
    private PrintStream verboseStream;

    /** Instantiates a new CSVDataOutput. */
    public CSVDataOutput(final PrintWriter theOut) {
        this.out = theOut;
    }

    /** Verbose opts. */
    private boolean verboseOpts() {
        return verbose && (verboseStream != null);
    }

    /* (non-Javadoc)
     * @see com.blockwithme.longdb.client.io.DataOutput#description() */
    @Override
    public String description() {
        return "CVS Output Stream";
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
            out.print(HEADER);
            if (verboseOpts())
                verboseStream
                        .println("Start writing data from "
                                + theSource.description() + " To "
                                + this.description());

            final StringBuilder buff = new StringBuilder();
            while (theSource.hasNext()) {
                final Row r = theSource.next();
                rowcount++;
                final Columns cols = r.columns();
                colcount += cols.size();
                /* if (verboseOpts())
                 * verboseStream.print("Writing to CVS OuputStream row:" +
                 * r.rowId() + " columns:" +
                 * Arrays.toString(r.columns().columns())); */
                for (final LongHolder col : cols) {
                    buff.setLength(0);
                    buff.append(r.rowId()).append('\t');
                    buff.append(col.value()).append('\t');
                    final Bytes byts = cols.getBytes(col.value());
                    assert (byts != null);
                    final byte[] blob = byts.toArray(false);
                    buff.append(new String(Base64.encodeBase64(blob), "UTF-8"));
                    buff.append('\n');
                    out.print(buff.toString());
                }
            }
            if (verboseOpts())
                verboseStream.print("Written " + rowcount + " rows and "
                        + colcount + " columns to CVS OuputStream");
        } catch (final UnsupportedEncodingException e) {
            throw new IllegalStateException("Error writing to stream ", e);
        } finally {
            DBUtils.closeQuitely(theSource);
            DBUtils.closeQuitely(out);
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
     * @see com.blockwithme.longdb.client.io.DataOutput#verboseOptions(java.io.
     * PrintStream, boolean) */
    @Override
    public void verboseOptions(final PrintStream thePrintStrm,
            final boolean theVerbose) {
        verboseStream = thePrintStrm;
        this.verbose = theVerbose;
    }
}
