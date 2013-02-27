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
// $codepro.audit.disable unnecessaryImport
package com.blockwithme.longdb.client.io.csv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.commons.codec.binary.Base64;

import com.blockwithme.longdb.Columns;
import com.blockwithme.longdb.client.entities.Row;
import com.blockwithme.longdb.entities.Bytes;
import com.carrotsearch.hppc.LongObjectOpenHashMap;

// TODO: Auto-generated Javadoc
// $codepro.audit.disable unnecessaryImport

/** DataInput that reads data in CSV (character separated values) format. This
 * class reads data line by line, it expects each line to be in following format
 * : "row_id" "tcol_id" "base64(blob)" (fields separated by tabs). <br>
 * "base64(blob)" is base64 encoded blob data.
 * 
 * @see CSVDataOutput */
@ParametersAreNonnullByDefault
public class CSVDataInput extends BufferedReaderDataInput {

    /** The line count. */
    private int linecount;

    /** The verbose flag. */
    private boolean verbose;

    /** The verbose stream. */
    private PrintStream verboseStream;

    /** Instantiates a new an instance of CSVDataInput.
     * 
     * @param theInReader the source input stream. */
    public CSVDataInput(final Reader theInReader) {
        super(new BufferedReader(theInReader));
    }

    /** Verbose opts. */
    private boolean verboseOpts() {
        return verbose && verboseStream != null;
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.client.io.csv.BufferedReaderDataInput#nextEntry() */
    @Override
    protected String nextEntry() {
        try {
            String line = reader.readLine();
            while (line != null && (line.startsWith("#") || line.isEmpty()))
                line = reader.readLine();
            return line;
        } catch (final IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.client.io.csv.BufferedReaderDataInput#parse(java
     * .lang.String) */
    @Override
    protected Row parse(final String theNextEntry) throws ParsingException {

        String rowStr = null;
        String colStr = null;
        byte[] blob = null;

        try {

            final Columns cols = new Columns(false);
            final LongObjectOpenHashMap<Bytes> map = new LongObjectOpenHashMap<Bytes>();
            final int t1 = theNextEntry.indexOf('\t');
            final int t2 = theNextEntry.indexOf('\t', t1 + 1);
            rowStr = theNextEntry.substring(0, t1).trim();
            colStr = theNextEntry.substring(t1, t2).trim();
            final String object = theNextEntry.substring(t2 + 1).trim();
            final Long roid = Long.valueOf(rowStr);
            final Long coid = Long.valueOf(colStr);
            blob = Base64.decodeBase64(object.getBytes("UTF-8"));
            final Bytes byts = new Bytes(blob);
            map.put(coid, byts);
            cols.putAll(map);
            linecount++;
            if (verboseOpts())
                verboseStream.println("Parsed line#" + linecount + " rowId:"
                        + rowStr + " colId:" + colStr + " blob size:"
                        + blob.length);
            return new Row(roid, cols);
        } catch (final NumberFormatException | UnsupportedEncodingException e) {
            final String msg = "Error parsing row :" + rowStr + " colId:"
                    + colStr + " blob size:"
                    + (blob != null ? blob.length : "0");
            throw new ParsingException(msg, e);
        }
    }

    /* (non-Javadoc)
     * @see com.blockwithme.longdb.client.io.DataInput#description() */
    @Override
    public String description() {
        return "CVS Input Source";
    }

    /* (non-Javadoc)
     * @see com.blockwithme.longdb.client.io.DataInput#getLogStream() */
    @Override
    public PrintStream getLogStream() {
        return verboseStream;
    }

    /* (non-Javadoc)
     * @see com.blockwithme.longdb.client.io.DataInput#verbose() */
    @Override
    public boolean verbose() {
        return verbose;
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.client.io.DataInput#verboseOptions(java.io.PrintStream
     * , boolean) */
    @Override
    public void verboseOptions(final PrintStream thePrintStrm,
            final boolean theVerbose) {
        verboseStream = thePrintStrm;
        this.verbose = theVerbose;
    }
}
