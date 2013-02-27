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
package com.blockwithme.longdb.client.io.json;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Writer;

import javax.annotation.ParametersAreNonnullByDefault;

import org.codehaus.jackson.Base64Variants;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import com.blockwithme.longdb.Columns;
import com.blockwithme.longdb.client.entities.Row;
import com.blockwithme.longdb.client.io.DataInput;
import com.blockwithme.longdb.client.io.DataOutput;
import com.blockwithme.longdb.entities.Bytes;
import com.blockwithme.longdb.entities.LongHolder;
import com.blockwithme.longdb.util.DBUtils;

/** Implementation of DataOutput interface which prints data coming from any
 * DataInput interface to a output writer in JSON format.
 * 
 * @see DataOutput, DataInput */
@ParametersAreNonnullByDefault
public class JSONDataOutput implements DataOutput {

    /** The output writer */
    private final Writer out;

    /** The verbose flag */
    private boolean verbose;

    /** The verbose stream. */
    private PrintStream verboseStream;

    /** @param theWriter
     *        output writer */
    public JSONDataOutput(final Writer theWriter) {
        this.out = theWriter;
    }

    /** Verbose options.
     * 
     * @return true, if enabled */
    private boolean verboseOpts() {
        return verbose && verboseStream != null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.client.io.DataOutput#description()
     */
    @Override
    public String description() {
        return "JSON Ouput Stream";
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.blockwithme.longdb.client.io.DataOutput#dump(com.blockwithme.longdb.client
     * .io.DataInput)
     */
    @Override
    public void dump(final DataInput theInput) {
        JsonGenerator gen = null;
        long rowcount = 0;
        long colcount = 0;
        if (!verboseOpts()) {
            this.verboseOptions(theInput.getLogStream(), theInput.verbose());
        }
        try {
            if (verboseOpts())
                verboseStream.print("Start writing data from "
                        + theInput.description() + " To " + this.description());

            final JsonFactory factory = new JsonFactory();
            gen = factory.createJsonGenerator(out);

            gen.useDefaultPrettyPrinter();
            gen.writeStartObject();
            gen.writeFieldName("row");
            gen.writeStartArray();

            while (theInput.hasNext()) {
                rowcount++;
                final Row r = theInput.next();
                gen.writeStartObject();
                gen.writeNumberField("rid", r.rowId());
                final Columns cols = r.columns();
                colcount += cols.size();
                /*
                 * if (verboseOpts())
                 * verboseStream.print("Writing to JSON OuputStream row:" +
                 * r.rowId() + " columns:" +
                 * Arrays.toString(r.columns().columns()));
                 */

                gen.writeFieldName("col");
                gen.writeStartArray();
                for (final LongHolder longHolder : cols) {
                    gen.writeStartObject();
                    gen.writeNumberField("cid", longHolder.value());
                    gen.writeFieldName("blob");
                    final Bytes byts = cols.getBytes(longHolder.value());
                    assert (byts != null);
                    final byte[] blob = byts.toArray(false);
                    gen.writeBinary(Base64Variants.MIME_NO_LINEFEEDS, blob, 0,
                            blob.length);
                    gen.writeEndObject();
                }
                gen.writeEndArray();
                gen.writeEndObject();
                gen.flush();
            }
            gen.writeEndArray();
            gen.writeEndObject();
            gen.flush();

            if (verboseOpts())
                verboseStream.print("Written " + rowcount + " rows and "
                        + colcount + " columns to JSON OuputStream");
        } catch (final IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            try {
                gen.close();
            } catch (final Exception e) {
                // ignore
            }
            DBUtils.closeQuitely(out);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.client.io.DataOutput#getLogStream()
     */
    @Override
    public PrintStream getLogStream() {
        return verboseStream;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.client.io.DataOutput#verbose()
     */
    @Override
    public boolean verbose() {
        return verbose;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.blockwithme.longdb.client.io.DataOutput#verboseOptions(java.io.PrintStream
     * , boolean)
     */
    @Override
    public void verboseOptions(final PrintStream thePrintStrm,
            final boolean isVerbose) {
        verboseStream = thePrintStrm;
        this.verbose = isVerbose;
    }
}
