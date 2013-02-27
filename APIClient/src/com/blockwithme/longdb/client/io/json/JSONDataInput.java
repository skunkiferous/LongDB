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
import java.io.Reader;
import java.util.NoSuchElementException;

import javax.annotation.ParametersAreNonnullByDefault;

import org.codehaus.jackson.Base64Variants;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

import com.blockwithme.longdb.Columns;
import com.blockwithme.longdb.client.entities.Row;
import com.blockwithme.longdb.client.io.DataInput;
import com.blockwithme.longdb.entities.Bytes;
import com.carrotsearch.hppc.LongObjectOpenHashMap;

/** Implementation of DataInput interface that reads data in 'JSON' format from a
 * Reader. TODO: mention a sample format here
 * 
 * @see DataInput, JSONDataOutput */
@ParametersAreNonnullByDefault
public class JSONDataInput implements DataInput {

    /** The current token. */
    private JsonToken currTkn;

    /** Jackson JSON streaming parser. */
    private final JsonParser parser;

    /** The verbose. */
    private boolean verbose;

    /** The verbose stream. */
    private PrintStream verboseStream;

    /** construct the Json Data source using the reader object */
    public JSONDataInput(final Reader theReader) {
        try {
            final JsonFactory factory = new JsonFactory();
            parser = factory.createJsonParser(theReader);
            // TODO Do we really get the same each time? Should we not validate
            // it?
            parser.nextToken(); // START_OBJECT
            parser.nextToken(); // FIELD_NAME row
            parser.nextToken(); // START_ARRAY
            currTkn = parser.nextToken();
        } catch (final Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /** Parses the element. */
    private void parseElement(final LongObjectOpenHashMap<Bytes> theMap)
            throws IOException {
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            Long cid = null;
            byte[] blob = null;
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                if (parser.getCurrentToken() == JsonToken.FIELD_NAME) {
                    final String fieldname = parser.getCurrentName();
                    if ("cid".endsWith(fieldname)) {
                        parser.nextToken();
                        cid = parser.getLongValue();
                    }
                    if ("blob".endsWith(fieldname)) {
                        parser.nextToken();
                        blob = parser
                                .getBinaryValue(Base64Variants.MIME_NO_LINEFEEDS);
                    }
                    // TODO What if we get something else? Should we not fail?
                }
            }
            theMap.put(cid, new Bytes(blob));
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.Closeable#close()
     */
    @Override
    public void close() throws IOException {
        parser.close(); // $codepro.audit.disable closeInFinally
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.client.io.DataInput#description()
     */
    @Override
    public String description() {
        return "JSON Input Source";
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.client.io.DataInput#getLogStream()
     */
    @Override
    public PrintStream getLogStream() {
        return verboseStream;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext() {
        return currTkn.equals(JsonToken.START_OBJECT);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#next()
     */
    @Override
    public Row next() {
        Row r = null;
        Long rid = null;
        final LongObjectOpenHashMap<Bytes> map = new LongObjectOpenHashMap<Bytes>();
        try {
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                if (parser.getCurrentToken() == JsonToken.FIELD_NAME) {
                    final String fieldname = parser.getCurrentName();
                    if ("rid".endsWith(fieldname)) {
                        parser.nextToken();
                        rid = parser.getLongValue();
                    }
                    // TODO What if we get something else? Should we not fail?
                } else if (parser.getCurrentToken() == JsonToken.START_ARRAY) {
                    parseElement(map);
                }
                // TODO What if we get something else? Should we not fail?
            }
            currTkn = parser.nextToken();
            if (rid != null)
                r = new Row(rid, new Columns(map, false));
            if (r == null)
                throw new NoSuchElementException("No more values.");
            return r;
        } catch (final Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#remove()
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException("Operation not supported.");
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.client.io.DataInput#verbose()
     */
    @Override
    public boolean verbose() {
        return verbose;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.blockwithme.longdb.client.io.DataInput#verboseOptions(java.io.PrintStream
     * , boolean)
     */
    @Override
    public void verboseOptions(final PrintStream thePrintStrm,
            final boolean theVerbose) {
        verboseStream = thePrintStrm;
        this.verbose = theVerbose;
    }
}
