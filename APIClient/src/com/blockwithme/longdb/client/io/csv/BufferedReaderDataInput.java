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

import java.io.BufferedReader;
import java.io.IOException;
import java.util.NoSuchElementException;

import javax.annotation.ParametersAreNonnullByDefault;

import com.blockwithme.longdb.client.entities.Row;
import com.blockwithme.longdb.client.io.DataInput;

/** Abstract class provides partial implementation of DataInput for the formats
 * that have one row entry per line e.g. CSV
 * 
 * @see DataInput, CSVDataInput, CSVDataOutput */
@ParametersAreNonnullByDefault
public abstract class BufferedReaderDataInput implements DataInput {

    /** The next line string */
    private String nextline;

    /** Are we closed? */
    private boolean closed;

    /** Buffered reader */
    protected BufferedReader reader;

    /** Instantiates a new InputStreamDataInput.
     * 
     * @param theReader
     *        the 'Reader' that is used for this Data sources. */
    protected BufferedReaderDataInput(final BufferedReader theReader) {
        this.reader = theReader;
    }

    /** Implementation of this method returns a line String that can be passed to
     * 'parse' method to get a 'Row' object.
     * 
     * @return next entry. */
    protected abstract String nextEntry();

    /** Parses the line stream into 'Row' Object.
     * 
     * @param theNextEntry
     *        the next entry returned from nextEntry() method.
     * @return the row object
     * @throws ParsingException */
    protected abstract Row parse(final String theNextEntry)
            throws ParsingException;

    /*
     * (non-Javadoc)
     * 
     * @see java.io.Closeable#close()
     */
    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            reader.close(); // $codepro.audit.disable closeInFinally
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext() {
        if (closed)
            return false;
        if (nextline == null) {
            nextline = nextEntry();
            closed = nextline != null;
        }
        return closed;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#next()
     */
    @Override
    public Row next() {
        while (hasNext()) {
            try {
                return parse(nextline);
            } catch (final ParsingException e) {
                if (verbose()) {
                    e.printStackTrace(getLogStream());
                }
            } finally {
                // Must be cleared, even when getting parse exception,
                // otherwise we get an infinite loop!
                nextline = null;
            }
        }

        throw new NoSuchElementException("No more values");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#remove()
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException("Remove not supported");
    }

}
