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
package com.blockwithme.longdb.client.io;

import java.io.Closeable;
import java.io.PrintStream;
import java.util.Iterator;

import javax.annotation.ParametersAreNonnullByDefault;

import com.blockwithme.longdb.client.entities.Row;

/** This interface represents Data Source in form of an iterator, from where Data
 * can be extracted Row by Row. The source of data can be either a file or a DB
 * table. Iterator methods hasNext() and next() can be used to iterate over
 * rows. */
@ParametersAreNonnullByDefault
public interface DataInput extends Iterator<Row>, Closeable {

    /** Short Description of DataInput Stream/Table.
     * 
     * @return the description string */
    String description();

    /** Returns verbose PrintStream.
     * 
     * @return printStream set the Print Stream to print verbose events */
    PrintStream getLogStream();

    /** Verbose flag.
     * 
     * @return the verbose flag */
    boolean verbose();

    /** To specify verbose options like - verbose flag and Stream where verbose
     * information needs to be printed.
     * 
     * @param thePrintStrm
     *        set the Print Stream to print verbose events
     * @param theVerboseFlag
     *        the verbose flag */
    void verboseOptions(final PrintStream thePrintStrm,
            final boolean theVerboseFlag);

}
