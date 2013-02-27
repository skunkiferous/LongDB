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

import java.io.PrintStream;

import javax.annotation.ParametersAreNonnullByDefault;

/** This interface represents a data 'sink' where data from 'DataInput' interface
 * can be dumped. The dump() implementation goes over data available from data
 * source saves it in a persistent data storage like database of file.
 * (depending on its implementation) */
@ParametersAreNonnullByDefault
public interface DataOutput {

    /** Description of Data Output Interface.
     * 
     * @return the description string */
    String description();

    /** This method iterates over data available from data source, saves it in a
     * persistent data storage like database of file
     * 
     * @param theSource
     *        the source object */
    void dump(DataInput theSource);

    /** Returns log stream used for verbose purpose. This is *not* the stream
     * where data is being written
     * 
     * @return the log stream */
    PrintStream getLogStream();

    /** @return verbose flag */
    boolean verbose();

    /** To specify verbose options like - verbose flag and Stream where verbose
     * information needs to be printed.
     * 
     * @param thePrintStream
     *        set the PrintStream to print verbose events.
     * @param theVerboseFlag
     *        the verbose flag */
    void verboseOptions(PrintStream thePrintStream, boolean theVerboseFlag);
}
