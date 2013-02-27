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
package com.blockwithme.longdb.tools;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.CRC32;
import java.util.zip.GZIPOutputStream;

import javax.annotation.ParametersAreNonnullByDefault;

/** GZip Output Stream extends GZIPOutputStream and provides accessor method of
 * protected field CRC32. So that CRC value can be accessed after writing Data
 * into stream */
@ParametersAreNonnullByDefault
public class CRCGZipOutputStream extends GZIPOutputStream {
    /** Instantiates a new CRCGZipOutputStream,
     * 
     * @param theOutStrm
     *        the output stream
     * @throws IOException
     *         Signals that an I/O exception has occurred. */
    public CRCGZipOutputStream(final OutputStream theOutStrm)
            throws IOException {
        super(theOutStrm);
    }

    /** Gets the CRC, Call this method to get the CRC value of bytes written so
     * far.
     * 
     * @return the crc value */
    public CRC32 getCRC() {
        return crc;
    }

    /** Sets the dictionary.
     * 
     * @param theDictionaryData
     *        the new dictionary */
    public void setDictionary(final byte[] theDictionaryData) {
        def.setDictionary(theDictionaryData);
    }

    /** Called at the time of each write operation to update the CRC value.
     * 
     * @param theInputArray
     *        the input */
    public void updateCRC(final byte[] theInputArray) {
        crc.update(theInputArray);
    }
}
