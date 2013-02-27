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
package com.blockwithme.longdb.util;

import static com.blockwithme.longdb.common.constants.ByteConstants.BYTE_MASK;

import com.blockwithme.longdb.entities.Bytes;

/** The BytesUtil class provides utility class for related classes */
public final class BytesUtil {

    /**  */
    private static final int SECOND_BYTE_MASK = 0x7F00;

    /** Reads a variable length non-negative int from a Bytes at the given
     * offset. The number of bytes read is determined with bytesForSize(int).
     * 
     * @param theBytes
     *        the bytes to read from.
     * @param theOffset
     *        the offset
     * @return the resultant int */
    // TODO remove the following comment and fix the magic numbers
    // CHECKSTYLE stop magic number check
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "CD_CIRCULAR_DEPENDENCY")
    public static int readSize(final Bytes theBytes, final int theOffset) {
        final int ch1 = (theBytes.get(theOffset) & BYTE_MASK);
        final int ch2 = (theBytes.get(theOffset + 1) & BYTE_MASK);
        if (ch2 > 127) {
            final int ch3 = (theBytes.get(theOffset + 2) & BYTE_MASK);
            final int ch4 = (theBytes.get(theOffset + 3) & BYTE_MASK);
            return ((ch4 << 23) + (ch3 << 15) + ((ch2 << 8) & SECOND_BYTE_MASK) + (ch1 << 0));
        }
        return (ch2 << 8) + (ch1 << 0);
    }

    // CHECKSTYLE resume magic number check
    /** Internal constructor */
    private BytesUtil() {
    }

}
