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
package com.blockwithme.longdb.bdb;

import static com.blockwithme.longdb.common.constants.ByteConstants.LONG_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.LONG_BYTES_X_2;

import java.io.Serializable;
import java.util.Arrays;

import javax.annotation.ParametersAreNonnullByDefault;

/** Keys are of two types : <br>
 * A. 16 bytes key combining rowId and columId. Blobs are stored against these.<br>
 * B. 8 bytes key containing rowIds only, columIds are stored against these.<br>
 * This comparator will parse the keys and finds : <br>
 * 1. If rowIDs are different, then returns the comparative values of rowIds.<br>
 * 2. If the rowIDs are same and keys don't have columnIds (i.e. 8 byte keys)
 * returns zero. <br>
 * 3. For 16 byte keys having same rowIds, it compares the columnId values. */
@ParametersAreNonnullByDefault
@SuppressWarnings("serial")
public class KeyComparator implements java.util.Comparator<byte[]>,
        Serializable {

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
     */
    @Override
    public int compare(final byte[] theFirstArray, final byte[] theSecondArray) {
        return cmp(theFirstArray, theSecondArray);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
     */
    public static int cmp(final byte[] theFirstArray,
            final byte[] theSecondArray) {
        // TODO DataConversionUtil.toLong(Arrays.copyOfRange(...)) is less
        // then optimal... This could make a measurable speed difference if
        // optimized, but since I suspect we're going to completely rewrite it
        // later, we can leave it like that for now ...
        final long r1 = DataConversionUtil.toLong(Arrays.copyOfRange(
                theFirstArray, 0, 8));
        final long r2 = DataConversionUtil.toLong(Arrays.copyOfRange(
                theSecondArray, 0, 8));
        final int result = Long.compare(r1, r2);

        if (result != 0
                || (theFirstArray.length == LONG_BYTES && theSecondArray.length == LONG_BYTES))
            return result;

        else if (theFirstArray.length == LONG_BYTES_X_2
                && theSecondArray.length == LONG_BYTES_X_2) {
            final long c1 = DataConversionUtil.toLong(Arrays.copyOfRange(
                    theFirstArray, 8, 16));
            final long c2 = DataConversionUtil.toLong(Arrays.copyOfRange(
                    theSecondArray, 8, 16));
            return Long.compare(c1, c2);
        } else if (theFirstArray.length == LONG_BYTES
                && theSecondArray.length == LONG_BYTES_X_2) {
            return -1;
        } else
            return 1;

    }
}
