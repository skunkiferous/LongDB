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

import java.io.Serializable;
import java.util.Arrays;

import javax.annotation.ParametersAreNonnullByDefault;

/** Keys are of two types : <br>
 * A. 16 bytes key combining rowId and columId. Blobs are stored against these.<br>
 * B. 8 bytes key containing rowIds only, colum ids are stored against these.<br>
 * Keys representing same row are considered to be duplicate keys. <br>
 * Compares two keys if the first eight bytes are same and length are same it
 * considers to be duplicate. */
@ParametersAreNonnullByDefault
@SuppressWarnings("serial")
public class DuplicateKeyComparator implements java.util.Comparator<byte[]>,
        Serializable {

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
     */
    @Override
    public int compare(final byte[] theFirstArray, final byte[] theSecondArray) {
        final Long r1 = DataConversionUtil.toLong(Arrays.copyOfRange(
                theFirstArray, 0, 8));
        final Long r2 = DataConversionUtil.toLong(Arrays.copyOfRange(
                theSecondArray, 0, 8));
        final int result = r1.compareTo(r2);
        if (result != 0)
            return result;
        else if (theFirstArray.length == theSecondArray.length)
            return 0;
        else
            return (theFirstArray.length > theSecondArray.length) ? 1 : -1;
    }
}
