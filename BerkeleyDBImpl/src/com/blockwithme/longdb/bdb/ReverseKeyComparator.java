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

import javax.annotation.ParametersAreNonnullByDefault;

/** Keys are of two types : <br>
 * A. 16 bytes key combining rowId and columId. Blobs are stored against these.<br>
 * B. 8 bytes key containing rowIds only, columIds are stored against these.<br>
 * This comparator will parse the keys and finds : <br>
 * 1. If rowIDs are different, then returns the comparative values of rowIds.<br>
 * 2. If the rowIDs are same and keys don't have columnIds (i.e. 8 byte keys)
 * returns zero. <br>
 * 3. For 16 byte keys having same rowIds, it compares the columnId values and
 * returns the 'REVERSE' value. */
@ParametersAreNonnullByDefault
@SuppressWarnings("serial")
public class ReverseKeyComparator implements java.util.Comparator<byte[]>,
        Serializable {

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
     */
    @Override
    public int compare(final byte[] theFirstArray, final byte[] theSecondArray) {
        // DRY! Don't-Repeat-Yourself!
        return -KeyComparator.cmp(theFirstArray, theSecondArray);
    }
}
