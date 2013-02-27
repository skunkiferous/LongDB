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
package com.blockwithme.longdb.leveldb;

import static com.blockwithme.longdb.common.constants.ByteConstants.BYTE_MASK;

import java.io.Serializable;

import javax.annotation.ParametersAreNonnullByDefault;

/** Comparator that compares two byte arrays, byte wise starting from first array
 * element. */
@SuppressWarnings("serial")
@ParametersAreNonnullByDefault
public class BytewiseComparator implements java.util.Comparator<byte[]>,
        Serializable {

    /** Compares two byte arrays, byte wise starting from first array element. */
    @Override
    public int compare(final byte[] theLeftBytes, final byte[] theRightBytes) {
        for (int i = 0, j = 0; i < theLeftBytes.length
                && j < theRightBytes.length; i++, j++) {
            final int a = (theLeftBytes[i] & BYTE_MASK);
            final int b = (theRightBytes[j] & BYTE_MASK);
            if (a != b) {
                return a - b;
            }
        }
        return theLeftBytes.length - theRightBytes.length;
    }
}
