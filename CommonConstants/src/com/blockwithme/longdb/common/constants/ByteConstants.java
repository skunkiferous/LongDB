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
/**
 *
 */
package com.blockwithme.longdb.common.constants;

/** The Interface ByteConstants. */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "DM_DEFAULT_ENCODING")
public interface ByteConstants {

    /** Number of bytes in long */
    int LONG_BYTES = 8;

    /** Number of bytes in double */
    int DOUBLE_BYTES = 8;

    /** Number of bytes in int */
    int INT_BYTES = 4;

    /** Number of bytes in float */
    int FLOAT_BYTES = 4;

    /** Number of bytes in char */
    int CHAR_BYTES = 2;

    /** Number of bytes in short */
    int SHORT_BYTES = 2;

    /** Number of bytes in long into 2 */
    int LONG_BYTES_X_2 = LONG_BYTES * 2;

    /** Number of bits in byte */
    int BYTE_BITS = 8;

    /** Number of bits in long */
    int LONG_BITS = LONG_BYTES * BYTE_BITS;

    /** Number of bits in double */
    int DOUBLE_BITS = DOUBLE_BYTES * BYTE_BITS;

    /** Number of bits in int */
    int INT_BITS = INT_BYTES * BYTE_BITS;

    /** Number of bits in float */
    int FLOAT_BITS = FLOAT_BYTES * BYTE_BITS;

    /** Number of bits in char */
    int CHAR_BITS = CHAR_BYTES * BYTE_BITS;

    /** Number of bits in short */
    int SHORT_BITS = SHORT_BYTES * BYTE_BITS;

    /** number of bits in one byte */
    int B_ONE_BYTE = 8;

    /** number of bits in two bytes */
    int B_TWO_BYTES = 16;

    /** number of bits in three bytes */
    int B_THREE_BYTES = 24;

    /** number of bits in four bytes */
    int B_FOUR_BYTES = 32;

    /** number of bits in five bytes */
    int B_FIVE_BYTES = 40;

    /** number of bits in six bytes */
    int B_SIX_BYTES = 48;

    /** number of bits in seven bytes */
    int B_SEVEN_BYTES = 56;

    /** To mask everything except last one byte */
    int BYTE_MASK = 255;

    /** To mask everything except last one short */
    int SHORT_MASK = 0xFFFF;

    /** The byte array representation of blank String */
    byte[] BLANK_STR_BYTES = "".getBytes();

}
