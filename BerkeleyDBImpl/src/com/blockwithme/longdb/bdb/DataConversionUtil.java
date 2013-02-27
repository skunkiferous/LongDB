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

import static com.blockwithme.longdb.common.constants.ByteConstants.BYTE_MASK;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_FIVE_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_FOUR_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_ONE_BYTE;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_SEVEN_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_SIX_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_THREE_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_TWO_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.LONG_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.LONG_BYTES_X_2;

import java.util.Arrays;

import javax.annotation.ParametersAreNonnullByDefault;

import com.google.common.base.Preconditions;
import com.sleepycat.je.DatabaseEntry;

/** Provides utility methods to convert to/from long values to DB entries. */
// TODO The only BDB-specific method is "longtoEntry(long)"
// All the rest should be moved to base-impl project
// CHECKSTYLE IGNORE FOR NEXT 1 LINES
@ParametersAreNonnullByDefault
public class DataConversionUtil {

    /** first index */
    private static final int INDX1 = 1;

    /** Sixteenth byte */
    private static final int INDX16 = 16;

    /** second index */
    private static final int INDX2 = 2;

    /** third index */
    private static final int INDX3 = 3;

    /** fourth index */
    private static final int INDX4 = 4;

    /** fifth index */
    private static final int INDX5 = 5;

    /** sixth index */
    private static final int INDX6 = 6;

    /** seventh index */
    private static final int INDX7 = 7;

    /** eighth byte */
    private static final int INDX8 = 8;

    /** Combines rowId and columnId as a byte array of size 16
     * 
     * @param theRowId
     *        the row id
     * @param theColumnId
     *        the column id
     * @return the combined byte[] */
    public static byte[] combine(final long theRowId, final long theColumnId) {

        final byte[] keyBytes = new byte[LONG_BYTES_X_2];
        final byte[] rbytes = toByta(theRowId);
        final byte[] cbytes = toByta(theColumnId);
        System.arraycopy(rbytes, 0, keyBytes, 0, rbytes.length);
        System.arraycopy(cbytes, 0, keyBytes, rbytes.length, cbytes.length);
        return keyBytes;
    }

    /** Utility method to Convert a long into a DatabaseEntry.
     * 
     * @param theKey
     *        the key
     * @return the database entry */
    public static DatabaseEntry longtoEntry(final long theKey) {
        final DatabaseEntry rowEntry = new DatabaseEntry();
        rowEntry.setData(toByta(theKey));
        return rowEntry;
    }

    /** Splits column the 16 byte array and returns last 8 bytes as column id
     * 
     * @param theCombinedBytes
     *        the combined rowID columnID bytes.
     * @return the last 8 bytes as column id. */
    public static long splitColumn(final byte[] theCombinedBytes) {
        // TODO toLong(Arrays.copyOfRange(...)) is less then optimal...
        // This could make a measurable speed difference if optimized,
        // but since I suspect we're going to completely rewrite it
        // later, we can leave it like that for now ...
        return toLong(Arrays.copyOfRange(theCombinedBytes, INDX8, INDX16));
    }

    /** Splits column the 16 byte array and returns first 8 bytes as row id
     * 
     * @param theCombinedBytes
     *        the combined rowID columnID bytes.
     * @return the first 8 bytes as row id. */
    public static long splitRow(final byte[] theCombinedBytes) {
        // TODO toLong(Arrays.copyOfRange(...)) is less then optimal...
        // This could make a measurable speed difference if optimized,
        // but since I suspect we're going to completely rewrite it
        // later, we can leave it like that for now ...
        return toLong(Arrays.copyOfRange(theCombinedBytes, 0, INDX8));
    }

    /** Converts long to byte array
     * 
     * @param theData
     *        the data
     * @return the byte[] */
    public static byte[] toByta(final long theData) {
        return new byte[] { (byte) ((theData >> B_SEVEN_BYTES) & BYTE_MASK),
                (byte) ((theData >> B_SIX_BYTES) & BYTE_MASK),
                (byte) ((theData >> B_FIVE_BYTES) & BYTE_MASK),
                (byte) ((theData >> B_FOUR_BYTES) & BYTE_MASK),
                (byte) ((theData >> B_THREE_BYTES) & BYTE_MASK),
                (byte) ((theData >> B_TWO_BYTES) & BYTE_MASK),
                (byte) ((theData >> B_ONE_BYTE) & BYTE_MASK),
                (byte) ((theData >> 0) & BYTE_MASK), };
    }

    /** converts byte array to long.
     * 
     * @param theData
     *        the byte array to be converted
     * @return the long */
    public static long toLong(final byte[] theData) {
        Preconditions.checkNotNull(theData, "theData is null");
        if (theData.length != LONG_BYTES)
            throw new IllegalArgumentException("Expected length: " + LONG_BYTES
                    + " Actual length: " + theData.length);
        // ----------
        return // (Below) convert to longs before shift because digits
               // are lost with int's beyond the 32-bit limit
        (long) (BYTE_MASK & theData[0]) << B_SEVEN_BYTES
                | (long) (BYTE_MASK & theData[INDX1]) << B_SIX_BYTES
                | (long) (BYTE_MASK & theData[INDX2]) << B_FIVE_BYTES
                | (long) (BYTE_MASK & theData[INDX3]) << B_FOUR_BYTES
                | (long) (BYTE_MASK & theData[INDX4]) << B_THREE_BYTES
                | (long) (BYTE_MASK & theData[INDX5]) << B_TWO_BYTES
                | (long) (BYTE_MASK & theData[INDX6]) << B_ONE_BYTE
                | (long) (BYTE_MASK & theData[INDX7]) << 0;
    }

}
