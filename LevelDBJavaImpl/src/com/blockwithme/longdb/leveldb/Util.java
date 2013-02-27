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
import static com.blockwithme.longdb.common.constants.ByteConstants.B_FIVE_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_FOUR_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_ONE_BYTE;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_SEVEN_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_SIX_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_THREE_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_TWO_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.LONG_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.LONG_BYTES_X_2;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import javax.annotation.CheckForNull;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.LongArrayList;

/** The Class Util. */
@ParametersAreNonnullByDefault
public final class Util {

    /** Logger for this class */
    private static final Logger LOG = LoggerFactory.getLogger(Util.class);

    /** first index */
    private static final int INDX1 = 1;

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

    /** Combines two long into 16 byte array.
     * 
     * @param theRowId
     *        the row id
     * @param theColumnId
     *        the column id
     * @return the resultant byte[] */
    public static byte[] combine(final long theRowId, final long theColumnId) {

        final byte[] keyBytes = new byte[LONG_BYTES_X_2];
        final byte[] rbytes = toByta(theRowId);
        final byte[] cbytes = toByta(theColumnId);
        System.arraycopy(rbytes, 0, keyBytes, (LONG_BYTES - rbytes.length),
                rbytes.length);
        System.arraycopy(cbytes, 0, keyBytes, (LONG_BYTES_X_2 - cbytes.length),
                cbytes.length);
        return keyBytes;
    }

    /** Force drop. */
    public static void forceDrop(final File theFile) {
        if (!theFile.isDirectory()) {
            try {
                theFile.setWritable(true);
                FileUtils.forceDelete(theFile);
            } catch (final IOException e) {
                LOG.warn("exception ignored - forceDrop(File f=" + theFile
                        + ")");
                // ignore.
            }
        } else {
            final File[] files = theFile.listFiles();
            for (final File file : files) {
                forceDrop(file);
            }
        }
    }

    /** Index of.
     * 
     * @param theColIds
     *        the all column ids
     * @param theId
     *        the id
     * @return the long */
    public static long indexOf(final byte[] theColIds, final long theId) {
        int offset = 0;
        while (offset + LONG_BYTES < theColIds.length) {
            if (toLong(Arrays.copyOfRange(theColIds, offset,
                    (offset + LONG_BYTES))) == theId)
                return offset;
            offset += LONG_BYTES;
        }
        return -1L;
    }

    /** Split column.
     * 
     * @param theRowColumnPair
     *        the combined row column pair
     * @return the column id */
    public static long splitColumn(final byte[] theRowColumnPair) {
        return toLong(Arrays.copyOfRange(theRowColumnPair, LONG_BYTES,
                LONG_BYTES_X_2));
    }

    /** Split column ids.
     * 
     * @param theColumns
     *        the all columns
     * @return the long array list */
    @CheckForNull
    public static LongArrayList splitColumnIds(final byte[] theColumns) {
        if (theColumns == null || theColumns.length == 0)
            return null;
        long offset = 0;
        final LongArrayList colIds = new LongArrayList();
        while (offset + LONG_BYTES <= theColumns.length) {
            final long colId = Util.toLong(Arrays.copyOfRange(theColumns,
                    (int) offset, (int) (offset + LONG_BYTES)));
            colIds.add(colId);
            offset += LONG_BYTES;
        }
        return colIds;
    }

    /** Split row.
     * 
     * @param theRowColumnPair
     *        the combined row and column pair
     * @return the long row ID */
    public static long splitRow(final byte[] theRowColumnPair) {
        return toLong(Arrays.copyOfRange(theRowColumnPair, 0, LONG_BYTES));
    }

    /** long to byte array.
     * 
     * @param theData
     *        the data
     * @return the byte array */
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

    /** byte array to long.
     * 
     * @param theData
     *        the data
     * @return the long */
    public static long toLong(final byte[] theData) {
        if (theData == null || theData.length != LONG_BYTES)
            return 0x0;
        // ----------
        return // (Below) convert to longs before shift because digits
               // are lost with ints beyond the 32-bit limit
        (long) (BYTE_MASK & theData[0]) << B_SEVEN_BYTES
                | (long) (BYTE_MASK & theData[INDX1]) << B_SIX_BYTES
                | (long) (BYTE_MASK & theData[INDX2]) << B_FIVE_BYTES
                | (long) (BYTE_MASK & theData[INDX3]) << B_FOUR_BYTES
                | (long) (BYTE_MASK & theData[INDX4]) << B_THREE_BYTES
                | (long) (BYTE_MASK & theData[INDX5]) << B_TWO_BYTES
                | (long) (BYTE_MASK & theData[INDX6]) << B_ONE_BYTE
                | (long) (BYTE_MASK & theData[INDX7]) << 0;
    }

    /** Instantiates a new util. */
    private Util() {
    }
}
