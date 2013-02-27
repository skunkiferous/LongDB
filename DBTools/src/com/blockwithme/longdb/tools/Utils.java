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
// $codepro.audit.disable methodInvocationInLoopCondition, useBufferedIO, constantConditionalExpression
package com.blockwithme.longdb.tools;

import static com.blockwithme.longdb.common.constants.ByteConstants.BYTE_BITS;
import static com.blockwithme.longdb.common.constants.ByteConstants.BYTE_MASK;
import static com.blockwithme.longdb.common.constants.ByteConstants.INT_BYTES;

import java.io.File;
import java.io.FileInputStream;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.GZIPInputStream;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.commons.io.FileUtils;

/** The class provides some utility methods for DBTool */
@ParametersAreNonnullByDefault
public final class Utils {

    /**  */
    private static final int SECOND_BYTE_MASK = 0xff00;

    /**  */
    private static final int ONE_K = 1024;

    /** Attempts to read file with name - 'OriginalFileName'.crc32 if present :
     * Assumes that the content inside 'OriginalFileName'.crc32 file is equal to
     * the CRC32 value of the Original file. Calculates CRC32 value of the
     * current file and Compares it with the original value. In case if the
     * .crc32 file is not found, or the file size is not 8 bytes this method
     * returns false, If the file is found but the values don't match this
     * method throws a RuntimeExcepiton.
     * 
     * @param theFile
     *        the file
     * @param isCompressed
     *        true if compressed
     * @return true, if checksum successful
     * @throws Exception */
    // TODO: When do we need to differentiate the "no crc file" and
    // "bad crc" cases? Is it not a failure, one way or another?
    public static boolean checkSum(final File theFile,
            final boolean isCompressed) throws Exception {

        final File crcFile = new File(theFile.getAbsolutePath() + ".crc32");
        if (!crcFile.exists() || crcFile.length() != INT_BYTES * 2) {
            return false;
        }
        final String crcCode = FileUtils.readFileToString(crcFile);
        final long expectedCRC = Long.valueOf(crcCode, 16); // $codepro.audit.disable
                                                            // handleNumericParsingErrors

        if (getCRC32(theFile, isCompressed) != expectedCRC)
            throw new RuntimeException("CRC-32 validation failed for file:"
                    + theFile.getAbsolutePath());
        return true;
    }

    /** Reads first 2 bytes, checks if its equal to GZIPInputStream.GZIP_MAGIC
     * 
     * @param theFile
     *        the file
     * @return true, if compressed
     * @throws Exception */
    public static boolean compressed(final File theFile) throws Exception {
        // TODO: I the file name ends with .gz, then I think that we
        // don't need to check
        FileInputStream fis = null;
        try {
            fis = FileUtils.openInputStream(theFile);
            while (true) {
                final byte[] bytes = new byte[2];
                fis.read(bytes);
                final int head = (bytes[0] & BYTE_MASK)
                        | ((bytes[1] << BYTE_BITS) & SECOND_BYTE_MASK);
                return head == GZIPInputStream.GZIP_MAGIC;
            }
        } finally {
            if (fis != null)
                fis.close();
        }
    }

    /** Get checksum CRC32 for the file content.
     * 
     * @param theFile
     *        the file
     * @param isCommpressed
     *        true if commpressed
     * @return the checksum (CRC32) of the file.
     * @throws Exception */
    @SuppressWarnings("resource")
    public static long getCRC32(final File theFile, final boolean isCommpressed)
            throws Exception {
        CheckedInputStream cis = null;
        try {
            final CRC32 checksum = new CRC32();
            // TODO: Would a buffered input stream make this faster?
            cis = new CheckedInputStream((isCommpressed ? new GZIPInputStream(
                    new FileInputStream(theFile))
                    : new FileInputStream(theFile)), checksum);
            final byte[] tempBuf = new byte[ONE_K];
            while (cis.read(tempBuf) >= 0)
                ; // just read the full stream. // $codepro.audit.disable
            return checksum.getValue();
        } finally {
            if (cis != null)
                cis.close();
        }
    }

    /** Hide utility class constructor. */
    private Utils() {
    }
}
