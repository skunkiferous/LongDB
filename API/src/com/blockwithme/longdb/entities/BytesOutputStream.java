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
// $codepro.audit.disable useBufferedIO, hidingInheritedFields
package com.blockwithme.longdb.entities;

import static com.blockwithme.longdb.common.constants.ByteConstants.BYTE_MASK;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_FIVE_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_FOUR_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_ONE_BYTE;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_SEVEN_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_SIX_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_THREE_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_TWO_BYTES;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import com.blockwithme.longdb.util.Util;

/** Represents an OutputStream and a DataOutput over a byte[]. WARNING: Note that
 * the DataOutput saves the values in *little endian*, as opposed to Sun's
 * standard of big endian. That means that data saved will not be readable with
 * a DataInputStream. Data should be read using BytesInputStream or the
 * unmarshaling methods of Util instead. */
public final class BytesOutputStream extends ByteArrayOutputStream implements
        DataOutput {

    /** The Initial capacity */
    private static final int INITIAL_CAPACITY_BYTES = 32;

    /** Capacity expanded by 4 bytes */
    private static final int FOUR_BYTES_EXPAND = 4;

    /** Capacity expanded by 8 bytes */
    private static final int EIGHT_BYTES_EXPAND = 8;

    /** The com.paintedboxes.ser where data is stored. */
    private byte[] buf;

    /** The number of valid bytes in the com.paintedboxes.ser. */
    private int count;

    /** Just for UTF8 */
    private DataOutputStream dos;

    /** Creates a new byte array output stream. The capacity initially is 32
     * bytes, though its size increases if necessary. */
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "CD_CIRCULAR_DEPENDENCY")
    public BytesOutputStream() {
        this(INITIAL_CAPACITY_BYTES);
    }

    /** Creates a new byte array output stream, with a com.paintedboxes.ser
     * capacity of the specified size, in bytes.
     * 
     * @param theSize
     *        the initial capacity. */
    public BytesOutputStream(final int theSize) {
        if (theSize < 0) {
            throw new IllegalArgumentException("Negative initial size: "
                    + theSize);
        }
        buf = new byte[theSize];
    }

    /** Extend the size by specified number of bytes. */
    private void extend(final int theBytes) {
        final int newcount = count + theBytes;
        if (newcount > buf.length) {
            buf = Arrays.copyOf(buf, Math.max(buf.length * 2, newcount));
        }
    }

    /** Get the data in Bytes form. */
    public Bytes bytes() {
        return new Bytes(buf, 0, count);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.ByteArrayOutputStream#close()
     */
    @Override
    public void close() throws IOException {
        // NOP
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.ByteArrayOutputStream#reset()
     */
    @Override
    public void reset() {
        count = 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.ByteArrayOutputStream#size()
     */
    @Override
    public int size() {
        return count;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.ByteArrayOutputStream#toByteArray()
     */
    @Override
    public byte toByteArray()[] {
        return Arrays.copyOf(buf, count);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.ByteArrayOutputStream#toString()
     */
    @Override
    public String toString() {
        try {
            return new String(buf, 0, count, "UTF-8");
        } catch (final UnsupportedEncodingException e) {
            throw new IllegalStateException("Impossible! UTF-8 not supported!",
                    e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.ByteArrayOutputStream#toString(java.lang.String)
     */
    @Override
    public String toString(final String theCharsetName)
            throws UnsupportedEncodingException {
        return new String(buf, 0, count, theCharsetName);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.ByteArrayOutputStream#write(byte[], int, int)
     */
    @Override
    public void write(final byte[] theByteArray, final int theOffset,
            final int theLength) {
        if ((theOffset < 0) || (theOffset > theByteArray.length)
                || (theLength < 0)
                || ((theOffset + theLength) > theByteArray.length)
                || ((theOffset + theLength) < 0)) {
            throw new IndexOutOfBoundsException("Invalid offset value.");
        } else if (theLength == 0) {
            return;
        }
        extend(theLength);
        System.arraycopy(theByteArray, theOffset, buf, count, theLength);
        count += theLength;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.ByteArrayOutputStream#write(int)
     */
    @Override
    public void write(final int theByte) {
        extend(1);
        buf[count] = (byte) theByte;
        count++;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataOutput#writeBoolean(boolean)
     */
    @Override
    public void writeBoolean(final boolean theValue) throws IOException {
        extend(1);
        buf[count] = (byte) (theValue ? 1 : 0);
        count++;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataOutput#writeByte(int)
     */
    @Override
    public void writeByte(final int theValue) throws IOException {
        extend(1);
        buf[count] = (byte) theValue;
        count++;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataOutput#writeBytes(java.lang.String)
     */
    @Override
    public void writeBytes(final String theStr) throws IOException {
        final int len = theStr.length();
        extend(len);
        for (int i = 0; i < len; i++) {
            buf[count++] = (byte) theStr.charAt(i);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataOutput#writeChar(int)
     */
    @Override
    public void writeChar(final int theValue) throws IOException {
        extend(2);
        buf[count++] = (byte) ((theValue >>> 0) & BYTE_MASK);
        buf[count++] = (byte) ((theValue >>> B_ONE_BYTE) & BYTE_MASK);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataOutput#writeChars(java.lang.String)
     */
    @Override
    public void writeChars(final String theStr) throws IOException {
        final int len = theStr.length();
        extend(len * 2);
        for (int i = 0; i < len; i++) {
            final int v = theStr.charAt(i);
            buf[count++] = (byte) ((v >>> 0) & BYTE_MASK);
            buf[count++] = (byte) ((v >>> B_ONE_BYTE) & BYTE_MASK);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataOutput#writeDouble(double)
     */
    @Override
    public void writeDouble(final double theValue) throws IOException {
        writeLong(Double.doubleToRawLongBits(theValue));
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataOutput#writeFloat(float)
     */
    @Override
    public void writeFloat(final float theValue) throws IOException {
        writeInt(Float.floatToRawIntBits(theValue));
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataOutput#writeInt(int)
     */
    @Override
    public void writeInt(final int theValue) throws IOException {
        extend(FOUR_BYTES_EXPAND);
        buf[count++] = (byte) ((theValue >>> 0) & BYTE_MASK);
        buf[count++] = (byte) ((theValue >>> B_ONE_BYTE) & BYTE_MASK);
        buf[count++] = (byte) ((theValue >>> B_TWO_BYTES) & BYTE_MASK);
        buf[count++] = (byte) ((theValue >>> B_THREE_BYTES) & BYTE_MASK);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataOutput#writeLong(long)
     */
    @Override
    public void writeLong(final long theValue) throws IOException {
        extend(EIGHT_BYTES_EXPAND);
        buf[count++] = (byte) ((theValue >>> 0) & BYTE_MASK);
        buf[count++] = (byte) ((theValue >>> B_ONE_BYTE) & BYTE_MASK);
        buf[count++] = (byte) ((theValue >>> B_TWO_BYTES) & BYTE_MASK);
        buf[count++] = (byte) ((theValue >>> B_THREE_BYTES) & BYTE_MASK);
        buf[count++] = (byte) ((theValue >>> B_FOUR_BYTES) & BYTE_MASK);
        buf[count++] = (byte) ((theValue >>> B_FIVE_BYTES) & BYTE_MASK);
        buf[count++] = (byte) ((theValue >>> B_SIX_BYTES) & BYTE_MASK);
        buf[count++] = (byte) ((theValue >>> B_SEVEN_BYTES) & BYTE_MASK);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataOutput#writeShort(int)
     */
    @Override
    public void writeShort(final int theValue) throws IOException {
        extend(2);
        buf[count++] = (byte) ((theValue >>> 0) & BYTE_MASK);
        buf[count++] = (byte) ((theValue >>> B_ONE_BYTE) & BYTE_MASK);
    }

    /** Write size.
     * 
     * @param theSize */
    public void writeSize(final int theSize) {
        extend(Util.bytesForSize(theSize));
        count += Util.writeSize(theSize, buf, count);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.ByteArrayOutputStream#writeTo(java.io.OutputStream)
     */
    @Override
    public void writeTo(final OutputStream theOutStrm) throws IOException {
        theOutStrm.write(buf, 0, count);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataOutput#writeUTF(java.lang.String)
     */
    @Override
    public void writeUTF(final String theStr) throws IOException {
        if (dos == null) {
            dos = new DataOutputStream(this);
        }
        dos.writeUTF(theStr);
    }
}
