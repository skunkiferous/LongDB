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
// $codepro.audit.disable exceptionUsage.exceptionCreation
package com.blockwithme.longdb.entities;

import static com.blockwithme.longdb.common.constants.ByteConstants.BYTE_MASK;
import static com.blockwithme.longdb.common.constants.ByteConstants.INT_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.LONG_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.SHORT_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.SHORT_MASK;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import com.blockwithme.longdb.util.Util;

/** Represents an InputStream and a DataInput over a Bytes object. WARNING: Note
 * that the DataInput expects that values to be saves in *little endian*, as
 * opposed to Sun's standard of big endian. That means that data saved with
 * Sun's DataOutputStream will not be readable with this stream. Data should be
 * saved using BytesOutputStream or the marshaling methods of Util instead. */
public final class BytesInputStream extends InputStream implements DataInput {

    /** Read buffer size */
    private static final int READ_BUF_SIZE = 4;

    /** The bytes. */
    private final Bytes bytes;

    /** The count. */
    private final long count;

    /** The mark. */
    private long mark;

    /** The current position. */
    private long pos;

    /** The read size buffer. */
    private byte[] readSizeBuf;

    /** Instantiates a new bytes input stream.
     * 
     * @param theBytes
     *        contents */
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "CD_CIRCULAR_DEPENDENCY")
    BytesInputStream(final Bytes theBytes) {
        this.bytes = theBytes;
        count = theBytes.length();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.InputStream#available()
     */
    @Override
    public int available() {
        final long avail = count - pos;
        return (avail > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) avail;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.InputStream#close()
     */
    @Override
    public void close() {
        // NOP
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.InputStream#mark(int)
     */
    @Override
    public void mark(final int theReadAheadLimit) {
        mark = pos;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.InputStream#markSupported()
     */
    @Override
    public boolean markSupported() {
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.InputStream#read()
     */
    @Override
    public int read() {
        return (pos < count) ? (bytes.get(pos++) & BYTE_MASK) : -1;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.InputStream#read(byte[], int, int)
     */
    @Override
    public int read(final byte[] theBuffer, final int theOffset, int theLength) {
        final long avail = count - pos;
        if (theLength > avail) {
            // Adjust before, because bytes.get() fails if not enough available
            theLength = (int) avail;
        }
        bytes.copyTo(theBuffer, theOffset, theLength, pos);
        pos += theLength;
        return theLength;
    }

    /** Read all bytes left, possibly returning the original Bytes array.
     * 
     * @param isModifiable
     *        if result is modifiable
     * @return the resultant byte[] */
    public byte[] readAll(final boolean isModifiable) {
        final byte[] resultArray;
        if (pos == 0) {
            resultArray = bytes.toArray(isModifiable);
        } else {
            resultArray = new byte[available()];
            bytes.copyTo(resultArray, 0, resultArray.length, pos);
        }
        pos += resultArray.length;
        return resultArray;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataInput#readBoolean()
     */
    @Override
    public boolean readBoolean() throws IOException {
        return readByte() != 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataInput#readByte()
     */
    @Override
    public byte readByte() throws IOException {
        if (pos >= count)
            throw new EOFException();
        return bytes.get(pos++);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataInput#readChar()
     */
    @Override
    public char readChar() throws IOException {
        return (char) readShort();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataInput#readDouble()
     */
    @Override
    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataInput#readFloat()
     */
    @Override
    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataInput#readFully(byte[])
     */
    @Override
    public void readFully(final byte[] theByteArray) throws IOException {
        readFully(theByteArray, 0, theByteArray.length);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataInput#readFully(byte[], int, int)
     */
    @Override
    public void readFully(final byte[] theBuffer, final int theOffset,
            final int theLength) throws IOException {
        if (theLength < 0)
            throw new IndexOutOfBoundsException();
        final long avail = count - pos;
        if (theLength > avail) {
            throw new EOFException();
        }
        bytes.copyTo(theBuffer, theOffset, theLength, pos);
        pos += theLength;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataInput#readInt()
     */
    @Override
    public int readInt() throws IOException {
        if ((count - pos) < INT_BYTES)
            throw new EOFException();
        final int result = bytes.getInt(pos);
        pos += INT_BYTES;
        return result;
    }

    /** Reads a line.
     * 
     * @return the line string
     * @throws IOException
     *         Signals that an I/O exception has occurred.
     * @deprecated Method deprecated. */
    @Override
    @Deprecated
    public String readLine() throws IOException {
        throw new IOException("deprecated!");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataInput#readLong()
     */
    @Override
    public long readLong() throws IOException {
        if ((count - pos) < LONG_BYTES)
            throw new EOFException();
        final long result = bytes.getLong(pos);
        pos += LONG_BYTES;
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataInput#readShort()
     */
    @Override
    public short readShort() throws IOException {
        if ((count - pos) < SHORT_BYTES)
            throw new EOFException();
        final short result = bytes.getShort(pos);
        pos += SHORT_BYTES;
        return result;
    }

    /** Reads a 31 bit unsigned int, possibly packed in 2 bytes.
     * 
     * @return the resultant int
     * @throws IOException
     *         Signals that an I/O exception has occurred. */
    public int readSize() throws IOException {
        byte[] buf = readSizeBuf;
        if (buf == null) {
            buf = readSizeBuf = new byte[READ_BUF_SIZE];
        }
        final int avail = available();
        if (avail >= READ_BUF_SIZE) {
            read(buf);
            pos -= READ_BUF_SIZE;
            final int result = Util.readSize(buf, 0);
            pos += Util.bytesForSize(result);
            return result;
        }
        if (avail >= 2) {
            read(buf, 0, 2);
            buf[2] = buf[3] = 0;
            pos -= 2;
            final int result = Util.readSize(buf, 0);
            final int used = Util.bytesForSize(result);
            if (used == 2) {
                pos += 2;
                return result;
            }
        }
        throw new IOException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataInput#readUnsignedByte()
     */
    @Override
    public int readUnsignedByte() throws IOException {
        return readByte() & BYTE_MASK;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataInput#readUnsignedShort()
     */
    @Override
    public int readUnsignedShort() throws IOException {
        return readShort() & SHORT_MASK;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataInput#readUTF()
     */
    @Override
    public String readUTF() throws IOException {
        return DataInputStream.readUTF(this);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.InputStream#reset()
     */
    @Override
    public void reset() {
        pos = mark;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.InputStream#skip(long)
     */
    @Override
    public long skip(long theNumber) {
        if ((pos + theNumber) > count) {
            theNumber = count - pos;
        }
        if (theNumber < 0) {
            return 0;
        }
        pos += theNumber;
        return theNumber;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataInput#skipBytes(int)
     */
    @Override
    public int skipBytes(final int theNumber) throws IOException {
        return (int) skip(theNumber);
    }
}
