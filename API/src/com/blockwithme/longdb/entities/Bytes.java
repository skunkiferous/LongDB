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
// $codepro.audit.disable com.instantiations.assist.eclipse.analysis.recursiveCallWithNoCheck, exceptionUsage.exceptionCreation, useEquals, useBufferedIO
package com.blockwithme.longdb.entities;

import static com.blockwithme.longdb.common.constants.ByteConstants.BYTE_BITS;
import static com.blockwithme.longdb.common.constants.ByteConstants.BYTE_MASK;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_FIVE_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_FOUR_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_ONE_BYTE;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_SEVEN_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_SIX_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_THREE_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_TWO_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.CHAR_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.DOUBLE_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.FLOAT_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.INT_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.LONG_BITS;
import static com.blockwithme.longdb.common.constants.ByteConstants.LONG_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.SHORT_BYTES;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import com.blockwithme.longdb.util.BytesUtil;
import com.blockwithme.longdb.util.Util;

/** Contains some data in the form of bytes. TODO refactor the Bytes class to use
 * BlowoenSer when it is finished. */
public final class Bytes {

    /** Position of last 'byte' */
    private static final int LONG_INDEX = 7;

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

    /** True if the content must not be modified. */
    private final boolean copyOnWrite;

    /** The bytes, if not a packed primitive. */
    private final byte[] data;

    /** The hashCode */
    private int hashCode;

    /** The length of data to return */
    private final int length;

    /** Reference to another optional Bytes containing more data. */
    private final Bytes next;

    /** The primitive data, if it's a packed primitive. */
    private final long primitive;

    /** The start offset in data. */
    private final int start;

    /** Checks the input.
     * 
     * @param theData
     *        the data
     * @param theStart
     *        the start
     * @param theLength
     *        the length
     * @return the byte[] */
    private static byte[] check(final byte[] theData, final int theStart,
            final int theLength) {
        check(theData.length, theStart, theLength);
        return theData;
    }

    /** Checks the input */
    private static void check(final int theDataLength, final int theStart,
            final int theLength) {
        if (theStart < 0) {
            throw new IllegalArgumentException("start: " + theStart);
        }
        if (theStart > theDataLength) {
            throw new IllegalArgumentException("start: " + theStart
                    + " data.length: " + theDataLength);
        }
        if (theLength < 0) {
            throw new IllegalArgumentException("length: " + theLength);
        }
        final int left = theDataLength - theStart;
        if (left < theLength) {
            throw new IllegalArgumentException("length: " + theLength
                    + " available: " + left);
        }
    }

    /** Packs multiple Bytes into a single Bytes, without adding any data. */
    private static Bytes concat(final int theFirst, final Bytes[] theBytes) {
        final Bytes head = theBytes[theFirst];
        final int left = theBytes.length - theFirst - 1;
        if (left == 0) {
            return head;
        }
        if (head.next == null) {
            return new Bytes(concat(theFirst + 1, theBytes), head.data,
                    head.primitive, head.start, head.length, head.copyOnWrite);
        }
        final Bytes[] tmpBytes = new Bytes[left + 1];
        tmpBytes[0] = head.next;
        System.arraycopy(theBytes, theFirst + 1, tmpBytes, 1, left);
        return new Bytes(concat(0, tmpBytes), head.data, head.primitive,
                head.start, head.length, head.copyOnWrite);
    }

    /** Reads bytes into an array. */
    private static void copyTo(Bytes theBytes, final byte[] theBuffer,
            int theOffset, int theMissing, long theFrom) {
        while (theMissing > 0 && theBytes != null) {
            final int length = theBytes.length;
            final long avail = length - theFrom;
            if (avail > 0) {
                final int copy = (avail > theMissing) ? theMissing
                        : (int) avail;
                final int where = theBytes.start + (int) theFrom;
                if (theBytes.primitive()) {
                    final long primitive = theBytes.primitive;
                    for (int i = 0; i < copy; i++) {
                        theBuffer[theOffset + i] = (byte) (primitive >> (B_ONE_BYTE * (where + i)));
                    }
                } else {
                    System.arraycopy(theBytes.data, where, theBuffer,
                            theOffset, copy);
                }
                theMissing -= copy;
                theFrom += copy;
                theOffset += copy;
            }
            theBytes = theBytes.next;
        }
    }

    /** Packs multiple Bytes into a single Bytes, without adding any data.
     * 
     * @param theBytes
     *        the bytes to be packed
     * @return the resultant bytes */
    public static Bytes concat(final Bytes... theBytes) {
        if ((theBytes == null) || (theBytes.length == 0)) {
            return null;
        }
        return concat(0, theBytes);
    }

    /** Marshal array into Bytes.
     * 
     * @param theArray
     *        the array to be marshaled
     * @return the resultant bytes */
    public static Bytes marshal(final BigInteger[] theArray) {
        final int count = theArray.length;
        final BytesOutputStream dos = new BytesOutputStream();
        dos.writeSize(count);
        try {
            for (int i = 0; i < count; i++) {
                final byte[] tmpArray = theArray[i].toByteArray();
                dos.writeSize(tmpArray.length);
                dos.write(tmpArray);
            }
            return dos.bytes();
        } catch (final IOException e) {
            throw new UndeclaredThrowableException(e);
        } finally {
            try {
                dos.close();
            } catch (final IOException e) { // NOPMD
            }
        }
    }

    /** Marshal array into Bytes.
     * 
     * @param theArray
     *        the array to be marshaled
     * @return the resultant bytes */
    public static Bytes marshal(final boolean[] theArray) {
        // TODO : pack 8 boolean per byte!
        final int count = theArray.length;
        final BytesOutputStream dos = new BytesOutputStream();
        dos.writeSize(count);
        try {
            for (int i = 0; i < count; i++) {
                dos.writeBoolean(theArray[i]);
            }
            return dos.bytes();
        } catch (final IOException e) {
            throw new UndeclaredThrowableException(e);
        } finally {
            try {
                dos.close();
            } catch (final IOException e) { // NOPMD
            }
        }
    }

    /** Marshal array into Bytes.
     * 
     * @param theArray
     *        the array to be marshaled
     * @return the resultant bytes */
    public static Bytes marshal(final Date[] theArray) {
        final int count = theArray.length;
        final BytesOutputStream dos = new BytesOutputStream();
        dos.writeSize(count);
        try {
            for (int i = 0; i < count; i++) {
                dos.writeLong(theArray[i].getTime());
            }
            return dos.bytes();
        } catch (final IOException e) {
            throw new UndeclaredThrowableException(e);
        } finally {
            try {
                dos.close();
            } catch (final IOException e) { // NOPMD
            }
        }
    }

    /** Marshal array into Bytes.
     * 
     * @param theArray
     *        the array to be marshaled
     * @return the resultant bytes */
    public static Bytes marshal(final double[] theArray) {
        final int count = theArray.length;
        final BytesOutputStream dos = new BytesOutputStream();
        dos.writeSize(count);
        try {
            for (int i = 0; i < count; i++) {
                dos.writeDouble(theArray[i]);
            }

            return dos.bytes();
        } catch (final IOException e) {
            throw new UndeclaredThrowableException(e);
        } finally {
            try {
                dos.close();
            } catch (final IOException e) { // NOPMD
            }
        }
    }

    /** Marshal array into Bytes.
     * 
     * @param theArray
     *        the array to be marshaled
     * @return the resultant bytes */
    public static Bytes marshal(final float[] theArray) {
        final int count = theArray.length;
        final BytesOutputStream dos = new BytesOutputStream();
        dos.writeSize(count);
        try {
            for (int i = 0; i < count; i++) {
                dos.writeFloat(theArray[i]);
            }

            return dos.bytes();
        } catch (final IOException e) {
            throw new UndeclaredThrowableException(e);
        } finally {
            try {
                dos.close();
            } catch (final IOException e) { // NOPMD
            }
        }
    }

    /** Marshal array into Bytes.
     * 
     * @param theArray
     *        the array to be marshaled
     * @return the resultant bytes */
    public static Bytes marshal(final int[] theArray) {
        final int count = theArray.length;
        final BytesOutputStream dos = new BytesOutputStream();
        dos.writeSize(count);
        try {
            for (int i = 0; i < count; i++) {
                dos.writeInt(theArray[i]);
            }
            return dos.bytes();
        } catch (final IOException e) {
            throw new UndeclaredThrowableException(e);
        } finally {
            try {
                dos.close();
            } catch (final IOException e) { // NOPMD
            }
        }
    }

    /** Marshal array into Bytes.
     * 
     * @param theArray
     *        the array to be marshaled
     * @return the resultant bytes */
    public static Bytes marshal(final long[] theArray) {
        final int count = theArray.length;
        final BytesOutputStream dos = new BytesOutputStream();
        dos.writeSize(count);
        try {
            for (int i = 0; i < count; i++) {
                dos.writeLong(theArray[i]);
            }
            return dos.bytes();
        } catch (final IOException e) {
            throw new UndeclaredThrowableException(e);
        } finally {
            try {
                dos.close();
            } catch (final IOException e) { // NOPMD
            }

        }
    }

    /** Marshal array into Bytes.
     * 
     * @param theArray
     *        the array to be marshaled
     * @return the resultant bytes */
    public static Bytes marshal(final short[] theArray) {
        final int count = theArray.length;
        final BytesOutputStream dos = new BytesOutputStream();
        dos.writeSize(count);
        try {
            for (int i = 0; i < count; i++) {
                dos.writeShort(theArray[i]);
            }

            return dos.bytes();
        } catch (final IOException e) {
            throw new UndeclaredThrowableException(e);
        } finally {
            try {
                dos.close();
            } catch (final IOException e) { // NOPMD
            }
        }
    }

    /** Marshal array into Bytes.
     * 
     * @param theArray
     *        the array to be marshaled
     * @return the resultant bytes */
    public static Bytes marshal(final String[] theArray) {
        final int count = theArray.length;
        final BytesOutputStream dos = new BytesOutputStream();
        dos.writeSize(count);
        try {
            for (int i = 0; i < count; i++) {
                dos.writeUTF(theArray[i]);
            }

            return dos.bytes();
        } catch (final IOException e) {
            throw new UndeclaredThrowableException(e);
        } finally {
            try {
                dos.close();
            } catch (final IOException e) { // NOPMD
            }
        }
    }

    /** Packs multiple Bytes into a single Bytes. Upon unpacking, the individual
     * Bytes are returned.
     * 
     * @param theBytes
     *        the bytes to be packed
     * @return the resultant bytes */
    public static Bytes pack(final Bytes... theBytes) {
        final int count = theBytes.length;
        int extra = Util.bytesForSize(count);
        final int[] lenArray = new int[count];
        int i = 0;
        for (final Bytes b : theBytes) {
            final long length = b.length();
            if (length > Integer.MAX_VALUE) {
                throw new IllegalArgumentException(
                        "Cannot handle individual Bytes bigger than Integer.MAX_VALUE");
            }
            lenArray[i++] = (int) length;
            extra += Util.bytesForSize((int) length);
        }
        final byte[] headerBytes = new byte[extra];
        int offset = Util.writeSize(count, headerBytes, 0);
        for (i = 0; i < lenArray.length; i++) {
            offset += Util.writeSize(lenArray[i], headerBytes, offset);
        }
        return new Bytes(headerBytes, concat(theBytes));
    }

    /** Unmarshal into big integer array.
     * 
     * @param theInStrm
     *        the inputstream
     * @return the BigInteger array */
    public static BigInteger[] unmarshalBigIntegerArray(
            final BytesInputStream theInStrm) {
        try {
            final int size = theInStrm.readSize();
            final BigInteger[] resultArray = new BigInteger[size];
            for (int i = 0; i < size; i++) {
                final byte[] tmpArray = new byte[theInStrm.readSize()];
                theInStrm.readFully(tmpArray);
                resultArray[i] = new BigInteger(tmpArray);
            }
            return resultArray;
        } catch (final IOException e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    /** Unmarshal into boolean array.
     * 
     * @param theInStrm
     *        input stream
     * @return the resultant boolean array */
    public static boolean[] unmarshalBooleanArray(
            final BytesInputStream theInStrm) {
        // TODO : pack 8 boolean per byte!
        try {
            final int size = theInStrm.readSize();
            final boolean[] resultArray = new boolean[size];
            for (int i = 0; i < size; i++) {
                resultArray[i] = theInStrm.readBoolean();
            }
            return resultArray;
        } catch (final IOException e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    /** Unmarshal into date array.
     * 
     * @param theInStrm
     *        the input stream
     * @return the resultant date array */
    public static Date[] unmarshalDateArray(final BytesInputStream theInStrm) {
        try {
            final int size = theInStrm.readSize();
            final Date[] resultArray = new Date[size];
            for (int i = 0; i < size; i++) {
                resultArray[i] = new Date(theInStrm.readLong());
            }
            return resultArray;
        } catch (final IOException e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    /** Unmarshal into double array.
     * 
     * @param theInStrm
     *        the inputstream
     * @return the resultant double array */
    public static double[] unmarshalDoubleArray(final BytesInputStream theInStrm) {
        try {
            final int size = theInStrm.readSize();
            final double[] resultArray = new double[size];
            for (int i = 0; i < size; i++) {
                resultArray[i] = theInStrm.readDouble();
            }
            return resultArray;
        } catch (final IOException e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    /** Unmarshal into float array.
     * 
     * @param theInStrm
     *        the inputstream
     * @return the resultant float array */
    public static float[] unmarshalFloatArray(final BytesInputStream theInStrm) {
        try {
            final int size = theInStrm.readSize();
            final float[] resultArray = new float[size];
            for (int i = 0; i < size; i++) {
                resultArray[i] = theInStrm.readFloat();
            }
            return resultArray;
        } catch (final IOException e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    /** Unmarshal into int array.
     * 
     * @param theInStrm
     *        the inputstream
     * @return the resultant int array */
    public static int[] unmarshalIntArray(final BytesInputStream theInStrm) {
        try {
            final int size = theInStrm.readSize();
            final int[] resultArray = new int[size];
            for (int i = 0; i < size; i++) {
                resultArray[i] = theInStrm.readInt();
            }
            return resultArray;
        } catch (final IOException e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    /** Unmarshal into long array.
     * 
     * @param theInStrm
     *        the inputStream
     * @return the resultant long array */
    public static long[] unmarshalLongArray(final BytesInputStream theInStrm) {
        try {
            final int size = theInStrm.readSize();
            final long[] resultArray = new long[size];
            for (int i = 0; i < size; i++) {
                resultArray[i] = theInStrm.readLong();
            }
            return resultArray;
        } catch (final IOException e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    /** Unmarshal into short array.
     * 
     * @param theInStrm
     *        the inputStream
     * @return the resultant short array */
    public static short[] unmarshalShortArray(final BytesInputStream theInStrm) {
        try {
            final int size = theInStrm.readSize();
            final short[] resultArray = new short[size];
            for (int i = 0; i < size; i++) {
                resultArray[i] = theInStrm.readShort();
            }
            return resultArray;
        } catch (final IOException e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    /** Unmarshal into string array.
     * 
     * @param theInStrm
     *        the inputStream
     * @return the resultant string array */
    public static String[] unmarshalStringArray(final BytesInputStream theInStrm) {
        try {
            final int size = theInStrm.readSize();
            final String[] resultArray = new String[size];
            for (int i = 0; i < size; i++) {
                resultArray[i] = theInStrm.readUTF();
            }
            return resultArray;
        } catch (final IOException e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    /** Unpacks multiple Bytes from a single Bytes. The content of bytes *must*
     * be the result of a call to pack().
     * 
     * @param theBytes
     *        the bytes to be unpacked
     * @return the resultant Bytes array */
    public static Bytes[] unpack(final Bytes theBytes) {
        int offset = 0;
        final int count = BytesUtil.readSize(theBytes, offset);
        offset += Util.bytesForSize(count);
        final int[] lengths = new int[count];
        for (int i = 0; i < count; i++) {
            final int len = BytesUtil.readSize(theBytes, offset);
            lengths[i] = len;
            offset += Util.bytesForSize(len);
        }
        final Bytes[] resultArray = new Bytes[count];
        long rstart = offset;
        for (int i = 0; i < count; i++) {
            resultArray[i] = theBytes.slice(rstart, lengths[i]);
            rstart = rstart + lengths[i];
        }
        return resultArray;
    }

    /** Real Constructor. Assumes the input was validated. */
    private Bytes(final Bytes theNext, final byte[] theData,
            final long thePrimitive, final int theStart, final int theLength,
            final boolean theCopyOnWrite) {

        if (theData != null)
            this.data = Arrays.copyOf(theData, theData.length);
        else
            this.data = null;

        this.primitive = thePrimitive;
        this.start = theStart;
        this.length = theLength;
        this.copyOnWrite = theCopyOnWrite;
        long nextLen = 0;
        if (theNext != null) {
            nextLen += theNext.length();
        }
        this.next = (nextLen == 0) ? null : theNext;
    }

    /** Construct a Bytes from a boolean.
     * 
     * @param theData
     *        the data */
    public Bytes(final boolean theData) {
        // No check!
        this(null, null, theData ? 1L : 0L, 0, 1, false);
    }

    /** Construct a Bytes from a boolean.
     * 
     * @param theData
     *        the data
     * @param theNext
     *        the next bytes */
    public Bytes(final boolean theData, final Bytes theNext) {
        // No check!
        this(theNext, null, theData ? 1L : 0L, 0, 1, false);
    }

    /** Construct a Bytes from a byte.
     * 
     * @param theData
     *        the data */
    public Bytes(final byte theData) {
        // No check!
        this(null, null, theData, 0, 1, false);
    }

    /** Construct a Bytes from a byte.
     * 
     * @param theData
     *        the data
     * @param theNext
     *        the next bytes */
    public Bytes(final byte theData, final Bytes theNext) {
        // No check!
        this(theNext, null, theData, 0, 1, false);
    }

    /** Construct a Bytes from a byte[]. Assumes copyOnWrite == false.
     * 
     * @param theData
     *        the data */
    public Bytes(final byte[] theData) {
        // No check!
        this(null, theData, 0L, 0, theData.length, false);
    }

    /** Construct a Bytes from a byte[].
     * 
     * @param theData
     *        the data
     * @param theCopyOnWrite
     *        copy on write */
    public Bytes(final byte[] theData, final boolean theCopyOnWrite) {
        // No check!
        this(null, theData, 0L, 0, theData.length, theCopyOnWrite);
    }

    /** Construct a Bytes from a byte[]. Assumes copyOnWrite == false.
     * 
     * @param theData
     *        the data
     * @param theNext
     *        the next */
    public Bytes(final byte[] theData, final Bytes theNext) {
        // No check!
        this(theNext, theData, 0L, 0, theData.length, false);
    }

    /** Construct a Bytes from a byte[]. Assumes copyOnWrite == false.
     * 
     * @param theData
     *        the data
     * @param theStart
     *        the start
     * @param theLength
     *        the length */
    public Bytes(final byte[] theData, final int theStart, final int theLength) {
        this(null, check(theData, theStart, theLength), 0L, theStart,
                theLength, false);
    }

    /** Construct a Bytes from a byte[].
     * 
     * @param theData
     *        the data
     * @param theStart
     *        the start
     * @param theLength
     *        the length
     * @param theCopyOnWrite
     *        the copy on write
     * @param theNext
     *        the next */
    public Bytes(final byte[] theData, final int theStart, final int theLength,
            final boolean theCopyOnWrite, final Bytes theNext) {
        this(theNext, check(theData, theStart, theLength), 0L, theStart,
                theLength, theCopyOnWrite);
    }

    /** Construct a Bytes from a byte[]. Assumes copyOnWrite == false.
     * 
     * @param theData
     *        the data
     * @param theStart
     *        the start
     * @param theLength
     *        the length
     * @param theNext
     *        the next */
    public Bytes(final byte[] theData, final int theStart, final int theLength,
            final Bytes theNext) {
        this(theNext, check(theData, theStart, theLength), 0L, theStart,
                theLength, false);
    }

    /** Construct a Bytes from a char.
     * 
     * @param theData
     *        the data */
    public Bytes(final char theData) {
        // No check!
        this(null, null, theData, 0, CHAR_BYTES, false);
    }

    /** Construct a Bytes from a char.
     * 
     * @param theData
     *        the data
     * @param theNext
     *        the next */
    public Bytes(final char theData, final Bytes theNext) {
        // No check!
        this(theNext, null, theData, 0, CHAR_BYTES, false);
    }

    /** Construct a Bytes from a double.
     * 
     * @param theData
     *        the data */
    public Bytes(final double theData) {
        // No check!
        this(null, null, Double.doubleToRawLongBits(theData), 0, DOUBLE_BYTES,
                false);
    }

    /** Construct a Bytes from a double.
     * 
     * @param theData
     *        the data
     * @param theNext
     *        the next */
    public Bytes(final double theData, final Bytes theNext) {
        // No check!
        this(theNext, null, Double.doubleToRawLongBits(theData), 0,
                DOUBLE_BYTES, false);
    }

    /** Construct a Bytes from a float.
     * 
     * @param theData
     *        the data */
    public Bytes(final float theData) {
        // No check!
        this(null, null, Float.floatToRawIntBits(theData), 0, FLOAT_BYTES,
                false);
    }

    /** Construct a Bytes from a float.
     * 
     * @param theData
     *        the data
     * @param theNext
     *        the next */
    public Bytes(final float theData, final Bytes theNext) {
        // No check!
        this(theNext, null, Float.floatToRawIntBits(theData), 0, FLOAT_BYTES,
                false);
    }

    /** Construct a Bytes from a int.
     * 
     * @param theData
     *        the data */
    public Bytes(final int theData) {
        // No check!
        this(null, null, theData, 0, INT_BYTES, false);
    }

    /** Construct a Bytes from a int.
     * 
     * @param theData
     *        the data
     * @param theNext
     *        the next */
    public Bytes(final int theData, final Bytes theNext) {
        // No check!
        this(theNext, null, theData, 0, INT_BYTES, false);
    }

    /** Construct a Bytes from a long.
     * 
     * @param theData
     *        the data */
    public Bytes(final long theData) {
        // No check!
        this(null, null, theData, 0, LONG_BYTES, false);
    }

    /** Construct a Bytes from a long.
     * 
     * @param theData
     *        the data
     * @param theNext
     *        the next */
    public Bytes(final long theData, final Bytes theNext) {
        // No check!
        this(theNext, null, theData, 0, LONG_BYTES, false);
    }

    /** Construct a Bytes from a short.
     * 
     * @param theData
     *        the data */
    public Bytes(final short theData) {
        // No check!
        this(null, null, theData, 0, SHORT_BYTES, false);
    }

    /** Construct a Bytes from a short.
     * 
     * @param theData
     *        the data
     * @param theNext
     *        the next */
    public Bytes(final short theData, final Bytes theNext) {
        // No check!
        this(theNext, null, theData, 0, SHORT_BYTES, false);
    }

    /** Returns one byte, without validation. */
    private byte get2(long theIndex) {
        Bytes bytes = this;
        while (true) { // $codepro.audit.disable constantConditionalExpression
            final int tempLength = bytes.length;
            if (theIndex < tempLength) {
                return bytes.get3((int) theIndex);
            }
            bytes = bytes.next;
            theIndex -= tempLength;
        }
    }

    /** Returns one byte, that we contain ourselves directly. */
    private byte get3(final int theIndex) {
        final int where = start + theIndex;
        if (primitive()) {
            return (byte) (primitive >> (BYTE_BITS * where));
        }
        return data[where];
    }

    /** Compares the content to array.
     * 
     * @param theArray
     *        the array
     * @return true, if contents are equal */
    public boolean contentEquals(final byte[] theArray) {
        return equals(new Bytes(theArray));
    }

    /** Returns true if this Bytes has no "next".
     * 
     * @return true, true if this Bytes has no "next" */
    public boolean contiguous() {
        return (next == null);
    }

    /** Returns a safe copy where copyOnWrite is false.
     * 
     * @return the bytes */
    public Bytes copy() {
        final Bytes newNext = (next == null) ? null : next.copy();
        if (copyOnWrite) {
            return new Bytes(toArray(true), newNext);
        }
        if (newNext == next) {
            return this;
        }
        return new Bytes(newNext, data, primitive, start, length, false);
    }

    /** Reads bytes into an array.
     * 
     * @param theArray
     *        the array */
    public void copyTo(final byte[] theArray) {
        copyTo(theArray, 0, theArray.length, 0L);
    }

    /** Reads bytes into an array. offset is the starting position in array. len
     * is the number of bytes to copy. from is the starting position in Bytes.
     * 
     * @param theArray
     *        the array
     * @param theOffset
     *        the offset
     * @param theLength
     *        the length
     * @param theFrom
     *        the from */
    public void copyTo(final byte[] theArray, final int theOffset,
            final int theLength, final long theFrom) {
        final long total = length();
        if ((theFrom < 0) || (theFrom >= total)) {
            throw new IllegalArgumentException("from: " + theFrom);
        }
        if (theLength < 0) {
            throw new IllegalArgumentException("len: " + theLength);
        }
        if (theOffset < 0) {
            throw new IllegalArgumentException("offset: " + theOffset);
        }
        final long left = total - theFrom;
        if (left < theLength) {
            throw new IllegalArgumentException("available: " + left + "  len: "
                    + theLength);
        }
        if (theArray == null) {
            throw new NullPointerException("buf");
        }
        if (theArray.length - theOffset < theLength) {
            throw new IllegalArgumentException("buf available: "
                    + (theArray.length - theOffset) + "  len: " + theLength);
        }
        copyTo(this, theArray, theOffset, theLength, theFrom);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object theObject) {
        if (this == theObject)
            return true;
        if (theObject == null)
            return false;
        if (!(theObject instanceof Bytes))
            return false;
        final Bytes other = (Bytes) theObject;
        final long total = length();
        if (total != other.length())
            return false;
        for (long i = 0; i < total; i++) {
            if (get2(i) != other.get2(i)) {
                return false;
            }
        }
        return true;
    }

    /** Returns one byte.
     * 
     * @param theIndex
     *        the index
     * @return the byte */
    public byte get(final long theIndex) {
        if ((theIndex < 0) || (theIndex >= length())) {
            throw new IllegalArgumentException("index: " + theIndex);
        }
        return get2(theIndex);
    }

    /** Un-marshals a boolean.
     * 
     * @return the Un-marshaled boolean */
    public boolean getBoolean() {
        return getBoolean(0);
    }

    /** Un-marshals a boolean at position offset.
     * 
     * @param theOffset
     *        the offset
     * @return the Un-marshaled boolean */
    public boolean getBoolean(final long theOffset) {
        return get(theOffset) != 0;
    }

    /** Returns the first byte.
     * 
     * @return the first byte */
    public byte getByte() {
        return get(0);
    }

    /** Returns one byte.
     * 
     * @param theOffset
     *        the offset
     * @return the byte */
    public byte getByte(final long theOffset) {
        return get(theOffset);
    }

    /** Un-marshals a char. Un-marshaling uses little-endian order, as in the
     * Util.marshal() method.
     * 
     * @return the Un-marshaled char */
    public char getChar() {
        return (char) getShort(0);
    }

    /** Un-marshals a char at position offset. Un-marshaling uses little-endian
     * order, as in the Util.marshal() method.
     * 
     * @param theOffset
     *        the offset
     * @return the Un-marshaled char */
    public char getChar(final long theOffset) {
        return (char) getShort(theOffset);
    }

    /** Un-marshals an double. Un-marshaling uses little-endian order, as in the
     * Util.marshal() method.
     * 
     * @return the Un-marshaled double */
    public double getDouble() {
        return getDouble(0);
    }

    /** Un-marshals an double at position offset. Un-marshaling uses
     * little-endian order, as in the Util.marshal() method.
     * 
     * @param theOffset
     *        the offset
     * @return the Un-marshaled double */
    public double getDouble(final long theOffset) {
        return Double.longBitsToDouble(getLong(theOffset));
    }

    /** Un-marshals an float. Un-marshaling uses little-endian order, as in the
     * Util.marshal() method.
     * 
     * @return the Un-marshaled float */
    public float getFloat() {
        return getFloat(0);
    }

    /** Un-marshals an float at position offset. Un-marshaling uses little-endian
     * order, as in the Util.marshal() method.
     * 
     * @param theOffset
     *        the offset
     * @return the Un-marshaled float */
    public float getFloat(final long theOffset) {
        return Float.intBitsToFloat(getInt(theOffset));
    }

    /** Un-marshals an int. Un-marshaling uses little-endian order, as in the
     * Util.marshal() method.
     * 
     * @return the Un-marshaled int */
    public int getInt() {
        return getInt(0);
    }

    /** Un-marshals an int at position offset. Un-marshaling uses little-endian
     * order, as in the Util.marshal() method.
     * 
     * @param theOffset
     *        the offset
     * @return the Un-marshaled int */
    public int getInt(final long theOffset) {
        if ((theOffset < 0) || (theOffset + 3 >= length())) {
            throw new IllegalArgumentException("index: " + theOffset);
        }
        if (primitive() && (theOffset == 0)) {
            return (int) primitiveValue();
        }
        final int ch3 = (get2(theOffset + 3) & BYTE_MASK);
        final int ch2 = (get2(theOffset + 2) & BYTE_MASK);
        final int ch1 = (get2(theOffset + 1) & BYTE_MASK);
        final int ch0 = (get2(theOffset + 0) & BYTE_MASK);
        return ((ch3 << B_THREE_BYTES) + (ch2 << B_TWO_BYTES)
                + (ch1 << B_ONE_BYTE) + (ch0 << 0));
    }

    /** Un-marshals an long. Un-marshaling uses little-endian order, as in the
     * Util.marshal() method.
     * 
     * @return the Un-marshaled long */
    public long getLong() {
        return getLong(0);
    }

    /** Un-marshals an long at position offset. Un-marshaling uses little-endian
     * order, as in the Util.marshal() method.
     * 
     * @param theOffset
     *        the offset
     * @return the Un-marshaled long */
    public long getLong(final long theOffset) {
        if ((theOffset < 0) || (theOffset + LONG_INDEX >= length())) {
            throw new IllegalArgumentException("index: " + theOffset);
        }
        if (primitive() && (theOffset == 0)) {
            return primitiveValue();
        }
        return (((long) get2(theOffset + INDX7) << B_SEVEN_BYTES)
                + ((long) (get2(theOffset + INDX6) & BYTE_MASK) << B_SIX_BYTES)
                + ((long) (get2(theOffset + INDX5) & BYTE_MASK) << B_FIVE_BYTES)
                + ((long) (get2(theOffset + INDX4) & BYTE_MASK) << B_FOUR_BYTES)
                + ((long) (get2(theOffset + INDX3) & BYTE_MASK) << B_THREE_BYTES)
                + ((get2(theOffset + INDX2) & BYTE_MASK) << B_TWO_BYTES)
                + ((get2(theOffset + INDX1) & BYTE_MASK) << B_ONE_BYTE) + ((get2(theOffset + 0) & BYTE_MASK) << 0));
    }

    /** Un-marshals a short. Un-marshaling uses little-endian order, as in the
     * Util.marshal() method.
     * 
     * @return the Un-marshaled short */
    public short getShort() {
        return getShort(0);
    }

    /** Un-marshals a short at position offset. Un-marshaling uses little-endian
     * order, as in the Util.marshal() method.
     * 
     * @param theOffset
     *        the offset
     * @return the Un-marshaled short */
    public short getShort(final long theOffset) {
        if ((theOffset < 0) || (theOffset + 1 >= length())) {
            throw new IllegalArgumentException("index: " + theOffset);
        }
        if (primitive() && (theOffset == 0)) {
            return (short) primitiveValue();
        }
        final int ch1 = (get2(theOffset + 1) & BYTE_MASK);
        final int ch0 = (get2(theOffset) & BYTE_MASK);
        return (short) ((ch1 << BYTE_BITS) + (ch0 << 0));
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        if (hashCode == 0) {
            // We can *only* compare the bytes, because different combinations
            // which each have the same content would differ if we compared the
            // individual lengths.
            final int prime = 31;
            int result = 1;
            final int end = start + length;
            for (int i = start; i < end; i++) {
                result = 31 * result + get3(i);
            }
            result = prime * result + ((next == null) ? 0 : next.hashCode());
            hashCode = (result == 0) ? 1 : result;
        }
        return hashCode;
    }

    /** Returns the length of the array.
     * 
     * @return the length of the array */
    public long length() {
        final Bytes b = next;
        return length + ((b == null) ? 0L : b.length());
    }

    /** Returns true if this Bytes contains a packed primitive, instead of a
     * byte[].
     * 
     * @return contains a packed primitive */
    public boolean primitive() {
        return (data == null);
    }

    /** Returns the "primitive value". Only useful if primitive() returns true.
     * 
     * @return the "primitive value" */
    public long primitiveValue() {
        final long mask = -1L >>> (LONG_BITS - (BYTE_BITS * length));
        return (primitive >> (BYTE_BITS * start)) & mask;
    }

    /** Returns a slice of the bytes, without making a copy.
     * 
     * @param theRangeStart
     *        the range start
     * @param theRangeLength
     *        the range length
     * @return the resultant bytes */
    public Bytes slice(final long theRangeStart, final long theRangeLength) {
        final long total = length();
        final long available = total - theRangeStart;
        if (theRangeLength > available) {
            throw new IllegalArgumentException("range.size(): "
                    + theRangeLength + " available: " + available);
        }
        final int tempLength = this.length;
        if (theRangeStart == 0) {
            // trimming at the end
            if (theRangeLength == total) {
                return this;
            }
            if (theRangeLength <= tempLength) {
                return new Bytes(null, data, primitive, start,
                        (int) theRangeLength, copyOnWrite);
            }
        } else {
            // trimming at the beginning
            final long myavailable = tempLength - theRangeStart;
            if (theRangeLength <= myavailable) {
                return new Bytes(null, data, primitive, start
                        + (int) theRangeStart, (int) theRangeLength,
                        copyOnWrite);
            }
        }
        // Must completely re-map ...
        final List<Bytes> stack = new ArrayList<Bytes>();
        Bytes bytes = this;
        long missing = theRangeLength;
        while (missing > 0) {
            stack.add(bytes);
            missing -= bytes.length;
            bytes = bytes.next;
        }
        bytes = null;
        while (missing != theRangeLength) {
            final Bytes top = stack.remove(stack.size() - 1);
            final int toplen = top.length;
            final int topstart = top.start;
            int end = topstart + toplen;
            if (missing < 0) {
                end += missing;
                missing = 0;
            }
            int strt = topstart;
            final int avail = end - topstart;
            if (missing + avail > theRangeLength) {
                strt = end - (int) (theRangeLength - missing);
            }
            final int len = end - strt;
            missing += len;
            bytes = new Bytes(bytes, top.data, top.primitive, strt, len,
                    top.copyOnWrite);
        }
        return bytes;
    }

    /** Returns the data as a byte[]. If modifiable is false, the caller
     * guarantees it will not modify the content of the array. This can allow
     * optimizations in some cases.
     * 
     * @param isModifiable
     *        if modifiable
     * @return the resultant byte[] */
    public byte[] toArray(final boolean isModifiable) {
        final byte[] tempData = this.data;
        if ((!isModifiable || !copyOnWrite) && (next == null) && (start == 0)
                && (tempData != null) && (length == tempData.length)) {
            return tempData;
        }
        final long total = length();
        if (total > Integer.MAX_VALUE) {
            throw new UnsupportedOperationException("lenght: " + total
                    + " cannot be allocated in a single array!");
        }
        final byte[] resultArray = new byte[(int) total];
        copyTo(resultArray);
        return resultArray;
    }

    /** Returns a new DataInputStream.
     * 
     * @return the bytes input stream */
    public BytesInputStream toInputStream() {
        return new BytesInputStream(this);
    }

}
