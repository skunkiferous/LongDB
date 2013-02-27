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
// $codepro.audit.disable unnecessaryCast
package com.blockwithme.longdb.util;

import static com.blockwithme.longdb.common.constants.ByteConstants.BYTE_MASK;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_FIVE_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_FOUR_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_ONE_BYTE;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_SEVEN_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_SIX_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_THREE_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.B_TWO_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.INT_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.SHORT_BYTES;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;

/** General utility static functions for the DB API.
 * 
 * @author monster TODO: Is this tested? */
public final class Util {

    /**  */
    private static final Class<?>[] ZERO_LENGTH_CLASS_ARRAY = new Class[0];

    /** Index position 1 */
    private static final int INDX1 = 1;

    /** Index position 2 */
    private static final int INDX2 = 2;

    /** Index position 3 */
    private static final int INDX3 = 3;

    /** Index position 4 */
    private static final int INDX4 = 4;

    /** Index position 5 */
    private static final int INDX5 = 5;

    /** Index position 6 */
    private static final int INDX6 = 6;

    /** Index position 7 */
    private static final int INDX7 = 7;

    /** Load backend class. */
    @SuppressWarnings("unchecked")
    private static <E> Class<? extends E> loadBEClass(
            final String thePropertyName, final Class<E> theMustImplement,
            final String theImplName) {
        final Class<? extends E> cls;
        try {
            cls = (Class<? extends E>) Class.forName(theImplName);
        } catch (final ClassNotFoundException e) {
            throw new IllegalStateException("Implementation " + theImplName
                    + " could not be found for " + thePropertyName + "!", e);
        }
        if (!theMustImplement.isAssignableFrom(cls)) {
            throw new IllegalStateException(theImplName + " for "
                    + thePropertyName + " does not implement "
                    + theMustImplement.getName() + "!");
        }
        if (Modifier.isAbstract(cls.getModifiers())) {
            throw new IllegalStateException(theImplName + " for "
                    + thePropertyName + " is abstract!");
        }
        final Constructor<? extends E> ctr;
        try {
            ctr = cls.getConstructor(ZERO_LENGTH_CLASS_ARRAY);
        } catch (final SecurityException e) {
            throw new IllegalStateException(theImplName + " for "
                    + thePropertyName + " cannot be accessed!", e);
        } catch (final NoSuchMethodException e) {
            throw new IllegalStateException(
                    theImplName + " for " + thePropertyName
                            + " does not have a default constructor!", e);
        }
        if (ctr == null) {
            throw new IllegalStateException(theImplName + " for "
                    + thePropertyName + " does not have a default constructor!");
        }
        if (!Modifier.isPublic(ctr.getModifiers())) {
            throw new IllegalStateException(theImplName + " for "
                    + thePropertyName + " default constructor is not public!");
        }
        return cls;
    }

    /** Returns the number of bytes required to store a variable length
     * non-negative int.
     * 
     * @param theSize
     * @return */
    public static int bytesForSize(final int theSize) {
        if (theSize < 0) {
            throw new IllegalArgumentException("size: " + theSize);
        }
        return (theSize <= Short.MAX_VALUE) ? SHORT_BYTES : INT_BYTES;
    }

    /** Looks up the name of a class based on a property, and creates an instance
     * of that class.
     * 
     * @param <E>
     * @param thePropertyName
     * @param theMustImplement
     * @return */
    public static <E> E createImplementation(final String thePropertyName,
            final Class<E> theMustImplement) {
        final Class<? extends E> cls = getImplementation(thePropertyName,
                theMustImplement);
        try {
            return cls.newInstance();
        } catch (final InstantiationException e) {
            throw new IllegalStateException(cls + " for " + thePropertyName
                    + " cannot be created!", e);
        } catch (final IllegalAccessException e) {
            throw new IllegalStateException(cls + " for " + thePropertyName
                    + " cannot be created!", e);
        }
    }

    /** Looks up the class based the fully qualified name passed to this method,
     * checks that it implements some existing type, and can be created without
     * parameters, and returns it.
     * 
     * @param <E>
     * @param theImplName
     * @param theMustImplement
     * @return */
    public static <E> Class<? extends E> getImplByName(
            final String theImplName, final Class<E> theMustImplement) {

        final Class<? extends E> cls = loadBEClass(theImplName,
                theMustImplement, theImplName);
        return cls;
    }

    /** Looks up the name of a class based on a property, checks that it
     * implements some existing type, and can be created without parameters, and
     * returns it.
     * 
     * @param <E>
     *        the element type
     * @param thePropertyName
     *        the property name
     * @param theMustImplement
     *        the must implement
     * @return the implementation */

    public static <E> Class<? extends E> getImplementation(
            final String thePropertyName, final Class<E> theMustImplement) {
        final String implName = System.getProperty(thePropertyName, "").trim();
        if (implName.isEmpty()) {
            throw new IllegalStateException("No implementation defined for "
                    + thePropertyName + " !");
        }
        final Class<? extends E> cls = loadBEClass(thePropertyName,
                theMustImplement, implName);
        return cls;
    }

    /** Marshals an boolean in a byte[] at position offset. The array must be big
     * enough.
     * 
     * @param theValue
     * @param theArray
     * @param theOffset */
    public static void marshal(final boolean theValue, final byte[] theArray,
            final int theOffset) {
        theArray[theOffset] = theValue ? (byte) 1 : (byte) 0;
    }

    /** Marshals an short in a byte[] at position offset. The array must be big
     * enough. Marshaling uses little-endian order, to make the resulting data
     * sortable.
     * 
     * @param theValue
     * @param theArray
     * @param theOffset */
    public static void marshal(final char theValue, final byte[] theArray,
            final int theOffset) {
        theArray[theOffset + 1] = (byte) (theValue >>> B_ONE_BYTE);
        theArray[theOffset + 0] = (byte) (theValue >>> 0);
    }

    /** Marshals a double in a byte[] at position offset. The array must be big
     * enough. Marshaling uses little-endian order, to make the resulting data
     * sortable.
     * 
     * @param theValue
     * @param theArray
     * @param theOffset */
    public static void marshal(final double theValue, final byte[] theArray,
            final int theOffset) {
        marshal(Double.doubleToRawLongBits(theValue), theArray, theOffset);
    }

    /** Marshals an float in a byte[] at position offset. The array must be big
     * enough. Marshaling uses little-endian order, to make the resulting data
     * sortable.
     * 
     * @param theValue
     * @param theArray
     * @param theOffset */
    public static void marshal(final float theValue, final byte[] theArray,
            final int theOffset) {
        marshal(Float.floatToRawIntBits(theValue), theArray, theOffset);
    }

    /** Marshals an int in a byte[] at position offset. The array must be big
     * enough. Marshaling uses little-endian order, to make the resulting data
     * sortable.
     * 
     * @param theValue
     * @param theArray
     * @param theOffset */
    public static void marshal(final int theValue, final byte[] theArray,
            final int theOffset) {
        theArray[theOffset + 3] = (byte) (theValue >>> B_THREE_BYTES);
        theArray[theOffset + 2] = (byte) (theValue >>> B_TWO_BYTES);
        theArray[theOffset + 1] = (byte) (theValue >>> B_ONE_BYTE);
        theArray[theOffset + 0] = (byte) (theValue >>> 0);
    }

    /** Marshals a long in a byte[] at position offset. The array must be big
     * enough. Marshaling uses little-endian order, to make the resulting data
     * sortable.
     * 
     * @param theValue
     * @param theArray
     * @param theOffset */
    public static void marshal(final long theValue, final byte[] theArray,
            final int theOffset) {
        theArray[theOffset + INDX7] = (byte) (theValue >>> B_SEVEN_BYTES);
        theArray[theOffset + INDX6] = (byte) (theValue >>> B_SIX_BYTES);
        theArray[theOffset + INDX5] = (byte) (theValue >>> B_FIVE_BYTES);
        theArray[theOffset + INDX4] = (byte) (theValue >>> B_FOUR_BYTES);
        theArray[theOffset + INDX3] = (byte) (theValue >>> B_THREE_BYTES);
        theArray[theOffset + INDX2] = (byte) (theValue >>> B_TWO_BYTES);
        theArray[theOffset + INDX1] = (byte) (theValue >>> B_ONE_BYTE);
        theArray[theOffset + 0] = (byte) (theValue >>> 0);
    }

    /** Marshals an short in a byte[] at position offset. The array must be big
     * enough. Marshaling uses little-endian order, to make the resulting data
     * sortable.
     * 
     * @param theValue
     * @param theArray
     * @param theOffset */
    public static void marshal(final short theValue, final byte[] theArray,
            final int theOffset) {
        theArray[theOffset + 1] = (byte) (theValue >>> B_ONE_BYTE);
        theArray[theOffset + 0] = (byte) (theValue >>> 0);
    }

    /** Reads a variable length non-negative int from array at the given offset.
     * The number of bytes read is determined with bytesForSize(int).
     * 
     * @param theArray
     * @param theOffset
     * @return */
    // TODO : remove the following comment and fix magic numbers
    // CHECKSTYLE stop magic number check
    public static int readSize(final byte[] theArray, final int theOffset) {
        final int ch1 = (theArray[theOffset] & BYTE_MASK);
        final int ch2 = (theArray[theOffset + 1] & BYTE_MASK);
        if (ch2 > 127) {
            final int ch3 = (theArray[theOffset + 2] & BYTE_MASK);
            final int ch4 = (theArray[theOffset + 3] & BYTE_MASK);
            return ((ch4 << 23) + (ch3 << 15) + ((ch2 << 8) & 0x7F00) + (ch1 << 0));
        }
        return (ch2 << 8) + (ch1 << 0);
    }

    // CHECKSTYLE resume magic number check
    /** Un-marshals a boolean from a byte[] at position offset.
     * 
     * @param theValueBytes
     * @param theOffset
     * @return */
    public static boolean unmarshalBoolean(final byte[] theValueBytes,
            final int theOffset) {
        return theValueBytes[theOffset] != 0;
    }

    /** Un-marshals a char from a byte[] at position offset. Un-marshaling uses
     * little-endian order, as in the marshal() method.
     * 
     * @param theValueBytes
     * @param theOffset
     * @return */
    // CHECKSTYLE stop magic number check
    public static char unmarshalChar(final byte[] theValueBytes,
            final int theOffset) {
        final int ch1 = (theValueBytes[theOffset + 1] & BYTE_MASK);
        final int ch0 = (theValueBytes[theOffset + 0] & BYTE_MASK);
        return (char) ((ch1 << 8) + (ch0 << 0));
    }

    // CHECKSTYLE resume magic number check
    /** Un-marshals an double from a byte[] at position offset. Un-marshaling
     * uses little-endian order, as in the marshal() method.
     * 
     * @param theValueBytes
     * @param theOffset
     * @return */
    public static double unmarshalDouble(final byte[] theValueBytes,
            final int theOffset) {
        return Double.longBitsToDouble(unmarshalLong(theValueBytes, theOffset));
    }

    /** Un-marshals an float from a byte[] at position offset. Un-marshaling uses
     * little-endian order, as in the marshal() method.
     * 
     * @param theValueBytes
     * @param theOffset
     * @return */
    public static float unmarshalFloat(final byte[] theValueBytes,
            final int theOffset) {
        return Float.intBitsToFloat(unmarshalInt(theValueBytes, theOffset));
    }

    /** Un-marshals an int from a byte[] at position offset. Un-marshaling uses
     * little-endian order, as in the marshal() method.
     * 
     * @param theValueBytes
     * @param theOffset
     * @return */
    public static int unmarshalInt(final byte[] theValueBytes,
            final int theOffset) {
        final int ch3 = (theValueBytes[theOffset + 3] & BYTE_MASK);
        final int ch2 = (theValueBytes[theOffset + 2] & BYTE_MASK);
        final int ch1 = (theValueBytes[theOffset + 1] & BYTE_MASK);
        final int ch0 = (theValueBytes[theOffset + 0] & BYTE_MASK);
        return ((ch3 << B_THREE_BYTES) + (ch2 << B_TWO_BYTES)
                + (ch1 << B_ONE_BYTE) + (ch0 << 0));
    }

    /** Un-marshals an long from a byte[] at position offset. Un-marshaling uses
     * little-endian order, as in the marshal() method.
     * 
     * @param theValueBytes
     * @param theOffset
     * @return */
    public static long unmarshalLong(final byte[] theValueBytes,
            final int theOffset) {
        return (((long) theValueBytes[theOffset + INDX7] << B_SEVEN_BYTES)
                + ((long) (theValueBytes[theOffset + INDX6] & BYTE_MASK) << B_SIX_BYTES)
                + ((long) (theValueBytes[theOffset + INDX5] & BYTE_MASK) << B_FIVE_BYTES)
                + ((long) (theValueBytes[theOffset + INDX4] & BYTE_MASK) << B_FOUR_BYTES)
                + ((long) (theValueBytes[theOffset + INDX3] & BYTE_MASK) << B_THREE_BYTES)
                + ((theValueBytes[theOffset + INDX2] & BYTE_MASK) << B_TWO_BYTES)
                + ((theValueBytes[theOffset + INDX1] & BYTE_MASK) << B_ONE_BYTE) + ((theValueBytes[theOffset + 0] & BYTE_MASK) << 0));
    }

    /** Un-marshals a short from a byte[] at position offset. Un-marshaling uses
     * little-endian order, as in the marshal() method.
     * 
     * @param theValueBytes
     * @param theOffset
     * @return */
    public static short unmarshalShort(final byte[] theValueBytes,
            final int theOffset) {
        final int ch1 = (theValueBytes[theOffset + 1] & BYTE_MASK);
        final int ch0 = (theValueBytes[theOffset + 0] & BYTE_MASK);
        return (short) ((ch1 << B_ONE_BYTE) + (ch0 << 0));
    }

    /** Writes a variable length non-negative int into array at the given offset.
     * The number of bytes read is determined with bytesForSize(int). The array
     * must be big enough to hold the data!
     * 
     * @param theSize
     * @param theArray
     * @param theOffset
     * @return */
    // TODO remove the following comments and fix magic numbers
    // CHECKSTYLE stop magic number check
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "ICAST_QUESTIONABLE_UNSIGNED_RIGHT_SHIFT")
    public static int writeSize(final int theSize, final byte[] theArray,
            final int theOffset) {
        final int count = bytesForSize(theSize);
        theArray[theOffset] = (byte) (theSize >>> 0);
        if (count == 4) {
            theArray[theOffset + 1] = (byte) ((theSize >>> 8) | -128);
            theArray[theOffset + 2] = (byte) (theSize >>> 15);
            theArray[theOffset + 3] = (byte) (theSize >>> 23);
        } else {
            theArray[theOffset + 1] = (byte) (theSize >>> 8);
        }
        return count;
    }

    // CHECKSTYLE resume magic number check
    /** Hide utility class constructor. */
    private Util() {
    }

}
