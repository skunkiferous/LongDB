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
// $codepro.audit.disable handleNumericParsingErrors, debuggingCode, illegalMainMethod
package com.blockwithme.longdb.entities;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Random;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

/** Represents a 64bit positive base-36 value. The value is expressed as
 * *lower-case*. It covers the whole range of a long, treating it as an unsigned
 * long. Leading zeroes are not part of the String by default, and are ignored
 * when converting a String to a Base36.. TODO Create a REAL JUnit test */
@ParametersAreNonnullByDefault
public final class Base36 implements Serializable {

    /** Position of Last but one bit in long */
    private static final int LONG_LAST_BUT_ONE_BIT = 63;

    /** Position of Last bit in long */
    private static final int LONG_LAST_BIT = 64;

    /** The base-36 radix */
    private static final int RADIX = 36;

    /** serialVersionUID */
    private static final long serialVersionUID = 1L;

    /** 12 Zeroes */
    private static final String ZEROS = "0000000000000";

    /** The base-36 radix maximal length. */
    public static final int MAX_LEN = 13;

    /** The textual form. */
    private transient String toString;

    /** The value. */
    private final long value;

    /** Adds leading zeros, so that the string is always MAX_LEN long. */
    private static String fixedSize(final String theString) {
        return ZEROS.substring(theString.length()) + theString;
    }

    /** Just for tests. */
    private static void print(final long theValue) {
        final Base36 b36 = new Base36(theValue);
        System.out.println(theValue + " " + b36 + " " + b36.toFixedString()
                + " " + get(b36.toString()).value);
    }

    /** Returns the Base36 representation of this long.
     * 
     * @param theValue
     * @return Base36 representation */
    public static Base36 get(final long theValue) {
        return new Base36(theValue);
    }

    /** Returns the Base36 representation of this base-36 encoded String
     * (non-case-sensitive).
     * 
     * @param theValue
     * @return Base36 representation */
    public static Base36 get(String theValue) {
        final int len = theValue.length();
        if (len > MAX_LEN) {
            throw new IllegalArgumentException("Maximum length is: " + MAX_LEN
                    + " value: '" + theValue + "'");
        }
        theValue = theValue.toLowerCase();
        long v;
        if ((len < MAX_LEN)
                || ((len == MAX_LEN) && (theValue.charAt(0) == '0'))) {
            v = Long.parseLong(theValue, RADIX);
        } else {
            final BigInteger parsedBi = new BigInteger(theValue, RADIX);

            v = parsedBi.longValue();
            if (parsedBi.testBit(LONG_LAST_BIT))
                v = v | (1L << LONG_LAST_BUT_ONE_BIT);
        }
        return new Base36(v);
    }

    /** Just for tests. */
    // CHECKSTYLE stop magic number check
    public static void main(final String[] theArgs) {
        print(0);
        print(1);
        print(36);
        for (int i = 2; i < 20; i++) {
            print(i * i);
        }
        print(-1);
        print(Long.MAX_VALUE);
        print(Long.MIN_VALUE);

        final Random rnd = new Random();
        for (int i = 0; i < 100000; i++) {
            final long test = rnd.nextLong();
            final Base36 a = get(test);
            final Base36 b = get(a.toString());
            if (b.value != test) {
                throw new IllegalStateException(String.valueOf(test));
            }
        }
    }

    // CHECKSTYLE resume magic number check
    /** Returns the base-36 String representation of the value, treated as an
     * unsigned long. If fixedSize is true, it will be MAX_LEN character long.
     * 
     * @param theValue
     *        the value
     * @param theFixedSize
     *        flag to set the size of the string fixed.
     * @return base-36 String representation */
    public static String toString(final long theValue,
            final boolean theFixedSize) {
        final String str;
        if (theValue >= 0) {
            str = Long.toString(theValue, RADIX);
        } else {
            str = new BigInteger(Long.toString(theValue
                    & ~(1L << LONG_LAST_BUT_ONE_BIT))).setBit(LONG_LAST_BIT)
                    .toString(RADIX);
        }
        return theFixedSize ? fixedSize(str) : str;
    }

    /** Constructor */
    private Base36(final long theValue) {
        this.value = theValue;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(@Nullable final Object theObject) {
        if ((theObject == null) || !(theObject instanceof Base36))
            return false;
        final Base36 other = (Base36) theObject;
        return (value == other.value);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return 31 + (int) (value ^ (value >>> 32));
    }

    /** Returns the fixed-size String representation.
     * 
     * @return */
    public String toFixedString() {
        return fixedSize(toString());
    }

    /** Returns the String representation.
     * 
     * @return */
    @Override
    public String toString() {
        if (toString == null) {
            toString = toString(value, false);
        }
        return toString;
    }

    /** @return the value */
    public long value() {
        return value;
    }

}
