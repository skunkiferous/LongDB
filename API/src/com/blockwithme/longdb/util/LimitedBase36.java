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
// $codepro.audit.disable debuggingCode, handleNumericParsingErrors
package com.blockwithme.longdb.util;

import java.io.Serializable;
import java.util.Random;

/** Represents a limited 64bit positive base-36 value. The value is expressed as
 * *lower-case*. It does NOT covers the whole range of a long. It allows base-36
 * values between 000000000000 and zzzzzzzzzzzz to be expressed, which only
 * covers a part of the whole long range. Those values are converted to a
 * strictly negative long value (and back). The purpose is to express
 * (lower-case) "strings" ID as a long value, while at the same time being
 * negative so that it can share a "long key space" with another non-negative
 * (0+) key. Leading zeroes are not part of the String by default, and are
 * ignored when converting a String to a LimitedBase36. TODO Create a REAL JUnit
 * test */
public class LimitedBase36 implements Serializable {

    /** The maximum allowable internal value (000000000000). */
    private static final long MAX_VALUE_INTERNAL = -1L;

    /** The minimum allowable internal value (zzzzzzzzzzzz). */
    private static final long MIN_VALUE_INTERNAL = -4738381338321616896L;

    /** The base-36 radix */
    private static final int RADIX = 36;

    /** serialVersionUID */
    private static final long serialVersionUID = 1L;

    /** 12 Zeroes */
    private static final String ZEROS = "000000000000";

    /** The base-36 radix maximal length. */
    public static final int MAX_LEN = 12;

    /** The textual form. */
    private transient String toString;

    /** The (always negative) "internal" representation. */
    protected long value;

    /** Adds leading zeros, so that the string is always MAX_LEN long. */
    private static String fixedSize(final String theString) {
        return ZEROS.substring(theString.length()) + theString;
    }

    /** Converts the external value to an internal equivalent. */
    private static long fromExternal(final long theExternal) {
        return -(theExternal + 1);
    }

    /** Converts the internal value to an external equivalent that will convert
     * to the right textual representation. */
    private static long fromInternal(final long theInternal) {
        return -(theInternal + 1);
    }

    /** Just for tests. */
    private static void print(final long theValue) {
        final LimitedBase36 b36 = new LimitedBase36(theValue);
        System.out.println(theValue + " " + b36 + " " + b36.toFixedString()
                + " " + new LimitedBase36(b36.toString()).value);
    }

    // CHECKSTYLE stop magic number check
    /** Just for tests. */
    public static void main(final String[] theArg) { // $codepro.audit.disable
                                                     // illegalMainMethod
        print(fromExternal(0));
        print(fromExternal(1));
        print(fromExternal(36));
        for (int i = 2; i < 20; i++) {
            print(fromExternal(i * i));
        }
        print(MIN_VALUE_INTERNAL);
        print(MAX_VALUE_INTERNAL);

        final Random rnd = new Random();
        for (int i = 0; i < 100000; i++) {
            final long test = rnd.nextLong();
            if ((test >= MIN_VALUE_INTERNAL) && (test <= MAX_VALUE_INTERNAL)) {
                final LimitedBase36 a = new LimitedBase36(test);
                final LimitedBase36 b = new LimitedBase36(a.toString());
                if (b.value != test) {
                    throw new IllegalStateException(String.valueOf(test));
                }
            }
        }
    }

    // CHECKSTYLE resume magic number check
    /** Converts the base-36 (non-case-sensitive) String to the internal long
     * representation. */
    public static long textToInternal(String theValue) {
        final int len = theValue.length();
        if (len > MAX_LEN) {
            throw new IllegalArgumentException("Maximum length is: " + MAX_LEN
                    + " value: '" + theValue + "'");
        }
        theValue = theValue.toLowerCase();
        final long v = Long.parseLong(theValue, RADIX);
        return fromExternal(v);
    }

    /** Returns the base-36 String representation of the value, treated as an
     * internal representation. If fixedSize is true, it will be MAX_LEN
     * character long. */
    public static String toString(final long theInternal,
            final boolean theFixedSize) {
        final long external = fromInternal(theInternal);
        final String str = Long.toString(external, RADIX);
        return theFixedSize ? fixedSize(str) : str;
    }

    /** Constructs a LimitedBase36 from the internal representation. */
    public LimitedBase36(final long theInternal) {
        assert ((theInternal >= MIN_VALUE_INTERNAL) && (theInternal <= MAX_VALUE_INTERNAL));
        value = theInternal;
    }

    /** Constructs a LimitedBase36 from the case-insensitive string
     * representation. */
    public LimitedBase36(final String theText) {
        this(textToInternal(theText));
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object theObject) {
        if ((theObject == null) || !(theObject instanceof LimitedBase36))
            return false;
        final LimitedBase36 other = (LimitedBase36) theObject;
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

    /** Returns the fixed-size String representation. */
    public String toFixedString() {
        return fixedSize(toString());
    }

    /** Returns the String representation. */
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
