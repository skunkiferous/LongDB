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
// $codepro.audit.disable debuggingCode, pluralizeCollectionNames
package com.blockwithme.longdb;

import static com.blockwithme.longdb.common.constants.ByteConstants.BYTE_BITS;
import static com.blockwithme.longdb.common.constants.ByteConstants.LONG_BITS;

import java.math.BigInteger;
import java.util.Random;

/** Merge multi-dimensional indexes into a single Morton ordering. TODO We need a
 * demorton2(int bitsPerValue, byte[] data) */
public class Morton {

    /** De-Mortonize one int from a mortonized a long. Number of bits per int
     * recovered is 64/N
     * 
     * @param theMortonized
     *        the mortonized number
     * @param theDimensions
     *        the dimensions
     * @param theIndex
     *        the index
     * @return the int */
    public static int demorton(final long theMortonized,
            final int theDimensions, final int theIndex) {
        final int bitsPerValue = LONG_BITS / theDimensions;
        int result = 0;
        for (int i = 0; i < bitsPerValue; i++) {
            result |= (theMortonized >> (theIndex + (theDimensions - 1) * i))
                    & (1L << i);
        }
        return result;
    }

    // CHECKSTYLE stop magic number check
    /** The main method.
     * 
     * @param theArgs
     *        the arguments */
    public static void main(final String[] theArgs) { // $codepro.audit.disable
                                                      // illegalMainMethod
        final int max = 1 << 21;
        final Random rnd = new Random();
        long totalMine = 0;
        long totalOther = 0;
        for (int i = 0; i < 1000; i++) {
            final int x = rnd.nextInt(max);
            final int y = rnd.nextInt(max);
            final int z = rnd.nextInt(max);
            final long beforeMine = System.nanoTime();
            final long mine = morton(x, y, z);
            totalMine += System.nanoTime() - beforeMine;
            final long beforeOther = System.nanoTime();
            final byte[] other = morton2(21, x, y, z);
            totalOther += System.nanoTime() - beforeOther;
            final byte[] reversed = new byte[other.length];
            for (int j = 0; j < reversed.length; j++) {
                reversed[j] = other[other.length - j - 1];
            }
            final BigInteger otherBD = new BigInteger(reversed);
            if (mine != otherBD.longValue()) {
                System.out.println("X: " + Integer.toBinaryString(x));
                System.out.println("Y: " + Integer.toBinaryString(y));
                System.out.println("Z: " + Integer.toBinaryString(z));
                System.out.println("MINE:  " + Long.toBinaryString(mine));
                System.out.println("OTHER: " + otherBD.toString(2));
            }
        }
        System.out.println("DONE!");
        System.out.println("MINE:  " + totalMine);
        System.out.println("OTHER: " + totalOther);
    }

    // CHECKSTYLE resume magic number check
    /** Mortonize N ints to a long. Number of bits per int is 64/N
     * 
     * @param theValues
     *        the values
     * @return the number */
    public static long morton(final int... theValues) {
        final int dimensions = theValues.length;
        final int bitsPerValue = LONG_BITS / dimensions;
        long result = 0;
        for (int i = 0; i < dimensions; i++) {
            final long v = theValues[i];
            for (int j = 0; j < bitsPerValue; j++) {
                result |= (v & (1L << j)) << (i + (dimensions - 1) * j);
            }
        }
        return result;
    }

    /** Mortonize N longs to a byte[] in <i>little-endian</i> byte-order: the
     * least significant byte is in the zeroth element. To create a BigInteger
     * from it, the array has to be reversed.
     * 
     * @param theBitsPerValue
     *        the bits per value
     * @param theValues
     *        the values
     * @return the byte[] */
    public static byte[] morton2(final int theBitsPerValue,
            final long... theValues) {
        final int dimensions = theValues.length;
        final int bits = dimensions * theBitsPerValue;
        final int bytes = (bits % BYTE_BITS == 0) ? bits / BYTE_BITS : 1 + bits
                / BYTE_BITS;
        final byte[] result = new byte[bytes];
        for (int i = 0; i < dimensions; i++) {
            final long v = theValues[i];
            for (int j = 0; j < theBitsPerValue; j++) {
                final byte bit = (byte) ((v & (1L << j)) >>> j);
                final int bitpos = (i + dimensions * j);
                final int bytepos = bitpos / BYTE_BITS; // Difference!
                result[bytepos] |= bit << (bitpos % BYTE_BITS);
            }
        }
        return result;
    }

    /** Mortonize N longs to a BigInteger. NB: A BigInteger is *very* expensive
     * to create!
     * 
     * @param theBitsPerValue
     *        the bits per value
     * @param theValues
     *        the values
     * @return the big integer */
    public static BigInteger morton3(final int theBitsPerValue,
            final long... theValues) {
        final int dimensions = theValues.length;
        final int bits = dimensions * theBitsPerValue;
        final int bytes = (bits % BYTE_BITS == 0) ? bits / BYTE_BITS : 1 + bits
                / BYTE_BITS;
        final byte[] result = new byte[bytes];
        for (int i = 0; i < dimensions; i++) {
            final long v = theValues[i];
            for (int j = 0; j < theBitsPerValue; j++) {
                final byte bit = (byte) ((v & (1L << j)) >>> j);
                final int bitpos = (i + dimensions * j);
                final int bytepos = bytes - (bitpos / BYTE_BITS) - 1; // Difference!
                result[bytepos] |= bit << (bitpos % BYTE_BITS);
            }
        }
        return new BigInteger(result);
    }
}
