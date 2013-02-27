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
// $codepro.audit.disable largeNumberOfParameters, pluralizeCollectionNames
//CHECKSTYLE stop magic number check
package com.blockwithme.longdb;

import static com.blockwithme.longdb.common.constants.ByteConstants.BYTE_BITS;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

import com.blockwithme.longdb.Backend;
import com.blockwithme.longdb.entities.Bytes;
import com.blockwithme.longdb.util.Util;

/** <code>Dataspace</code> represents the dimensionality of some data. It can be
 * used to describe a data type, or a selection.
 * 
 * @author sdiot */
public final class Dataspace implements Serializable {

    /** serialVersionUID. */
    static final long serialVersionUID = 42L;

    /** Used for scalars. */
    private static final long[] EMPTY_ARRAY = new long[0];

    /** 1 */
    private static final BigInteger ONE = BigInteger.ONE;

    /** [0L] */
    private static final long[] ZERO = new long[1];

    /**  */
    // TODO : not sure why use number 31 here
    private static final int NUMBER_31 = 31;

    /** Long.MAX_VALUE */
    private static final BigInteger MAX_SIZE = new BigInteger(
            String.valueOf(Long.MAX_VALUE));

    /** Cache */
    private static final ConcurrentHashMap<Dataspace, Dataspace> CACHE = new ConcurrentHashMap<Dataspace, Dataspace>(
            256, 0.75f, 4);

    /** Maximum number of dimensions. 6 is used to match the BlowoenSer wire
     * protocol. I am confident that 99.9% of programmers will never need to use
     * arrays with a rank higher than 5 (variable-length content in "3D + time"
     * space), so I don't see this as an important limitation. */
    public static final int MAX_RANK = 6;

    /** Default chunking for arrays. */
    public static final int DEFAULT_CHUNKING = 256;

    /** Means unlimited in the negative for the minimum index. */
    public static final long UNLIMITED_MIN = Long.MIN_VALUE;

    /** Means unlimited in the positive for the maximum index. */
    public static final long UNLIMITED_MAX = Long.MAX_VALUE;

    /** Means the size of a limited dimension is bigger than cannot be
     * represented with a positive long. */
    public static final long HUGE_SIZE = -1;

    /** Means unlimited in the size of a dimension. */
    public static final long UNLIMITED_SIZE = -2;

    /** [UNLIMITED_SIZE] */
    private static final long[] UNLIMITED_SIZE2 = new long[] { UNLIMITED_SIZE };

    /** Scalar (0 dimensional) Dataspace. */
    public static final Dataspace SCALAR = new Dataspace((byte) 0, EMPTY_ARRAY,
            EMPTY_ARRAY, EMPTY_ARRAY, false, false, 1L, null, null, true, ONE,
            1, "SCALAR").intern();

    /** Array Dataspace; 1-dimensional dataspace starting at 0, and unlimited. */
    public static final Dataspace ARRAY = new Dataspace((byte) 1, new long[1],
            new long[] { UNLIMITED_MAX }, new long[] { UNLIMITED_SIZE }, true,
            false, UNLIMITED_SIZE, null, null, true, ONE, 2, "ARRAY").intern();

    /** Array Dataspace; 1-dimensional dataspace starting at 0, unlimited, with
     * DEFAULT_CHUNKING. */
    public static final Dataspace CHUNKED_ARRAY = array(DEFAULT_CHUNKING)
            .intern();

    /** The minimum index of each dimension, inclusive. The array length must be
     * equal to rank. Can be UNLIMITED_MIN. */
    private final long[] min;

    /** The maximum index of each dimension, *inclusive*. The array length must
     * be equal to rank. Can be UNLIMITED_MAX. */
    private final long[] max;

    /** The chunk size. */
    private final Dataspace chunk;

    /** Number of chunks (rounded up) in each dimension. Null if chunk is null. */
    private final transient long[] chunksPerDim;

    /** True if there is an exact integral number of chunks in each dimension. */
    private final transient boolean perfectChunking;

    /** Number of chunks total. 1 when chunk is null. */
    private final transient BigInteger chunks;

    /** Factors used to compute the chunk keys. */
    private transient BigInteger[] factors;

    /** The size of each dimension. The array length must be equal to rank. Can
     * be UNLIMITED_SIZE. */
    private final transient long[] size;

    /** The number of dimensions, or rank, of this dataspace. Limited to 32. 0 is
     * used for scalars. */
    private final transient byte rank;

    /** true if *any* dimension is unlimited. */
    private final transient boolean unlimited;

    /** true if *any* dimension is huge. */
    private final transient boolean huge;

    /** The total volume (number of cells) in this dataspace. Can be HUGE_SIZE or
     * UNLIMITED_SIZE */
    private final transient long volume;

    /** Cached hashcode */
    private final transient int hashCode;

    /** Cached toString */
    private transient String toString;

    /** Defines a new Dataspace for a uni-dimensional chunked array. Throws an
     * exception on bad arguments.
     * 
     * @param theChunking
     * @return the dataspace */
    public static Dataspace array(final int theChunking) {
        final Dataspace chunk = get(ZERO, new long[] { theChunking }, null);
        return get(ZERO, UNLIMITED_SIZE2, chunk);
    }

    /** Defines a new Dataspace. Throws an exception on bad arguments.
     * 
     * @param theMin
     *        the min
     * @param theMax
     *        the max
     * @return the dataspace */
    public static Dataspace get(final long[] theMin, final long[] theMax) {
        return get(theMin, theMax, null);
    }

    /** Defines a new Dataspace. Throws an exception on bad arguments.
     * 
     * @param theMin
     *        the min
     * @param theMax
     *        the max
     * @param theChunk
     *        the chunk
     * @return the dataspace */
    public static Dataspace get(final long[] theMin, final long[] theMax,
            final Dataspace theChunk) {
        if (theMin.length != theMax.length) {
            throw new IllegalArgumentException("#min: " + theMin.length
                    + " #max: " + theMax.length);
        }
        if (theMin.length > MAX_RANK) {
            throw new IllegalArgumentException("rank: " + theMin.length);
        }
        final byte rank = (byte) theMin.length;
        if (rank == 0) {
            if (theChunk != null) {
                throw new IllegalArgumentException(
                        "SCALAR cannot have a chunk!");
            }
            return SCALAR;
        }
        final long[] size = new long[rank];
        boolean unlimited = false;
        boolean huge = false;
        BigInteger volume = ONE;

        final int hashCode = (NUMBER_31
                * (NUMBER_31 * (NUMBER_31 + Arrays.hashCode(theMax))) + Arrays
                    .hashCode(theMin))
                + (theChunk == null ? 0 : theChunk.hashCode);

        final BigInteger[] _size = new BigInteger[rank];
        for (int i = 0; i < rank; i++) {
            final long _min = theMin[i];
            final long _max = theMax[i];
            if (_min > _max) {
                throw new IllegalArgumentException("min[" + i + "]: " + _min
                        + " max[" + i + "]: " + _max);
            }
            final BigInteger min2 = new BigInteger(String.valueOf(_min));
            final BigInteger max2 = new BigInteger(String.valueOf(_max));
            _size[i] = max2.subtract(min2).add(ONE);
            if ((_min == UNLIMITED_MIN) || (_max == UNLIMITED_MAX)) {
                size[i] = UNLIMITED_SIZE;
                unlimited = true;
            } else if (_size[i].compareTo(MAX_SIZE) <= 0) {
                size[i] = _size[i].longValue();
            } else {
                // Overflow!
                size[i] = HUGE_SIZE;
                huge = true;
            }
            volume = volume.multiply(_size[i]);
        }
        final long volume2;
        if (unlimited) {
            volume2 = UNLIMITED_SIZE;
        } else if (huge) {
            volume2 = HUGE_SIZE;
        } else if (volume.compareTo(MAX_SIZE) <= 0) {
            volume2 = volume.longValue();
        } else {
            volume2 = HUGE_SIZE;
        }

        if (unlimited) {
            huge = false;
        }

        long[] chunksPerDim = null;
        boolean perfectChunking = true;
        BigInteger chunks = ONE;
        if (theChunk != null) {
            if (theChunk.chunk != null) {
                throw new IllegalArgumentException(
                        "chunks cannot (currently) themselves have chunks");
            }
            if (rank != theChunk.rank) {
                throw new IllegalArgumentException("rank: " + rank
                        + " chunk.rank: " + theChunk.rank);
            }
            if (theChunk.unlimited) {
                throw new IllegalArgumentException("chunk.unlimited: true");
            }
            if (theChunk.huge) {
                throw new IllegalArgumentException("chunk.huge: true");
            }
            if (theChunk.volume == HUGE_SIZE) {
                throw new IllegalArgumentException("chunk.volume: HUGE_SIZE");
            }
            chunksPerDim = new long[rank];
            boolean multipleChunks = false;
            for (int i = 0; i < rank; i++) {
                final long cmin = theChunk.min[i];
                if (cmin != 0) {
                    throw new IllegalArgumentException("chunk.min[" + i + "]: "
                            + cmin);
                }
                final long mysize = size[i];
                final long csize = theChunk.size[i];
                if ((mysize != HUGE_SIZE) && (mysize != UNLIMITED_SIZE)
                        && (mysize < csize)) {
                    throw new IllegalArgumentException("chunk.size[" + i
                            + "]: " + csize + " > size[" + i + "]: " + mysize);
                }
                final BigInteger csize2 = new BigInteger(String.valueOf(csize));
                final BigInteger[] dR = _size[i].divideAndRemainder(csize2);
                BigInteger cpd = dR[0];
                if (dR[1].signum() > 0) {
                    // changed by Tarun, earlier this was just 'cpd.add(ONE)'
                    // resultant value was not being assigned to cpd.
                    cpd = cpd.add(ONE);
                    perfectChunking = false;
                }
                if (cpd.compareTo(MAX_SIZE) > 0) {
                    throw new IllegalArgumentException("chunk.size[" + i
                            + "]: " + csize + "  Too many chunks in dimension "
                            + i);
                }
                chunks = chunks.multiply(cpd);
                chunksPerDim[i] = cpd.longValue();
                if (chunksPerDim[i] > 1) {
                    multipleChunks = true;
                }
            }
            if (!multipleChunks) {
                throw new IllegalArgumentException(
                        "dataspace can only contain one chunk!");
            }
        }

        return new Dataspace(rank, theMin, theMax, size, unlimited, huge,
                volume2, theChunk, chunksPerDim, perfectChunking, chunks,
                hashCode, null);
    }

    /** Defines a new Dataspace for a single point. Throws an exception on bad
     * arguments.
     * 
     * @param thePositions
     *        the positions
     * @return the dataspace */
    public static Dataspace point(final int... thePositions) {
        final long[] longs = new long[thePositions.length];
        for (int i = 0; i < longs.length; i++) {
            longs[i] = thePositions[i];
        }
        return point(longs);
    }

    /** Defines a new Dataspace for a single point. Throws an exception on bad
     * arguments.
     * 
     * @param thePositions
     *        the positions
     * @return the dataspace */
    public static Dataspace point(final long... thePositions) {
        return get(thePositions, thePositions, null);
    }

    /** De-serializes the Name.
     * 
     * @param theBackend
     *        the backend
     * @param theBytes
     *        the bytes
     * @return the dataspace */
    public static Dataspace unmarshal(final Backend theBackend,
            final Bytes theBytes) {
        final byte[] data = theBytes.toArray(false);
        byte rank = data[0];
        boolean nonZeroMin = false;
        final int count; // Me + children
        if (rank < 0) {
            nonZeroMin = true;
            rank = (byte) -rank;
            count = (data.length - 1 - rank * BYTE_BITS) / (rank * BYTE_BITS);
        } else {
            count = (data.length - 1) / (rank * BYTE_BITS);
        }
        final long[] longs = new long[(data.length - 1) / BYTE_BITS];
        for (int i = 0; i < longs.length; i++) {
            longs[i] = Util.unmarshalLong(data, 1 + i * BYTE_BITS);
        }
        final long[] zero = new long[rank];
        final int offset = nonZeroMin ? rank * BYTE_BITS : 0;
        Dataspace result = null;
        for (int i = count - 1; i >= 0; i--) {
            final int pos = i * rank * BYTE_BITS;
            final long[] min;
            final long[] max = new long[rank];
            if (nonZeroMin && (i == 0)) {
                min = new long[rank];
                for (int j = 0; j < rank; j++) {
                    min[j] = longs[j * 2];
                    max[j] = longs[j * 2 + 1];
                }

            } else {
                min = zero;
                for (int j = 0; j < rank; j++) {
                    max[j] = longs[j + pos + offset];
                }
            }
            result = Dataspace.get(min, max, result);
        }
        return result;
    }

    /** Constructor */
    private Dataspace(final byte theRank, final long[] theMin,
            final long[] theMax, final long[] theSize,
            final boolean isUnlimited, final boolean isHuge,
            final long theVolume, final Dataspace theChunk,
            final long[] theChunksPerDim, final boolean isPerfectChunking,
            final BigInteger theChunks, final int theHashCode,
            final String theToString) {
        this.rank = theRank;

        if (theMin != null)
            this.min = Arrays.copyOf(theMin, theMin.length);
        else
            this.min = null;

        if (theMax != null)
            this.max = Arrays.copyOf(theMax, theMax.length);
        else
            this.max = null;

        if (theSize != null)
            this.size = Arrays.copyOf(theSize, theSize.length);
        else
            this.size = null;

        this.unlimited = isUnlimited;
        this.huge = isHuge;
        this.volume = theVolume;
        this.chunk = theChunk;

        if (theChunksPerDim != null)
            this.chunksPerDim = Arrays.copyOf(theChunksPerDim,
                    theChunksPerDim.length);
        else
            this.chunksPerDim = null;

        this.perfectChunking = isPerfectChunking;
        this.chunks = theChunks;
        this.hashCode = theHashCode;
        this.toString = theToString;
    }

    /** De-serializes */
    private Object readResolve() {
        return get(min, max, chunk).intern();
    }

    /** The chunk size. It can be null (the chunk itself cannot (currently) have
     * chunks). The dataspace must be a multiple (> 1) of the chunk size, if
     * present.
     * 
     * @return the dataspace */
    public Dataspace chunk() {
        return chunk;
    }

    /** Number of chunks total. 1 when chunk is null.
     * 
     * @return the big integer */
    public BigInteger chunks() {
        return chunks;
    }

    /** Number of chunks (rounded up) in each dimension. 1 if chunk is null. */
    public long chunksPerDim(final int theDimension) {
        final long[] cpd = chunksPerDim;
        return (cpd == null) ? 1 : cpd[theDimension];
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(final Object theObject) {
        if (this == theObject) {
            return true;
        }
        if (theObject == null) {
            return false;
        }
        if (!(theObject instanceof Dataspace)) {
            return false;
        }
        final Dataspace other = (Dataspace) theObject;
        if (hashCode != other.hashCode) {
            return false;
        }
        if (!Arrays.equals(max, other.max)) {
            return false;
        }
        if (!Arrays.equals(min, other.min)) {
            return false;
        }
        if ((chunk == null) ? (other.chunk != null) : !chunk
                .equals(other.chunk)) {
            return false;
        }
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return hashCode;
    }

    /** true if *any* dimension is huge, but no dimension is unlimited.
     * 
     * @return true, if *any* dimension is huge, but no dimension is unlimited. */
    public boolean huge() {
        return huge;
    }

    /** Returns a cached version of this Dataspace.
     * 
     * @return the dataspace */
    public Dataspace intern() {
        final Dataspace intern = CACHE.putIfAbsent(this, this);
        if (intern != null) {
            return intern;
        }
        return this;
    }

    /** Returns the chunk key for the given coordinate.
     * 
     * @param theCoords
     *        the coordinates
     * @return the byte[] */
    public byte[] keyFor(final long... theCoords) {
        if (theCoords.length != rank) {
            throw new IllegalArgumentException("rank: " + rank + " #coords: "
                    + theCoords.length);
        }
        if (factors == null) {
            factors = new BigInteger[rank];
            BigInteger factor = ONE;
            for (int i = 0; i < rank; i++) {
                factors[i] = factor;
                factor = factor.multiply(new BigInteger(String
                        .valueOf(chunksPerDim[i - 1])));
            }
        }
        BigInteger key = BigInteger.ZERO;
        final long[] tempMin = this.min;
        final long[] tempMax = this.max;
        final long[] chunkSize = chunk.size;
        for (int i = 0; i < rank; i++) {
            final long c = theCoords[i];
            final long _min = tempMin[i];
            final long _max = tempMax[i];
            if (c < _min) {
                throw new IllegalArgumentException("min[" + i + "]: " + _min
                        + " coords[" + i + "]: " + c);
            }
            if (c > _max) {
                throw new IllegalArgumentException("max[" + i + "]: " + _min
                        + " coords[" + i + "]: " + c);
            }
            final long index = (c - _min) / chunkSize[i];
            key = key.add(new BigInteger(String.valueOf(index))
                    .multiply(factors[i]));
        }
        return key.toByteArray();
    }

    /** Returns the chunk key size.
     * 
     * @return the int */
    public int keySize() {
        return (chunks == null) ? 0 : chunks.toByteArray().length;
    }

    /** Serializes the Name.
     * 
     * @return the bytes */
    public Bytes marshal() {
        int children = 0;
        // Normally, there should be just one level, but this is future-roof ...
        Dataspace ds = chunk;
        while (ds != null) {
            children++;
            ds = ds.chunk;
        }
        final byte theRank = this.rank;
        boolean nonZeroMin = false;
        for (int i = 0; i < theRank; i++) {
            if (min[i] != 0) {
                nonZeroMin = true;
                break;
            }
        }
        int tempSize = 1 /* rank */;
        if (nonZeroMin) {
            tempSize += theRank * (16 + children * 8);
        } else {
            tempSize += theRank * (8 + children * 8);
        }
        final byte[] bytes = new byte[tempSize];
        bytes[0] = nonZeroMin ? theRank : (byte) -theRank;
        int offset = 1;
        ds = this;
        while (ds != null) {
            final long[] tempMin = ds.min;
            final long[] tempMax = ds.max;
            for (int i = 0; i < theRank; i++) {
                if (nonZeroMin) {
                    Util.marshal(tempMin[i], bytes, offset);
                    offset += 8;
                }
                Util.marshal(tempMax[i], bytes, offset);
                offset += 8;
            }
            nonZeroMin = false;
            ds = ds.chunk;
        }
        return new Bytes(bytes);
    }

    /** The maximum index of each dimension, *inclusive*. The array length must
     * be equal to rank. Can be UNLIMITED_MAX.
     * 
     * @param theDimension
     *        the dim
     * @return the long */
    public long max(final int theDimension) {
        return max[theDimension];
    }

    /** The minimum index of each dimension, inclusive. The array lenght must be
     * equal to rank. Can be UNLIMITED_MIN.
     * 
     * @param theDimension
     *        the dim
     * @return the long */
    public long min(final int theDimension) {
        return min[theDimension];
    }

    /** True if there is an exact integral number of chunks in each dimension.
     * 
     * @return true, if there is an exact integral number of chunks in each
     *         dimension */
    public boolean perfectChunking() {
        return perfectChunking;
    }

    /** The number of dimensions, or rank, of this dataspace. Limited to 32. 0 is
     * used for scalars.
     * 
     * @return the int */
    public int rank() {
        return rank;
    }

    /** The size of each dimension. The array length must be equal to rank. Can
     * be UNLIMITED_SIZE or HUGE_SIZE.
     * 
     * @param theDimension
     *        the dim
     * @return the long */
    public long size(final int theDimension) {
        return size[theDimension];
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        if (toString == null) {
            final StringBuilder buf = new StringBuilder();
            buf.append("Dataspace(rank=").append(rank);
            buf.append(", unlimited=").append(unlimited);
            buf.append(", dimensions=[");
            for (int i = 0; i < rank; i++) {
                final long _min = min[i];
                final long _max = max[i];
                if (i > 0) {
                    buf.append(", ");
                }
                buf.append('[');
                if (_min == UNLIMITED_MIN) {
                    buf.append('*');
                } else {
                    buf.append(_min);
                }
                buf.append(',');
                if (_max == UNLIMITED_MAX) {
                    buf.append('*');
                } else {
                    buf.append(_max);
                }
                buf.append(']');
            }
            buf.append("], ");
            buf.append(chunk);
            buf.append(')');

            toString = buf.toString();
        }
        return toString;
    }

    /** true if *any* dimension is unlimited.
     * 
     * @return true, if *any* dimension is unlimited. */
    public boolean unlimited() {
        return unlimited;
    }

    /** The total volume (number of cells) in this dataspace. Can be HUGE_SIZE or
     * UNLIMITED_SIZE
     * 
     * @return the long */
    public long volume() {
        return volume;
    }
}
// CHECKSTYLE.ON
