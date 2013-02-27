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
// $codepro.audit.disable useEquals, unnecessaryCast, com.instantiations.eclipse.analysis.audit.security.incompatibleTypesStoredInACollection, declareAsInterface
package com.blockwithme.longdb;

import static com.blockwithme.longdb.common.constants.ByteConstants.BYTE_BITS;
import static com.blockwithme.longdb.common.constants.ByteConstants.INT_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.LONG_BYTES;
import static com.blockwithme.longdb.common.constants.ByteConstants.SHORT_BYTES;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blockwithme.longdb.ser.LongConverter;
import com.blockwithme.longdb.util.Util;

/** <code>Datatype</code> represents the type of some data, along with it's
 * dataspace. E is the object representation of a single value. A is the array
 * type of he values, which will be a primitive array if appropriate.
 * 
 * @param <E>
 *        the element type
 * @param <A>
 *        the generic type
 * @author sdiot */
public final class Datatype<E, A> implements Serializable {

    /** serialVersionUID. */
    static final long serialVersionUID = 42L;

    /** Log */
    private static final Logger LOG = LoggerFactory.getLogger(Datatype.class);

    /** Maps the supported wrapper to primitives. */
    private static final Map<Class<?>, Class<?>> W2P_MAP = new HashMap<Class<?>, Class<?>>();
    static {
        W2P_MAP.put(Boolean.class, Boolean.TYPE);
        W2P_MAP.put(Byte.class, Byte.TYPE);
        W2P_MAP.put(Short.class, Short.TYPE);
        W2P_MAP.put(Character.class, Character.TYPE);
        W2P_MAP.put(Integer.class, Integer.TYPE);
        W2P_MAP.put(Long.class, Long.TYPE);
        W2P_MAP.put(Float.class, Float.TYPE);
        W2P_MAP.put(Double.class, Double.TYPE);
    }

    /** Cache */
    private static final ConcurrentHashMap<Datatype<?, ?>, Datatype<?, ?>> CACHE = new ConcurrentHashMap<Datatype<?, ?>, Datatype<?, ?>>(
            256, 0.75f, 4);

    /** The real type name */
    private final String name;

    /** The real type */
    private final transient Class<E> type;

    /** The array type */
    private final transient Class<A> array;

    /** The LongConverter, if any. */
    private final transient LongConverter<E> converter;

    /** true if this is a scalar datatype. */
    private final transient boolean scalar;

    /** Minimum byte size */
    private final transient int minSize;

    /** Maximum byte size */
    private final transient int maxSize;

    /** Bits per value. Non zero for types that map to a primitive value. */
    private final transient byte bitsPerValue;

    /** Signed? Only possibly true for Number types. */
    private final transient boolean signed;

    /** toString */
    private final transient String toString;

    /** Hashcode */
    private final transient int hashCode;

    /** Returns a new datatype. The array type A is implicit, but can be read
     * with arrayOf(). */
    @SuppressWarnings("unchecked")
    private static <E, A> Datatype<E, A> create(Class<E> theType, String theName) {
        if (theType == String.class) {
            theName = "string";
        }
        Datatype<E, A> newDT = new Datatype<E, A>(theName, theType, true);
        Datatype<E, A> result = (Datatype<E, A>) CACHE.get(newDT);
        if (result == null) {
            final Class<E> primitive = (Class<E>) W2P_MAP.get(theType);
            if (primitive != null) {
                theType = primitive;
                newDT = new Datatype<E, A>(theName, theType, true);
            } else {
                // type == null only for failed read-resolve, which we allow.
                // Otherwise the type must be pre-defined.
                if ((theType != null) && !W2P_MAP.values().contains(theType)) {
                    throw new IllegalArgumentException("Unsupported datatype: "
                            + theType);
                }
                newDT = new Datatype<E, A>(theName, theType, false);
            }
            final Datatype<E, A> oldDT = (Datatype<E, A>) CACHE.putIfAbsent(
                    newDT, newDT);
            if (oldDT == null) {
                result = newDT;
            } else {
                result = oldDT;
            }
        }
        return result;
    }

    /** Returns the size for some type, if the type is recognized. Otherwise 0. */
    private static int sizeFor(final Class<?> theType) {
        if ((theType == Byte.class) || (theType == Boolean.class)) {
            return 1;
        }
        if ((theType == Short.class) || (theType == Character.class)) {
            return SHORT_BYTES;
        }
        if ((theType == Integer.class) || (theType == Float.class)) {
            return INT_BYTES;
        }
        if ((theType == Long.class) || (theType == Double.class)
                || (theType == Date.class) || (theType == Name.class)
                || (theType == Reference.class)) {
            return LONG_BYTES;
        }
        // Comes here if type is null ...
        return 0;
    }

    /** Returns a new datatype.
     * 
     * @param <E>
     *        the element type
     * @param <A>
     *        the generic type
     * @param theType
     *        the type
     * @return the datatype */
    public static <E, A> Datatype<E, A> get(final Class<E> theType) {
        return create(theType, theType.getName());
    }

    /** Returns a new datatype.
     * 
     * @param theTypeName
     *        the type name
     * @return the datatype */
    public static Datatype<?, ?> get(final String theTypeName) {
        Class<?> type = null;
        try {
            type = Class.forName(theTypeName);
        } catch (final ClassNotFoundException e) {
            LOG.error("Cannot find type '" + theTypeName + "'", e);
        }
        return create(type, theTypeName);
    }

    /** Constructor */
    @SuppressWarnings("unchecked")
    private Datatype(final String theName, final Class<E> theType,
            final boolean theScalar) {
        this.name = theName;
        this.type = theType;
        if (theType == null) {
            array = null;
        } else {
            // Ugly, but works.
            array = (Class<A>) Array.newInstance(theType, 0).getClass();
        }
        LongConverter<E> cvt = null;
        try {
            cvt = Util.createImplementation(theName + ".LongConverter",
                    LongConverter.class);
        } catch (final RuntimeException e) {
            LOG.debug("Failed to find LongConverter for " + theName, e);
        }
        converter = cvt;
        this.scalar = theScalar;
        final int size = sizeFor(theType);
        minSize = (size == 0) ? 1 : size;
        maxSize = (size == 0) ? Integer.MAX_VALUE : size;
        bitsPerValue = (byte) ((size == 0) ? 0 : ((Boolean.TYPE == theType) ? 1
                : (size / BYTE_BITS)));
        signed = Number.class.isAssignableFrom(theType);
        final StringBuilder buf = new StringBuilder();
        buf.append("Datatype(name=").append(theName);
        buf.append(')');
        toString = buf.toString();
        hashCode = toString.hashCode();
    }

    /** De-serializes */
    private Object readResolve() {
        return get(name);
    }

    /** The array type. If it cannot be resolved, then null! For primitive
     * wrapper types, this will actually be the primitive array type.
     * 
     * @return the class */
    public Class<A> arrayOf() {
        return array;
    }

    /** Bits per value. Non zero for types that map to a primitive value.
     * 
     * @return the byte */
    public byte bitsPerValue() {
        return bitsPerValue;
    }

    /** Returns the LongConverter, if any.
     * 
     * @return the long converter */
    public LongConverter<E> converter() {
        return converter;
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
        if (!(theObject instanceof Datatype)) {
            return false;
        }
        @SuppressWarnings("rawtypes")
        final Datatype other = (Datatype) theObject;
        if (hashCode != other.hashCode) {
            return false;
        }
        return name.equals(other.name);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return hashCode;
    }

    /** Returns the maximum size.
     * 
     * @return the int */
    public int maxSize() {
        return maxSize;
    }

    /** Returns the minimum size.
     * 
     * @return the int */
    public int minSize() {
        return minSize;
    }

    /** The real type name.
     * 
     * @return the string */
    public String name() {
        return name;
    }

    /** Returns true if this is a scalar datatype.
     * 
     * @return true, if this is a scalar datatype */
    public boolean scalar() {
        return scalar;
    }

    /** Signed? Only possibly true for Number types.
     * 
     * @return true, if Signed? */
    public boolean signed() {
        return signed;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return toString;
    }

    /** The real type. If it cannot be resolved, then null! For primitive wrapper
     * types, this will actually be the primitive type.
     * 
     * @return the class */
    public Class<E> type() {
        return type;
    }
}
