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
// $codepro.audit.disable typeDepth, useEquals, declareAsInterface
package com.blockwithme.longdb;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Set;

import com.blockwithme.longdb.Columns;
import com.blockwithme.longdb.entities.Bytes;
import com.carrotsearch.hppc.cursors.LongObjectCursor;

/** A Map is a mapping from keys of one type to values of another type, where
 * both can be converted to a primitive long. TODO Currently, the whole row is
 * read and converted to a LongLongOpenHashMap. Ideally, it should be possible
 * to access the row directly, by giving the row a "Map interface". This would
 * be useful if only a small portion of the data is read.
 * 
 * @param <KEY>
 *        the key type
 * @param <KEYS>
 *        the keys type
 * @param <VALUE>
 *        the value type
 * @param <VALUES>
 *        the values type */

public class Map<KEY, KEYS, VALUE, VALUES> extends AbstractMap<KEY, VALUE>
        implements Iterable<Map.Entry<KEY, VALUE>>, Serializable {

    /** The Class EntrySet. */
    private class EntrySet extends AbstractSet<Map.Entry<KEY, VALUE>> implements
            Set<Map.Entry<KEY, VALUE>> {

        /** The iterator. */
        private final Iterator<LongObjectCursor<Bytes>> iter = mapIterator();

        /*
         * (non-Javadoc)
         * 
         * @see java.util.AbstractCollection#iterator()
         */
        @Override
        public Iterator<Map.Entry<KEY, VALUE>> iterator() {
            return new Iterator<Map.Entry<KEY, VALUE>>() {
                private LongObjectCursor<Bytes> next;

                @Override
                public boolean hasNext() {
                    return iter.hasNext();
                }

                @Override
                public Entry<KEY, VALUE> next() {
                    next = iter.next();
                    return new Entry<KEY, VALUE>() {
                        @Override
                        public KEY getKey() {
                            return long2key(next.key);
                        }

                        @Override
                        public VALUE getValue() {
                            return long2value(next.value.primitiveValue());
                        }

                        @Override
                        public VALUE setValue(final VALUE theValue) {
                            return Map.this.put(getKey(), theValue);
                        }
                    };
                }

                @Override
                public void remove() {
                    Map.this.remove(next.key);
                }
            };
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.util.AbstractCollection#size()
         */
        @Override
        public int size() {
            return Map.this.size();
        }
    }

    /** serialVersionUID. */
    static final long serialVersionUID = 42L;

    /** The Map content: (map key = column name) -> (map value = column value). */
    private final transient Columns cols;

    /** The key datatype. */
    @edu.umd.cs.findbugs.annotations.SuppressWarnings
    private final transient Datatype<KEY, KEYS> keyType;

    /** The value datatype. */
    @edu.umd.cs.findbugs.annotations.SuppressWarnings
    private final transient Datatype<VALUE, VALUES> valueType;

    /** @param theKeyType
     *        the key type
     * @param theValueType
     *        the value type
     * @param theCols
     *        the columns */
    public Map(final Datatype<KEY, KEYS> theKeyType,
            final Datatype<VALUE, VALUES> theValueType, final Columns theCols) {
        if (theKeyType == null) {
            throw new NullPointerException("keyType");
        }
        if (theValueType == null) {
            throw new NullPointerException("valueType");
        }
        if (theKeyType.converter() == null) {
            throw new NullPointerException("keyType.converter()");
        }
        if (theValueType.converter() == null) {
            throw new NullPointerException("valueType.converter()");
        }
        this.keyType = theKeyType;
        this.valueType = theValueType;
        this.cols = theCols;
    }

    /** Returns true if we contain the given key. */
    private boolean containsKey(final long theKey) {
        return cols.containsColumn(theKey);
    }

    /** Returns a mapping. */
    private long get(final long theKey) {
        final Bytes result = cols.getBytes(theKey);
        return (result == null) ? 0 : result.primitiveValue();
    }

    /** Converts a KEY to a long. */
    private long key2long(final KEY theKey) {
        return keyType.converter().fromValue(theKey);
    }

    /** Converts a long to a KEY. */
    private KEY long2key(final long theKey) {
        return keyType.converter().toValue(theKey);
    }

    /** Converts a long to a VALUE. */
    private VALUE long2value(final long theValue) {
        return valueType.converter().toValue(theValue);
    }

    /** Returns a Iterator<LongObjectCursor<Bytes>> over the map. */
    private Iterator<LongObjectCursor<Bytes>> mapIterator() {
        return cols.bytesIterator();
    }

    /** Creates/Updates a mapping. */
    private long put(final long theKey, final long theValue) {
        final Bytes result = cols.putBytes(theKey, new Bytes(theValue));
        return (result == null) ? 0 : result.primitiveValue();
    }

    /** Removes a mapping. */
    private long remove(final long theKey) {
        final Bytes result = cols.removeBytes(theKey);
        return (result == null) ? 0 : result.primitiveValue();
    }

    /** Converts a VALUE to a long. */
    private long value2long(final VALUE theValue) {
        return valueType.converter().fromValue(theValue);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.AbstractMap#containsKey(java.lang.Object)
     */
    @Override
    public boolean containsKey(final Object theKey) {
        return get(theKey) != null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.AbstractMap#entrySet()
     */
    @Override
    public EntrySet entrySet() {
        return new EntrySet();
    }

    /** Returns a reference, if present.
     * 
     * @param theObject
     *        the obj
     * @return the value */
    @Override
    public VALUE get(final Object theObject) {
        if ((theObject == null) || (theObject.getClass() != keyType.type())) {
            return null;
        }
        @SuppressWarnings("unchecked")
        final long key = key2long((KEY) theObject);
        if (containsKey(key)) {
            return long2value(get(key));
        }
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Iterable#iterator()
     */
    @Override
    public Iterator<Map.Entry<KEY, VALUE>> iterator() {
        return entrySet().iterator();
    }

    /** Returns the key datatype.
     * 
     * @return the datatype */
    public Datatype<KEY, KEYS> keyType() {
        return keyType;
    }

    /** Inserts a new mapping .
     * 
     * @param theKey
     *        the key
     * @param theValue
     *        the value
     * @return the value */
    @Override
    public VALUE put(final KEY theKey, final VALUE theValue) {
        if (theKey == null) {
            throw new IllegalArgumentException("key == null");
        }
        if (theValue == null) {
            throw new IllegalArgumentException("value == null");
        }
        // You can't trust generics!
        if (theKey.getClass() != keyType.type()) {
            throw new IllegalArgumentException("key is a " + theKey.getClass());
        }
        if (theValue.getClass() != valueType.type()) {
            throw new IllegalArgumentException("value is a "
                    + theKey.getClass());
        }
        final long key2 = key2long(theKey);
        final long value2 = value2long(theValue);
        if (containsKey(key2)) {
            return long2value(put(key2, value2));
        }
        put(key2, value2);
        return null;
    }

    /** Removes a reference, if present.
     * 
     * @param theObject
     *        the obj
     * @return the value */
    @Override
    public VALUE remove(final Object theObject) {
        if ((theObject == null) || (theObject.getClass() != keyType.type())) {
            return null;
        }
        @SuppressWarnings("unchecked")
        final long key = key2long((KEY) theObject);
        if (containsKey(key)) {
            return long2value(remove(key));
        }
        return null;
    }

    /** Returns the number of entries.
     * 
     * @return the int */
    @Override
    public int size() {
        return cols.size();
    }

    /** Returns the value datatype.
     * 
     * @return the datatype */
    public Datatype<VALUE, VALUES> valueType() {
        return valueType;
    }
}
