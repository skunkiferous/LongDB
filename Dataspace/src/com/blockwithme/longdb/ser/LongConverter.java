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
// $codepro.audit.disable finalMethodParameterInInterface
package com.blockwithme.longdb.ser;

/** Converts values to and from Long. TODO This just a copy of LongConverter in
 * BlowoenSer. Remove when BlowoenSer is finished and can be depended on.
 * 
 * @param <E>
 *        the element type */
public interface LongConverter<E> {

    /** The default implementation of long converter to and from same type */
    LongConverter<Long> DEFAULT = new LongConverter<Long>() {
        @Override
        public long fromValue(final Long theValue) {
            return theValue;
        }

        @Override
        public Long[] newArray(final int theLength) {
            return new Long[theLength];
        }

        @Override
        public Long toValue(final long theValue) {
            return theValue;
        }
    };

    /** Converts a value to a Long.
     * 
     * @param theValue
     *        the value
     * @return the value converted to long */
    long fromValue(E theValue);

    /** Creates an array of the given length.
     * 
     * @param theLength
     *        the length
     * @return an array of the given length */
    E[] newArray(int theLength);

    /** Converts a Long to a value.
     * 
     * @param theValue
     *        the value
     * @return the converted value */
    E toValue(long theValue);
}
