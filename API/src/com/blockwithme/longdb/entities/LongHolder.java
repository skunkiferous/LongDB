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
package com.blockwithme.longdb.entities;

import com.blockwithme.longdb.common.constants.ByteConstants;

/** Holds an long TODO This just a copy of LongHolder in BlowoenSer. Remove when
 * BlowoenSer is finished and can be depended on. */
public class LongHolder {
    /** The value. */
    private long value;

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object theOtherObj) {
        return ((theOtherObj instanceof LongHolder) && (value == ((LongHolder) theOtherObj).value));
    }

    /** Returns the raw primitive value bits. Only implemented by primitive
     * holders.
     * 
     * @return raw primitive value */
    public long getRaw() {
        return value;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return (int) (value ^ (value >>> ByteConstants.INT_BITS));
    }

    /** Sets the raw primitive value bits. Only implemented by primitive holders.
     * 
     * @param theRawBits
     *        the raw primitive value */
    public void setRaw(final long theRawBits) {
        value = theRawBits;
    }

    /** Returns the value as an object.
     * 
     * @return the value as an object */
    public Long toObject() {
        return Long.valueOf(value);
    }

    /** @return the value */
    public long value() {
        return value;
    }

    /** @param theValue
     *        the value to set */
    public void value(final long theValue) {
        this.value = theValue;
    }

}
