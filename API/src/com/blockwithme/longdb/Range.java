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
package com.blockwithme.longdb;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import com.blockwithme.longdb.common.constants.ByteConstants;

/** Represents a range. It can be used to represent a range of table keys, or row
 * columns, or a slice of column content. */
@ParametersAreNonnullByDefault
public class Range {

    /** End, inclusive. */
    private long end;

    /** Start, inclusive. */
    private long start;

    /** Returns a range that matches all possible long values.
     * 
     * @return the range */
    public static Range fullRange() {
        return new Range(Long.MIN_VALUE, Long.MAX_VALUE);
    }

    /** @param theStart
     *        the start of range
     * @param theEnd
     *        the end of range */
    public Range(final long theStart, final long theEnd) {
        this.start = theStart;
        this.end = theEnd;
    }

    /** Returns true if the range contains the value.
     * 
     * @param theValue
     *        the value to be checked.
     * @return true, if contains */
    public boolean contains(final long theValue) {
        return (start <= theValue) && (theValue <= end);
    }

    /** Returns true if the range is empty.
     * 
     * @return true, if empty */
    public boolean empty() {
        return end < start;
    }

    /** @return the range end */
    public long end() {
        return end;
    }

    /** @param theEnd
     *        the end to set */
    public Range end(final long theEnd) {
        this.end = theEnd;
        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(@Nullable final Object theObject) {
        if (this == theObject)
            return true;
        if (theObject == null)
            return false;
        if (!(theObject instanceof Range))
            return false;
        final Range other = (Range) theObject;
        if (end != other.end)
            return false;
        if (start != other.start)
            return false;
        return true;
    }

    /** Returns true if the range matches all possible long values.
     * 
     * @return true, if its full long number range. */
    public boolean full() {
        return (start == Long.MIN_VALUE) && (end == Long.MAX_VALUE);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + (int) (end ^ (end >>> ByteConstants.INT_BITS));
        result = prime * result
                + (int) (start ^ (start >>> ByteConstants.INT_BITS));
        return result;
    }

    /** @return the range start */
    public long start() {
        return start;
    }

    /** @param theStart
     *        the start to set */
    public Range start(final long theStart) {
        this.start = theStart;
        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "[" + start + "," + end + "]";
    }

}
