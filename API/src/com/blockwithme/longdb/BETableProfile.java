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

/** A table profile contains hints to the backend about the expected usage
 * characteristics for that table. If the backend supports several table
 * implementations, or if it supports tuning parameters, those will be set
 * according to the values in the table profile. If the table changes such that
 * it is not representative of the profile anymore, the table should still work,
 * but it might result in bad performance, or resource depletion (out-of-memory
 * errors). The expected average row size is computed using
 * expectedNumberOfColumns * expectedSizeOfColumns. */
public class BETableProfile {
    /** The possible access patterns. */
    public static enum Access {
        /** The Append mostly access. */
        AppendMostly,
        /** The Balanced access. */
        Balanced,
        /** The Read mostly access. */
        ReadMostly,
        /** The Update mostly access. */
        UpdateMostly
    };

    /** The expected access pattern. */
    private final Access access = Access.Balanced;

    /** Expected average number of columns per rows, after the usage stabilizes. */
    private int expectedNumberOfColumns = -1;

    /** Expected number of rows, after the usage stabilizes. */
    private int expectedNumberOfRows = -1;

    /** Expected average size of columns in bytes, after the usage stabilizes. */
    private int expectedSizeOfColumns = -1;

    /** Try to achieve maximal speed by caching aggressively. The compromise
     * between throughput and latency is usually not configurable, so we just
     * use the generic term speed to mean both. */
    private boolean optimizeForSpeed;

    /** Should the columns order be reversed?. */
    private boolean reverseColumnsOrder;

    /** Use timestamp to detect collisions?. */
    private boolean tryDetectCollisions;

    /** @return the access */
    public Access access() {
        return access;
    }

    /** @return the expectedNumberOfColumns */
    public int expectedNumberOfColumns() {
        return expectedNumberOfColumns;
    }

    /** sets the expected Number Of Columns */
    public BETableProfile expectedNumberOfColumns(
            final int theExpectedNumberOfColumns) {
        expectedNumberOfColumns = theExpectedNumberOfColumns;
        return this;
    }

    /** @return the expectedNumberOfRows */
    public int expectedNumberOfRows() {
        return expectedNumberOfRows;
    }

    /** Expected number of rows.
     * 
     * @param theExpectedNumberOfRows
     *        the expected number of rows */
    public BETableProfile expectedNumberOfRows(final int theExpectedNumberOfRows) {
        expectedNumberOfRows = theExpectedNumberOfRows;
        return this;
    }

    /** @return the expectedSizeOfColumns */
    public int expectedSizeOfColumns() {
        return expectedSizeOfColumns;
    }

    /** Expected size of columns.
     * 
     * @param theExpectedSizeOfColumns
     *        the expected size of columns */
    public BETableProfile expectedSizeOfColumns(
            final int theExpectedSizeOfColumns) {
        expectedSizeOfColumns = theExpectedSizeOfColumns;
        return this;
    }

    /** @return the optimizeForSpeed */
    public boolean optimizeForSpeed() {
        return optimizeForSpeed;
    }

    /** @param theSpeedOptimization
     *        the optimizeForSpeed to set */
    public BETableProfile optimizeForSpeed(final boolean theSpeedOptimization) {
        this.optimizeForSpeed = theSpeedOptimization;
        return this;
    }

    /** @return the reverseColumnsOrder */
    public boolean reverseColumnsOrder() {
        return reverseColumnsOrder;
    }

    /** @param theReverseColumnsOrder
     *        the reverseColumnsOrder to set */
    public BETableProfile reverseColumnsOrder(
            final boolean theReverseColumnsOrder) {
        this.reverseColumnsOrder = theReverseColumnsOrder;
        return this;
    }

    /** @return the tryDetectCollisions */
    public boolean tryDetectCollisions() {
        return tryDetectCollisions;
    }

    /** @param theTryDetectCollisions
     *        the tryDetectCollisions to set */
    public BETableProfile tryDetectCollisions(
            final boolean theTryDetectCollisions) {
        this.tryDetectCollisions = theTryDetectCollisions;
        return this;
    }

}
