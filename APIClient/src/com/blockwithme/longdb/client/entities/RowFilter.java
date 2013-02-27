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
package com.blockwithme.longdb.client.entities;

import javax.annotation.ParametersAreNonnullByDefault;

import com.blockwithme.longdb.Range;
import com.carrotsearch.hppc.LongArrayList;

/** RowFilter Criteria - To Specify the filter criteria that needs to be used
 * while exporting/importing Table Data to/from External Data streams. */
@ParametersAreNonnullByDefault
public class RowFilter {

    /** The in list. */
    private final LongArrayList inList;

    /** The range. */
    private final Range range;

    /** Instantiates a list based filter.
     * 
     * @param theInList
     *        the list of row ids to be included. */
    public RowFilter(final LongArrayList theInList) {
        this.inList = theInList;
        range = null;
    }

    /** Instantiates a new range based filter.
     * 
     * @param theRange
     *        the range */
    public RowFilter(final Range theRange) {
        this.range = theRange;
        inList = null;
    }

    /** List of row Ids to be included.
     * 
     * @return the list of row ids to be included. */
    public LongArrayList inList() {
        return inList;
    }

    /** Range filter.
     * 
     * @return rowId range to be included, returns null if filter is list based
     *         filter. */
    public Range range() {
        return range;
    }
}
