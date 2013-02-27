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
package com.blockwithme.longdb.mem;

import java.util.Iterator;

import javax.annotation.ParametersAreNonnullByDefault;

import com.blockwithme.longdb.Columns;
import com.blockwithme.longdb.base.AbstractKeyIterator;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.LongObjectCursor;

/** The KeyIterator class for MemoryDB implementation. */
@ParametersAreNonnullByDefault
public class MemoryKeyIterator extends AbstractKeyIterator<MemoryTable> {
    /** The rows. */
    private final Iterator<LongObjectCursor<Columns>> cursor;

    /** Instantiates a new memory key iterator.
     * 
     * @param theTable
     *        the table
     * @param theRows
     *        the rows */
    protected MemoryKeyIterator(final MemoryTable theTable,
            final LongObjectOpenHashMap<Columns> theRows) {
        super(theTable);
        cursor = theRows.iterator();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractKeyIterator#nextKey()
     */
    @Override
    protected long nextKey() {
        return cursor.next().key;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractKeyIterator#hasNext()
     */
    @Override
    public boolean hasNext() {
        return cursor.hasNext();
    }

}
