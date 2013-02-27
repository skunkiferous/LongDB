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
package com.blockwithme.longdb.base;

import java.util.Iterator;

import javax.annotation.ParametersAreNonnullByDefault;

import com.blockwithme.longdb.BETable;
import com.blockwithme.longdb.entities.LongHolder;
import com.google.common.base.Preconditions;

/** Abstract implementation of a key iterator.
 * 
 * @param <E>
 *        the type of Table on which row iterator is implemented. */
@ParametersAreNonnullByDefault
public abstract class AbstractKeyIterator<E extends BETable> implements
        Iterator<LongHolder> {
    /** The LongHolder */
    private final LongHolder holder = new LongHolder();

    /** The table. */
    protected final E table;

    /** Did we remove the last value? */
    private boolean removed;

    /** Did we call next? */
    private boolean first = true;

    /** Protected Constructor.
     * 
     * @param theTable
     *        the table instance on which the row iterator is implemented. */
    protected AbstractKeyIterator(final E theTable) {
        Preconditions.checkNotNull(theTable, "table is null");
        this.table = theTable;
    }

    /** Returns the next key.
     * 
     * @return the long */
    protected abstract long nextKey();

    /** Do we have another key?.
     * 
     * @return true, if we have another key */
    @Override
    public abstract boolean hasNext();

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#next()
     */
    @Override
    public final LongHolder next() {
        holder.value(nextKey());
        removed = false;
        first = false;
        return holder;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#remove()
     */
    @Override
    public final void remove() {
        if (first) {
            throw new IllegalStateException("next() was never called");
        }
        if (removed) {
            throw new IllegalStateException("element already removed!");
        }
        table.remove(holder.value());
        removed = true;
    }
}
