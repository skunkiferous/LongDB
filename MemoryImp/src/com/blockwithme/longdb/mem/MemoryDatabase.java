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

import java.util.Map;

import javax.annotation.ParametersAreNonnullByDefault;

import com.blockwithme.longdb.BETableProfile;
import com.blockwithme.longdb.base.AbstractDatabase;
import com.blockwithme.longdb.entities.Base36;

// TODO: Auto-generated Javadoc
/** Implementation of a memory-backed BEDatabase. Useful for testing. */
@ParametersAreNonnullByDefault
public class MemoryDatabase extends
        AbstractDatabase<MemoryBackend, MemoryDatabase, MemoryTable> {

    /** Instantiates a new memory database.
     * 
     * @param theBackend the backend
     * @param theDB the database */
    protected MemoryDatabase(final MemoryBackend theBackend, final String theDB) {
        super(theBackend, theDB);
    }

    /* (non-Javadoc)
     * @see com.blockwithme.longdb.base.AbstractDatabase#closeInternal() */
    @Override
    protected void closeInternal() {
        // NOP
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.base.AbstractDatabase#createInternal(com.blockwithme
     * .longdb.entities.Base36, com.blockwithme.longdb.BETableProfile) */
    @Override
    protected MemoryTable createInternal(final Base36 theTable,
            final BETableProfile theProfile) {
        return new MemoryTable(this, theTable,
                theProfile.reverseColumnsOrder(), false);
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.base.AbstractDatabase#dropInternal(com.blockwithme
     * .longdb.base.AbstractTable) */
    @Override
    protected void dropInternal(final MemoryTable theTable) {
        theTable.dropped();
    }

    /* (non-Javadoc)
     * @see
     * com.blockwithme.longdb.base.AbstractDatabase#openInternal(java.util.Map) */
    @Override
    protected void openInternal(final Map<Base36, MemoryTable> theTables) {
        // NOP
    }
}
