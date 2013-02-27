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

import com.blockwithme.longdb.base.AbstractBackend;

/** Implementation of a memory-backed backend. Useful for testing. */
@ParametersAreNonnullByDefault
public class MemoryBackend extends
        AbstractBackend<MemoryBackend, MemoryDatabase, MemoryTable> {

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractBackend#createDatabaseInternal(java
     * .lang.String)
     */
    @Override
    protected MemoryDatabase createDatabaseInternal(final String theDB) {
        return new MemoryDatabase(this, theDB);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.blockwithme.longdb.base.AbstractBackend#dropInternal(com.paintedboxes
     * .db.base.AbstractDatabase)
     */
    @Override
    protected void dropInternal(final MemoryDatabase theDB) {
        // NOP
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractBackend#openInternal(java.util.Map)
     */
    @Override
    protected void openInternal(final Map<String, MemoryDatabase> theDatabase) {
        // NOP
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractBackend#shutdownInternal()
     */
    @Override
    protected void shutdownInternal() {
        // NOP
    }

}
