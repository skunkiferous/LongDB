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

import java.util.ArrayList;
import java.util.List;

import javax.annotation.ParametersAreNonnullByDefault;

import com.blockwithme.longdb.Backend;
import com.blockwithme.longdb.discovery.BackendInstance;
import com.blockwithme.longdb.discovery.BackendType;
import com.google.inject.Binder;

/** The Class Memory DB Type. */
@ParametersAreNonnullByDefault
public class MemoryDBType implements BackendType {

    /** The Project Name constant. */
    public static final String PROJECT_NAME = "MemoryImpl";

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.discovery.BackendType#description()
     */
    @Override
    public String description() {
        return "Implementation of a memory-backed backend. Useful for testing.";
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.discovery.BackendType#instances()
     */
    @Override
    public List<BackendInstance> instances() {

        final BackendInstance ins = new BackendInstance() {
            @Override
            public void configure(final Binder theBinder) {
                theBinder.bind(Backend.class).to(MemoryBackend.class);
            }

            @Override
            public String description() {
                return "default instance";
            }

            @Override
            public String name() {
                return "default";
            }
        };
        final List<BackendInstance> instances = new ArrayList<BackendInstance>();
        instances.add(ins);
        return instances;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.discovery.BackendType#name()
     */
    @Override
    public String name() {
        return PROJECT_NAME;
    }
}
