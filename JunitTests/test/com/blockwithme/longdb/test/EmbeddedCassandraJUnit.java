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
package com.blockwithme.longdb.test;

import com.blockwithme.longdb.Backend;
import com.blockwithme.longdb.cassandra.embedded.CassandraEmbConstants;
import com.blockwithme.longdb.discovery.BackendServiceLoader;

/** The Class EmbeddedCassandraJUnit. */
public class EmbeddedCassandraJUnit extends BETableJUnit {

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.test.BETableJUnit#backend()
     */
    @Override
    protected Backend backend() {
        return BackendServiceLoader.getInstance().loadInstance(
                CassandraEmbConstants.PROJECT_NAME, "default");
    }

}
