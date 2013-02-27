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
package com.blockwithme.longdb.cassandra.embedded;

import javax.annotation.ParametersAreNonnullByDefault;

import com.blockwithme.longdb.Backend;
import com.blockwithme.longdb.base.AbstractBackendType;

/** The Class Cassandra Embedded DB Type definition */
@ParametersAreNonnullByDefault
public class CassandraEmbeddedDBType extends AbstractBackendType {

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractBackendType#backendClass()
     */
    @Override
    protected Class<? extends Backend> backendClass() {
        return CassandraEmbBackend.class;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractBackendType#configBeanClass()
     */
    @Override
    protected Class<? extends Object> configBeanClass() {
        return CassandraEmbConfig.class;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractBackendType#configFileName()
     */
    @Override
    protected String configFileName() {
        return "CassandraEmbConfig.properties";
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.discovery.BackendType#name()
     */
    @Override
    public String name() {
        return CassandraEmbConstants.PROJECT_NAME;
    }
}
