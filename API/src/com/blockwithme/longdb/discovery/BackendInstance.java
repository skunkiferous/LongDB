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
package com.blockwithme.longdb.discovery;

import javax.annotation.ParametersAreNonnullByDefault;

import com.google.inject.Module;

/** Represents a Backend Server/local Instance. <br>
 * In case of Client-Server Databases a Backend a Cluster of Database servers
 * catering to one DB instance, which is equivalent to term 'Cluster' in case of
 * Cassandra <br>
 * In case of Embedded Databases :<br>
 * CassandraEmbedded - Currently CassandraEmbedded doesn't allow to load more
 * than one embedded instances <br>
 * Berkeley Database (BDB) - Multiple instances can be configured to run in a
 * single JVM, each instance will point to a file */
@ParametersAreNonnullByDefault
public interface BackendInstance extends Module {

    /** Gives information about the instance, e.g. hostName/filePath
     * 
     * @return the Description */
    String description();

    /** Name of the DB instance as configured in the corresponding configuration
     * file.
     * 
     * @return the Name of the instance */
    String name();

}
