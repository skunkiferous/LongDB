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
// $codepro.audit.disable synchronizedMethod, assignmentToNonFinalStatic
package com.blockwithme.longdb.cassandra.util;

import java.io.IOException;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.cassandra.thrift.CassandraDaemon;

/** The Cassandra Daemon Service used by embedded database. */
@ParametersAreNonnullByDefault
public final class CassandraDeamonService {

    /** The cassandra instance. */
    private static CassandraDaemon CASSANDRA_INSTANCE; // $codepro.audit.disable
                                                       // variableShouldBeFinal

    /** The daemon service instance. */
    private static CassandraDeamonService INSTANCE;

    /** flag indicate if service is running. */
    private boolean started;

    /** Gets the singleton object.
     * 
     * @param theYamlPath
     *        the path to cassandra yaml file
     * @return the singleton object */
    public static synchronized CassandraDeamonService getSingletonObject(
            final String theYamlPath) throws IOException {
        System.setProperty("cassandra.config", "file:///" + theYamlPath);
        if (INSTANCE == null) {
            INSTANCE = new CassandraDeamonService();
        }
        return INSTANCE;
    }

    /** A private Constructor prevents any other class from instantiating. */
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
    private CassandraDeamonService() throws IOException {
        CASSANDRA_INSTANCE = new CassandraDaemon();
        CASSANDRA_INSTANCE.init(null);
        CASSANDRA_INSTANCE.start();
        started = true;
    }

    /** Stop Cassandra daemon service */
    public synchronized void stop() {
        if (started) {
            CASSANDRA_INSTANCE.stop();
            started = false;
        }
    }
}
