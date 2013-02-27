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

import com.google.inject.Inject;
import com.google.inject.name.Named;

/** The configuration bean class for embedded cassandra backend. */
@ParametersAreNonnullByDefault
public class CassandraEmbConfig {

    /** The cluster name. */
    private String clusterName;

    /** The replication factor. */
    private String replicationFactor;

    /** The strategy class. */
    private String strategyClass;

    /** Sets the cluster name.
     * 
     * @param theClusterName
     *        the cluster name */
    void clusterName(final String theClusterName) {
        this.clusterName = theClusterName;
    }

    /** sets the replication factor.
     * 
     * @param theReplicationFactor
     *        the replication factor */
    @Inject
    void replicationFactor(
            @Named("defaults.replicationFactor") final String theReplicationFactor) {
        this.replicationFactor = theReplicationFactor;
    }

    /** Sets the strategy class.
     * 
     * @param theStrategyClass
     *        the strategy class */
    @Inject
    void strategyClass(
            @Named("defaults.strategyClass") final String theStrategyClass) {
        this.strategyClass = theStrategyClass;
    }

    /** @return the cluster name. */
    public String clusterName() {
        return clusterName;
    }

    /** @return the replication factor */
    public String replicationFactor() {
        return replicationFactor;
    }

    /** @return the string */
    public String strategyClass() {
        return strategyClass;
    }

}
