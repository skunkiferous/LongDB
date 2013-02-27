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
package com.blockwithme.longdb.cassandra;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/** The Cassandra configuration bean class. */
@ParametersAreNonnullByDefault
public class CassandraConfig {

    /** The cluster name. */
    private String clusterName;

    /** The database url. */
    private String dbUrl;

    /** The number of retries. */
    private int numRetries;

    /** The password. */
    private String password;

    /** The read consistency level. */
    private String readConsistencyLevel;

    /** The replication factor. */
    private int replicationFactor;

    /** The sleep between hosts milliseconds. */
    private int sleepBwHostsMilli;

    /** The strategy class. */
    private String strategyClass;

    /** The user name. */
    private String userName;

    /** The write consistency level. */
    private String writeConsistencyLevel;

    /** Cluster name.
     * 
     * @param theClusterName
     *        the cluster name */
    @Inject
    void clusterName(@Named("clusterName") final String theClusterName) {
        this.clusterName = theClusterName;
    }

    /** Db url.
     * 
     * @param theDBUrl
     *        the database url */
    @Inject
    void dbUrl(@Named("dbUrl") final String theDBUrl) {
        this.dbUrl = theDBUrl;
    }

    /** Number of retries.
     * 
     * @param theRetries
     *        the number of retries */
    @Inject
    void numRetries(@Named("failoverPolicy.numRetries") final int theRetries) {
        this.numRetries = theRetries;
    }

    /** Password.
     * 
     * @param thePassword
     *        the password */
    @Inject
    void password(
            @Nullable @Named("credentials.password") final String thePassword) {
        this.password = thePassword;
    }

    /** Read consistency level.
     * 
     * @param theReadConsistencyLevel
     *        the read consistency level */
    @Inject
    void readConsistencyLevel(
            @Named("consistencyLevel.read") final String theReadConsistencyLevel) {
        this.readConsistencyLevel = theReadConsistencyLevel;
    }

    /** Replication factor.
     * 
     * @param theReplicationFactor
     *        the replication factor */
    @Inject
    void replicationFactor(
            @Named("replicationFactor") final int theReplicationFactor) {
        this.replicationFactor = theReplicationFactor;
    }

    /** Sleep between hosts milliseconds.
     * 
     * @param theSleepBwHostsMilli
     *        the sleep between hosts value in milliseconds */
    @Inject
    void sleepBwHostsMilli(
            @Named("failoverPolicy.sleepBwHostsMilli") final int theSleepBwHostsMilli) {
        this.sleepBwHostsMilli = theSleepBwHostsMilli;
    }

    /** Strategy class.
     * 
     * @param theStrategyClass
     *        the strategy class */
    @Inject
    void strategyClass(@Named("strategyClass") final String theStrategyClass) {
        this.strategyClass = theStrategyClass;
    }

    /** User name.
     * 
     * @param theUserName
     *        the user name */
    @Inject
    void userName(@Nullable @Named("credentials.user") final String theUserName) {
        this.userName = theUserName;
    }

    /** Write consistency level.
     * 
     * @param theWriteConsistencyLevel
     *        the write consistency level */
    @Inject
    void writeConsistencyLevel(
            @Named("consistencyLevel.write") final String theWriteConsistencyLevel) {
        this.writeConsistencyLevel = theWriteConsistencyLevel;
    }

    /** Cluster name.
     * 
     * @return the Cassandra Cluster name */
    public String clusterName() {
        return clusterName;
    }

    /** Database url. 'hostname':'port'
     * 
     * @return the Database url */
    public String dbUrl() {
        return dbUrl;
    }

    /** Number of retries.
     * 
     * @return the number of retries */
    public int numRetries() {
        return numRetries;
    }

    /** Password.
     * 
     * @return the password */
    public String password() {
        return password;
    }

    /** Read consistency level.
     * 
     * @return the read consistency level */
    public String readConsistencyLevel() {
        return readConsistencyLevel;
    }

    /** Replication factor.
     * 
     * @return the replication factor */
    public int replicationFactor() {
        return replicationFactor;
    }

    /** sleep between hosts time in milliseconds.
     * 
     * @return the sleep between hosts time in milliseconds */
    public int sleepBwHostsMilli() {
        return sleepBwHostsMilli;
    }

    /** Strategy class name.
     * 
     * @return the Strategy class name */
    public String strategyClass() {
        return strategyClass;
    }

    /** User name.
     * 
     * @return the user name */
    public String userName() {
        return userName;
    }

    /** Write consistency level.
     * 
     * @return the write consistency level. */
    public String writeConsistencyLevel() {
        return writeConsistencyLevel;
    }

}
