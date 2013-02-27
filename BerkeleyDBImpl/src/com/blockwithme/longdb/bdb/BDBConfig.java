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
package com.blockwithme.longdb.bdb;

import javax.annotation.ParametersAreNonnullByDefault;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.LockMode;

/** The Configuration bean class for properties present in BDBConfig.properties,
 * Guice annotations are present to inject the values of the properties */
@ParametersAreNonnullByDefault
public class BDBConfig {

    /** The cache mode settings. */
    private CacheMode cacheMode;

    /** The cache size. */
    private long cacheSize;

    /** The description. */
    private String description;

    /** The file path. */
    private String filePath;

    /** The locking flag. */
    private boolean locking;

    /** The lock mode settings. */
    private LockMode lockMode;

    /** The lock timeout. */
    private long lockTimeout;

    /** The backend name. */
    private String name;

    /** The transactional flag. */
    private boolean transactional;

    /** The transaction time out. */
    private long transactionTimeOut;

    /** Cache mode.
     * 
     * @param theCacheMode
     *        the cache mode */
    @Inject
    void cacheMode(@Named("cacheMode") final String theCacheMode) {
        this.cacheMode = CacheMode.valueOf(theCacheMode);
    }

    /** Sets the cache size.
     * 
     * @param theCacheSize
     *        the cache size */
    @Inject
    void cacheSize(@Named("cacheSize") final long theCacheSize) {
        this.cacheSize = theCacheSize;
    }

    /** Description.
     * 
     * @param theDescription
     *        the description */
    @Inject
    void description(@Named("description") final String theDescription) {
        this.description = theDescription;
    }

    /** Sets the file path.
     * 
     * @param theFilePath
     *        the file path */
    @Inject
    void filePath(@Named("filePath") final String theFilePath) {
        this.filePath = theFilePath;
    }

    /** @param theLocking
     *        sets locking on/off */
    @Inject
    void locking(@Named("locking") final boolean theLocking) {
        this.locking = theLocking;
    }

    /** @param theLockMode
     *        the lock mode */
    @Inject
    void lockMode(@Named("lockMode") final String theLockMode) {
        this.lockMode = LockMode.valueOf(theLockMode);
    }

    /** Sets the lock timeout.
     * 
     * @param theLockTimeout
     *        the lock timeout */
    @Inject
    void lockTimeout(@Named("lock_timeout") final long theLockTimeout) {
        this.lockTimeout = theLockTimeout;
    }

    /** Name.
     * 
     * @param theName
     *        the name */
    @Inject
    void name(@Named("name") final String theName) {
        this.name = theName;
    }

    /** Sets the transactional flat.
     * 
     * @param isTransactional
     *        the transactional flag */
    @Inject
    void transactional(@Named("transactional") final boolean isTransactional) {
        this.transactional = isTransactional;
    }

    /** Sets the transaction time out.
     * 
     * @param theTxnTimeOut
     *        the transaction time out */
    @Inject
    void transactionTimeOut(@Named("txn_timeout") final long theTxnTimeOut) {
        this.transactionTimeOut = theTxnTimeOut;
    }

    /** @return the cache mode */
    public CacheMode cacheMode() {
        return cacheMode;
    }

    /** Gets the cache size.
     * 
     * @return the cache size */
    public long cacheSize() {
        return cacheSize;
    }

    /** Description.
     * 
     * @return the string */
    public String description() {
        return description;
    }

    /** Gets the file path.
     * 
     * @return the file path */
    public String filePath() {
        return filePath;
    }

    /** @return true, if locking is on */
    public boolean locking() {
        return locking;
    }

    /** @return the lock mode */
    public LockMode lockMode() {
        return lockMode;
    }

    /** Gets the lock timeout.
     * 
     * @return the lock timeout */
    public long lockTimeout() {
        return lockTimeout;
    }

    /** @return the backend type name */
    public String name() {
        return name;
    }

    /** @return true, transactional flag */
    public boolean transactional() {
        return transactional;
    }

    /** Gets the transaction time out.
     * 
     * @return the transaction time out */
    public long transactionTimeOut() {
        return transactionTimeOut;
    }

}
