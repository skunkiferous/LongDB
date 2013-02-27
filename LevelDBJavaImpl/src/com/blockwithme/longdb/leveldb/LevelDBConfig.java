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
package com.blockwithme.longdb.leveldb;

import javax.annotation.ParametersAreNonnullByDefault;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/** The Class LevleDBConfig. */
@ParametersAreNonnullByDefault
public class LevelDBConfig {

    /** The cache size. */
    private long cacheSize;

    /** The compression on. */
    private boolean compressionOn;

    /** The file path. */
    private String filePath;

    /** The read fill cache. */
    private boolean readFillCache;

    /** The write buffer size. */
    private int writeBufferSize;

    /** The write synchronously. */
    private boolean writeSynchronously;

    /** Cache size.
     * 
     * @param theCacheSize
     *        the cache size */
    @Inject
    void cacheSize(@Named("cacheSize") final long theCacheSize) {
        this.cacheSize = theCacheSize;
    }

    /** Compression on.
     * 
     * @param theCompressionOn
     *        the compression on flag */
    @Inject
    void compressionOn(@Named("compressionOn") final boolean theCompressionOn) {
        this.compressionOn = theCompressionOn;
    }

    /** File path.
     * 
     * @param theFilePath
     *        the file path */
    @Inject
    void filePath(@Named("filePath") final String theFilePath) {
        this.filePath = theFilePath;
    }

    /** Read fill cache.
     * 
     * @param theReadFillCache
     *        the read fill cache flag */
    @Inject
    void readFillCache(@Named("readFillCache") final boolean theReadFillCache) {
        this.readFillCache = theReadFillCache;
    }

    /** Write buffer size.
     * 
     * @param theWriteBufferSize
     *        the write buffer size */
    @Inject
    void writeBufferSize(@Named("writeBufferSize") final int theWriteBufferSize) {
        this.writeBufferSize = theWriteBufferSize;
    }

    /** Write synchronously.
     * 
     * @param theWriteSynchronously
     *        the write synchronously flag */
    @Inject
    void writeSynchronously(
            @Named("writeSynchronously") final boolean theWriteSynchronously) {
        this.writeSynchronously = theWriteSynchronously;
    }

    /** Cache size.
     * 
     * @return the cache size */
    public long cacheSize() {
        return cacheSize;
    }

    /** Compression on.
     * 
     * @return is compression on. */
    public boolean compressionOn() {
        return compressionOn;
    }

    /** File path.
     * 
     * @return the data File path */
    public String filePath() {
        return filePath;
    }

    /** Read fill cache.
     * 
     * @return is read fill cache on */
    public boolean readFillCache() {
        return readFillCache;
    }

    /** Write buffer size.
     * 
     * @return the write buffer size */
    public int writeBufferSize() {
        return writeBufferSize;
    }

    /** Write synchronously.
     * 
     * @return is write synchronous on */
    public boolean writeSynchronously() {
        return writeSynchronously;
    }

}
