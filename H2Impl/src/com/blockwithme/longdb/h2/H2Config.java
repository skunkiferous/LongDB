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
package com.blockwithme.longdb.h2;

import javax.annotation.ParametersAreNonnullByDefault;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/** The H2 database configuration bean */
@ParametersAreNonnullByDefault
public class H2Config {

    /** The db file. */
    private String dbFile;

    /** Db file.
     * 
     * @param theDBFile
     *        the db file */
    @Inject
    void dbFile(@Named("dbFile") final String theDBFile) {
        this.dbFile = theDBFile;
    }

    /** DB file name.
     * 
     * @return the DB file name */
    public String dbFile() {
        return dbFile;
    }

}
