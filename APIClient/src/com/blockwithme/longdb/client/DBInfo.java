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
package com.blockwithme.longdb.client;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import edu.umd.cs.findbugs.annotations.CheckForNull;

/** Encapsulates Database settings information. */
@ParametersAreNonnullByDefault
public class DBInfo {

    /** The be type. */
    private final String beType;

    /** The db name. */
    @CheckForNull
    private final String dbName;

    /** The instance name. */
    private final String instanceName;

    /** @param theBEType
     *        the backend type, i.e. 'CassandraImpl', 'VoltDBImpl' etc.
     * @param theInstanceName
     *        the name of selected backend instance (as per the type
     *        specific configuration file)
     * @param theDBName
     *        the selected database name, this can be null if the database
     *        to be selected is not created yet. */
    public DBInfo(final String theBEType, final String theInstanceName,
            @Nullable final String theDBName) {
        this.beType = checkNotNull(theBEType, "beType");
        this.instanceName = checkNotNull(theInstanceName, "instanceName");
        this.dbName = theDBName;
    }

    /** @return the Backend type */
    public String backendType() {
        return beType;
    }

    /** @return the database name (can be null) */
    @CheckForNull
    public String dbName() {
        return dbName;
    }

    /** @return the database instance name */
    public String instanceName() {
        return instanceName;
    }
}
