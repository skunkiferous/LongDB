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
// $codepro.audit.disable declareAsInterface
package com.blockwithme.longdb;

import com.blockwithme.longdb.BEDatabase;
import com.blockwithme.longdb.BETable;
import com.blockwithme.longdb.entities.Base36;

/** A database, on top of a Backend database. */
public class Database {

    /** The "meta" table ID */
    private static final Base36 META = Base36.get("meta");

    /** The "root" row ID */
    private static final Base36 ROOT = Base36.get("root");

    /** The database */
    private final BEDatabase database; // NOPMD

    /** The meta-info table. */
    private final BETable meta; // NOPMD

    /** Returns the root of the hierarchy. */
    private final Map<Name, Name[], Reference, Reference[]> rootMap;

    /** Constructor.
     * 
     * @param theDatabase
     *        the database instance */
    public Database(final BEDatabase theDatabase) {
        if (theDatabase == null) {
            throw new NullPointerException("database");
        }
        this.database = theDatabase;
        meta = this.database.get(META);
        assert (meta != null);
        rootMap = new Map<Name, Name[], Reference, Reference[]>(
                Name.datatype(), Reference.datatype(), meta.get(ROOT.value()));
    }

    /** Returns the root of the hierarchy.
     * 
     * @return the map */
    public Map<Name, Name[], Reference, Reference[]> root() {
        return rootMap;
    }
}
