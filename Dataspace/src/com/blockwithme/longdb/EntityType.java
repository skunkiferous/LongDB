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
package com.blockwithme.longdb;

/** The different supported entity types. */
public enum EntityType {

    /** The datatype. */
    DATATYPE(0),
    /** The dataspace. */
    DATASPACE(1),
    /** The map. */
    MAP(2),
    /** The dataset. */
    DATASET(3);

    /** The number of types. */
    static final int COUNT = 4;

    /** The type ID */
    private final transient int id;

    /** Finds the type with the given ID.
     * 
     * @param theId
     *        the ID
     * @return the entity type */
    static EntityType find(final int theId) {
        for (final EntityType type : values()) {
            if (type.id == theId) {
                return type;
            }
        }
        return null;
    }

    /** Internal constructor */
    private EntityType(final int theId) {
        this.id = theId;
    }

    /** The type ID.
     * 
     * @return the ID */
    public int id() { // NOPMD
        return id;
    }
}
