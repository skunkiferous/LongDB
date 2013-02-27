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

/** A reference to one of the supported entity types. */
public final class Reference {
    /** The datatype for references */
    private static Datatype<Reference, Reference[]> DATA_TYPE;

    /** The ID of the referenced object. */
    private final long id;

    /** Returns the datatype for references.
     * 
     * @return the datatype */
    public static Datatype<Reference, Reference[]> datatype() {
        if (DATA_TYPE == null) {
            DATA_TYPE = Datatype.get(Reference.class);
        }
        return DATA_TYPE;
    }

    /** Constructor.
     * 
     * @param theId
     *        the identifier */
    public Reference(final long theId) {
        this.id = theId;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object theObject) {
        if (this == theObject)
            return true;
        if (theObject == null)
            return false;
        if (!(theObject instanceof Reference))
            return false;
        final Reference other = (Reference) theObject;
        if (id != other.id)
            return false;
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return (int) (id ^ (id >>> 32));
    }

    /** The ID of the referenced object.
     * 
     * @return the identifier */
    public long id() { // NOPMD
        return id;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Reference(id=" + id + ")";
    }

    /** Returns the entity datatype.
     * 
     * @return the entity type */
    public EntityType type() {
        return EntityType.find((int) (id % EntityType.COUNT));
    }
}
