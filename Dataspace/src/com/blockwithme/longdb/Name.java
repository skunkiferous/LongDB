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

import com.blockwithme.longdb.util.LimitedBase36;

/** A name for one of the supported object types. */
public final class Name extends LimitedBase36 {

    /** serialVersionUID */
    private static final long serialVersionUID = -8377475341661967414L;

    /** Returns the datatype for names */
    private static Datatype<Name, Name[]> DATA_TYPE;

    /** Returns the datatype for names. */
    public static Datatype<Name, Name[]> datatype() {
        if (DATA_TYPE == null) {

            DATA_TYPE = Datatype.get(Name.class);
        }
        return DATA_TYPE;
    }

    /** Converts Strings to Names. */
    public static Name[] toNames(final String... theNames) {
        final Name[] resultArray = new Name[theNames.length];
        for (int i = 0; i < resultArray.length; i++) {
            resultArray[i] = new Name(theNames[i]);
        }
        return resultArray;
    }

    /** Constructs a Name from the internal representation. */
    public Name(final long theInternal) {
        super(theInternal);
    }

    /** Constructs a Name from the case-insensitive string representation.
     * 
     * @param theText
     *        the text */
    public Name(final String theText) {
        super(theText);
    }

    /** The encoded name of the referenced object. */
    public long encoded() {
        return value;
    }
}
