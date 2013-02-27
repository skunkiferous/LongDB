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
package com.blockwithme.longdb.client.entities;

import javax.annotation.ParametersAreNonnullByDefault;

import com.blockwithme.longdb.Columns;

/** A wrapper class that represents a Row Object, Contains rowID and 'Columns'
 * Object - having multiple column Ids and Values. */
@ParametersAreNonnullByDefault
public class Row {

    /** The columns. */
    private Columns columns;

    /** The row id. */
    private long rowId;

    /** @param theRowId
     *        the row id
     * @param theColumns
     *        the columns */
    public Row(final long theRowId, final Columns theColumns) {
        this.rowId = theRowId;
        this.columns = theColumns;
    }

    /** Returns all Columns in the row. */
    public Columns columns() {
        return columns;
    }

    /** Sets columns of the row. */
    public Row columns(final Columns theColumns) {
        this.columns = theColumns;
        return this;
    }

    /** Gets the RowId. */
    public long rowId() {
        return rowId;
    }

    /** Sets the RowId. */
    public Row rowId(final long theRowId) {
        this.rowId = theRowId;
        return this;
    }
}
