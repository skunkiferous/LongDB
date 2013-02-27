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
/**
 *
 */
package com.paintedboxes.newdb;

/** Interface implemented by the receiver of a query result set for a single
 * family.
 * 
 * @author monster
 * @param <Q>
 *        The type of the query key, used to create the query.
 * @param <ROW>
 *        The type of the row key container
 * @param <KEY>
 *        The type of the column key container
 * @param <VALUE>
 *        The type of the value container */
public interface FamilyQueryReceiver<Q, ROW extends RowKey<?>, KEY extends ColumnKey<?>, VALUE extends Value<?>> {
    /** Called when the result arrives.
     * 
     * @param queryKey
     *        The key used to create the query.
     * @param result
     *        The result set. */
    void onQueryResult(final Q queryKey,
            final FamilyResultSet<ROW, KEY, VALUE> result);

    /** Called when the query failed.
     * 
     * @param queryKey
     *        The key used to create the query.
     * @param failure
     *        The reason for the failure. */
    void onQueryFailure(final Q queryKey, final Throwable failure);
}
