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
package com.blockwithme.longdb.voltdb;

import javax.annotation.ParametersAreNonnullByDefault;

/** The VoltDB Constants. */
@ParametersAreNonnullByDefault
public interface VoltDBConstants {

    /** The iterator limit used by row iterator for buffering certain number of
     * row Ids. */
    int ITERATOR_LIMIT = 500;

    /** The project name used by externally to load backend service. */
    String PROJECT_NAME = "VoltDBImpl";
}
