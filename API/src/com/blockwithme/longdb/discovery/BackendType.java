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
package com.blockwithme.longdb.discovery;

import java.util.List;

import javax.annotation.ParametersAreNonnullByDefault;

/** The interface BackendType Represents a type of backend Datastore API
 * implementation i.e. CassandraBackend, BDBBackend etc. Each Backend type can
 * have more than one {@link BackendInstances} */
@ParametersAreNonnullByDefault
public interface BackendType {

    /** @return Description of the BackendType */
    String description();

    /** @return the list of {@link BackendInstance} available for this
     *         BackendType */
    List<BackendInstance> instances();

    /** @return Name of the BackendType */
    String name();

}
