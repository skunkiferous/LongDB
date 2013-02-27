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
package com.blockwithme.longdb.util;

import java.io.File;

import javax.annotation.ParametersAreNonnullByDefault;

/** Configuration management utility methods used by sub modules. */
// CHECKSTYLE IGNORE FOR NEXT 1 LINES
@ParametersAreNonnullByDefault
public class ConfigUtil {

    /** Constant used to represent Root directory System/Environment property. */
    public static final String PB_ROOT = "PB_ROOT";

    /** Utility method used by sub modules to load get root folder path.
     * 
     * @return the folder path string */
    public static String rootFolderPath() {
        String rootFolder = System.getenv(PB_ROOT);
        if (rootFolder == null)
            rootFolder = System.getProperty(PB_ROOT);

        if (rootFolder == null)
            throw new IllegalStateException(
                    "System Property/Environment Variable '" + PB_ROOT
                            + "' could not be found.");
        final File file = new File(rootFolder);
        if (!file.exists())
            throw new IllegalStateException(
                    "Project root as specified in System Property/Environment Variable '"
                            + PB_ROOT + "' (" + rootFolder
                            + ") could not be found.");
        if (!file.isDirectory())
            throw new IllegalStateException(
                    "Project root as specified in System Property/Environment Variable '"
                            + PB_ROOT + "' (" + rootFolder
                            + ") is not a directory.");
        if (!file.canRead())
            throw new IllegalStateException(
                    "Project root as specified in System Property/Environment Variable '"
                            + PB_ROOT + "' (" + rootFolder
                            + ") cannot be read.");
        if (!file.canWrite())
            throw new IllegalStateException(
                    "Project root as specified in System Property/Environment Variable '"
                            + PB_ROOT + "' (" + rootFolder
                            + ") cannot be written.");
        if (!rootFolder.endsWith(File.separator))
            rootFolder += File.separator;
        return rootFolder;
    }

}
