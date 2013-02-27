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
// $codepro.audit.disable useBufferedIO
package com.blockwithme.longdb.test.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;

import com.blockwithme.longdb.util.ConfigUtil;

/** The Utility class. */
public class Util {

    /** The log config done. */
    private static boolean LOG_CONFIG_DONE;

    /** Log4j configuration initialization. */
    public static void setLogConfig() {
        if (!LOG_CONFIG_DONE) {
            final Properties props = new Properties();
            FileInputStream fis = null;
            try {
                final String confPath = ConfigUtil.rootFolderPath() + "conf"
                        + File.separator + "log4j-test.properties";
                fis = new FileInputStream(confPath);
                props.load(fis);
                PropertyConfigurator.configure(props);
            } catch (final IOException e) {
                e.printStackTrace();
            } finally {
                if (fis != null) {
                    try {
                        fis.close();
                    } catch (final IOException e) {
                        // ignore.
                    }
                }
            }
            LOG_CONFIG_DONE = true;
        }
    }
}
