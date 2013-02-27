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
// $codepro.audit.disable declareAsInterface, useBufferedIO
package com.blockwithme.longdb.base;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blockwithme.longdb.Backend;
import com.blockwithme.longdb.discovery.BackendInstance;
import com.blockwithme.longdb.discovery.BackendType;
import com.blockwithme.longdb.util.ConfigUtil;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.name.Names;

/** The Class AbstractBackendType provides partial implementation of BackendType
 * interface. */
@ParametersAreNonnullByDefault
public abstract class AbstractBackendType implements BackendType {

    /** The Logger. */
    private static final Logger LOG = LoggerFactory
            .getLogger(AbstractBackendType.class);

    /** Gets the single instance of AbstractBackendType.
     * 
     * @param theInstanceName
     *        the instance name
     * @return single instance of AbstractBackendType */
    private BackendInstance getInstance(final String theInstanceName) {

        final Properties iProps = (Properties) initConfig().clone();
        final String filePath = iProps.getProperty(theInstanceName + "."
                + "filePath");
        final String description = initConfig().getProperty("description")
                + " instance name - " + theInstanceName + " file path - "
                + filePath;
        updateProps(iProps, theInstanceName);
        return new BackendInstance() {

            @Override
            public void configure(final Binder theBinder) {
                Names.bindProperties(theBinder, iProps);
                theBinder.bind(configBeanClass());
                theBinder.bind(Backend.class).to(backendClass());
            }

            @Override
            public String description() {
                return description;
            }

            @Override
            public String name() {
                return theInstanceName;
            }
        };
    }

    /** Update props.
     * 
     * @param theProps
     *        the i props
     * @param theInstanceName
     *        the instance name */
    private void updateProps(final Properties theProps,
            final String theInstanceName) {
        final String prefix = theInstanceName + ".";
        for (final String pName : theProps.stringPropertyNames()) {
            if (pName.startsWith(prefix)) {
                final String newName = pName.substring(prefix.length());
                theProps.setProperty(newName, theProps.getProperty(pName));
            }
        }
    }

    /** Backend class.
     * 
     * @return The concrete Backend class to be injected for this Backend type. */
    protected abstract Class<? extends Backend> backendClass();

    /** Config bean class.
     * 
     * @return the class to be injected containing configuration values for this
     *         backend type */
    protected abstract Class<? extends Object> configBeanClass();

    /** Configuration file name.
     * 
     * @return name of the configuration file (file location is resolved as
     *         $PB_ROOT/config/) */
    protected abstract String configFileName();

    /** Loads configuration data from Configuration file and returns them in form
     * of a properties object.
     * 
     * @return the configuration properties */
    protected Properties initConfig() {
        Reader frdr = null;
        try {
            final String confPath = ConfigUtil.rootFolderPath()
                    + File.separator + "conf" + File.separator
                    + configFileName();
            final Properties properties = new Properties();
            frdr = new InputStreamReader(new FileInputStream(confPath),
                    Charsets.UTF_8.name());
            properties.load(frdr);
            return properties;
        } catch (final Exception e) {
            LOG.error("Error While initializing configuration", e);
            throw new IllegalStateException(
                    "Error While initializing configuration", e);
        } finally {
            if (frdr != null) { // $codepro.audit.disable unnecessaryNullCheck
                try {
                    frdr.close();
                } catch (final IOException ioe) {
                    // ignore
                }
            }
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.discovery.BackendType#description()
     */
    @Override
    public String description() {
        try {
            return initConfig().getProperty("description");
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.discovery.BackendType#instances()
     */
    @Override
    public List<BackendInstance> instances() {

        try {
            final String instances = initConfig().getProperty("instances");
            final StringTokenizer tknzr = new StringTokenizer(instances, ",",
                    false);
            final ImmutableList.Builder<BackendInstance> builder = new ImmutableList.Builder<BackendInstance>();
            while (tknzr.hasMoreTokens()) {
                builder.add(getInstance(tknzr.nextToken()));
            }
            return builder.build();
        } catch (final Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

}
