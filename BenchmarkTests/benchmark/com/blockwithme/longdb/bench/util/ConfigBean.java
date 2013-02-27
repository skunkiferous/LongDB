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
package com.blockwithme.longdb.bench.util;

import java.io.File;

import com.blockwithme.longdb.Backend;
import com.blockwithme.longdb.bench.constants.BenchmarkTestingConstants;
import com.blockwithme.longdb.util.ConfigUtil;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

/** Configuration Bean - Supplies Config parameters to Benchmark Junit class. */
public class ConfigBean {

    /** The Class ConfigModule. */
    public static class ConfigModule extends AbstractModule {

        /** The backend. */
        private final Backend backend;

        /** Instantiates a new config module.
         * 
         * @param theBackend
         *        the backend */
        public ConfigModule(final Backend theBackend) {
            this.backend = theBackend;
        }

        /*
         * (non-Javadoc)
         * 
         * @see com.google.inject.AbstractModule#configure()
         */
        @Override
        protected void configure() {
            final String confPath = ConfigUtil.rootFolderPath()
                    + BenchmarkTestingConstants.PROJECT_NAME + File.separator
                    + "conf" + File.separator + "log4j-benchmarks.properties";
            bind(String.class).annotatedWith(Names.named("log.config"))
                    .toInstance(confPath);
            bind(String.class).annotatedWith(Names.named("jub.consumers"))
                    .toInstance("CONSOLE,H2");
            bind(String.class).annotatedWith(Names.named("jub.db.file"))
                    .toInstance("benchmark_results/db");
            bind(Backend.class).toInstance(backend);
        }
    }

    /** The backend. */
    @Inject
    private Backend backend;

    /** The Junit Benchmarks consumers. */
    private String jubConsumers;

    /** The Junit Benchmarks db file. */
    private String jubDbFile;

    /** The log config. */
    private String logConfig;

    /** @return the backend */
    public Backend backend() {
        return backend;
    }

    /** @param theBackend
     *        the backend to set */
    public void backend(final Backend theBackend) {
        this.backend = theBackend;
    }

    /** @return the Junit Benchmarks Consumers */
    public String jubConsumers() {
        return jubConsumers;
    }

    /** @param theJubConsumers
     *        the Junit Benchmarks Consumers to set */
    @Inject
    @Named("jub.consumers")
    public void jubConsumers(final String theJubConsumers) {
        this.jubConsumers = theJubConsumers;
    }

    /** @return the jubDbFile */
    public String jubDbFile() {
        return jubDbFile;
    }

    /** @param theJubDbFile
     *        the Junit Benchmarks Db File to set */
    @Inject
    @Named("jub.db.file")
    public void jubDbFile(final String theJubDbFile) {
        this.jubDbFile = theJubDbFile;
    }

    /** @return the logConfig */
    public String logConfig() {
        return logConfig;
    }

    /** @param theLogConfig
     *        the logConfig to set */
    @Inject
    @Named("log.config")
    public void logConfig(final String theLogConfig) {
        this.logConfig = theLogConfig;
    }

}
