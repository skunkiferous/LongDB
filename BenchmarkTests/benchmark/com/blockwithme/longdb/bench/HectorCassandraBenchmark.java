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
package com.blockwithme.longdb.bench;

import org.junit.BeforeClass;

import com.blockwithme.longdb.bench.util.ConfigBean;
import com.blockwithme.longdb.cassandra.CassandraConstants;
import com.blockwithme.longdb.discovery.BackendServiceLoader;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.google.inject.Guice;
import com.google.inject.Injector;

/** The Benchmark Test suite for Cassandra (Server mode) */
@AxisRange(min = 0, max = 1)
@BenchmarkMethodChart(filePrefix = "HectorCassandraBenchMarks")
public class HectorCassandraBenchmark extends AbstractBenchmarkTests {

    /** @throws Exception */
    @BeforeClass
    public static void config() throws Exception {
        final ConfigBean.ConfigModule module = new ConfigBean.ConfigModule(
                BackendServiceLoader.getInstance().loadInstance(
                        CassandraConstants.PROJECT_NAME, "default"));
        final Injector injector = Guice.createInjector(module);
        final ConfigBean bean = injector.getInstance(ConfigBean.class);
        doInit(bean);
    }

}
