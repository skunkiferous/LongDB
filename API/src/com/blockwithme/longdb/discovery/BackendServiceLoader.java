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

import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.Set;

import javax.annotation.ParametersAreNonnullByDefault;

import com.blockwithme.longdb.Backend;
import com.google.common.base.Preconditions;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.multibindings.Multibinder;

/** This class provides a mechanism for the discovery of the different Backend
 * client instances available in the JVM currently. This discovery is based on
 * ServiceLoader facility, which searches the available {@link BackendType} in
 * the java Classpath. The {@link BackendType} in turn provides the available
 * {@link BackendInstance}. A {@link Backend} can be loaded using
 * {@link BackendServiceLoader#loadInstance(String, String)} method */
@ParametersAreNonnullByDefault
public final class BackendServiceLoader {

    /** Inner class to wrap the singleton instance of BackendServiceLoader */
    private static class BackendServiceInstanceLoader { // $codepro.audit.disable
        /** The Constant INSTANCE. */
        private static final BackendServiceLoader INSTANCE = new BackendServiceLoader(); // NOPMD
    }

    /** Inner class that wraps a collection of BackendType. The available backend
     * types are injected to this class */
    private static class BackendTypes {

        /** The collection of Backend types. */
        private final Set<BackendType> beTypes;

        /** Instantiates a new backend types.
         * 
         * @param theModules
         *        the modules */
        @Inject
        BackendTypes(final Set<BackendType> theModules) {
            beTypes = theModules;
        }

        /** The collection of Backend types */
        Set<BackendType> types() {
            return beTypes;
        }
    }

    /** Module class for injecting BackendType collection into BackendTypes
     * class. */
    private static class GuiceMultibinderModule extends AbstractModule {

        /*
         * (non-Javadoc)
         * 
         * @see com.google.inject.AbstractModule#configure()
         */
        @Override
        protected void configure() {
            final Multibinder<BackendType> binder = Multibinder.newSetBinder(
                    binder(), BackendType.class);
            final ServiceLoader<BackendType> classpathInterfaces = ServiceLoader
                    .load(BackendType.class);
            for (final BackendType inf : classpathInterfaces) {
                binder.addBinding().toInstance(inf);
            }
        }
    }

    /** The available services */
    private final BackendTypes services;

    /** Gets the single instance of BackendServiceLoader.
     * 
     * @return single instance of BackendServiceLoader */
    public static BackendServiceLoader getInstance() {
        return BackendServiceInstanceLoader.INSTANCE;
    }

    /** Private constructor, gets invoked by BackendServiceInstanceLoader through
     * static initialization */
    private BackendServiceLoader() {
        if (BackendServiceInstanceLoader.INSTANCE != null) {
            throw new IllegalStateException("Already instantiated");
        }
        final Injector inject = Guice
                .createInjector(new GuiceMultibinderModule());
        services = inject.getInstance(BackendTypes.class);
    }

    /** Discovers the available instances of {@link BackendType}. This discovery
     * is based on ServiceLoader facility, which searches the available
     * 
     * @return the iterator {@link BackendType} in the java Classpath. */
    public Iterator<BackendType> availableBackends() {
        final BackendTypes impls = getInstance().services;
        return impls.types().iterator();
    }

/**
     * Loads Backend instance based on information retrieved from.
     *
     * @param theType the backend type - [{@link BackendType#name()}]
     * @param theInstanceName the instance name - [{@link BackendInstance#name()]
     * @return the backend instance
     * {@link BackendServiceLoader#availableBackends()} method.
     */
    public Backend loadInstance(final String theType,
            final String theInstanceName) {
        Preconditions.checkNotNull(theType, "type is null");
        Preconditions.checkNotNull(theInstanceName, "instanceName is null");
        final BackendTypes servs = services;
        for (final BackendType serv : servs.types()) {
            if (serv.name().equals(theType)) {
                for (final BackendInstance instance : serv.instances()) {
                    if (instance.name().equals(theInstanceName)) {
                        final Injector inject = Guice.createInjector(instance);
                        return inject.getInstance(Backend.class);
                    }
                }
            }
        }
        throw new IllegalStateException("No Backend implementations found");
    }
}
