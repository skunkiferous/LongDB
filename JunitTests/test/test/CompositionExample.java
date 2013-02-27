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
// $codepro.audit.disable
// CHECKSTYLE IGNORE FOR NEXT 1000 LINES
/* This program is free software. It comes without any warranty, to
 * the extent permitted by applicable law. You can redistribute it
 * and/or modify it under the terms of the Do What The Fuck You Want
 * To Public License, Version 2, as published by Sam Hocevar. See
 * http://sam.zoy.org/wtfpl/COPYING for more details. */

package test; // NOPMD

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;

/** This is an example of how to simulate manually virtual methods in Java, when
 * using composition instead of inheritance. Additionally, it allows the
 * entities to change their composition and behavior at runtime. I have created
 * it to answer my own question in StackOverflow. It shows the following
 * characteristics: 1) The memory overhead per instance is fixed. It does not
 * depend on the number of methods. It is one object (ThingImpl), plus two
 * object references (AnyObjectImpl.thing and ThingImpl.vdt). 2) The call
 * overhead is also fixed. It is one method call and one field access for
 * properties, and one method call and 3 field access for logic. The method is
 * simple and private so you can expect the JIT to inline it. 3) The
 * "class hierarchy" can be composed *at runtime*. 4) The memory overhead of one
 * object references exist for every interface that the objects optionally
 * implement. It is always there, even it the interface is not implemented. An
 * alternative design might use a map instead. This would reduce the memory
 * usage if you have many optional interfaces, but visibly increasing the call
 * overhead. */
public final class CompositionExample {

    /** Base class for the God-Type. */
    @SuppressWarnings("rawtypes")
    abstract static class AnyObjectBase implements Handle {
        /** The implementations. */
        private Impl[] impls;

        /** Instantiates a new any object base.
         * 
         * @param impls
         *        the impls */
        public AnyObjectBase(final Impl[] impls) {
            this.impls = impls;
        }

        /** Find impl.
         * 
         * @param type
         *        the type
         * @return the impl */
        protected Impl findImpl(final Class type) {
            for (final Impl impl : impls) {
                if (type == impl.getImplType()) {
                    return impl;
                }
            }
            return null;
        }

        /*
         * (non-Javadoc)
         * 
         * @see test.CompositionExample.Handle#as(java.lang.Class)
         */
        @SuppressWarnings("unchecked")
        @Override
        public Object as(final Class type) { // NOPMD
            for (final Impl impl : impls) {
                if (type.isAssignableFrom(impl.getImplType())) {
                    return this;
                }
            }
            return null;
        }

        /*
         * (non-Javadoc)
         * 
         * @see test.CompositionExample.Handle#getBaseType()
         */
        @Override
        public Class getBaseType() {
            for (final Impl impl : impls) {
                if (impl.getImplType().getAnnotation(BaseType.class) != null) {
                    return impl.getImplType();
                }
            }
            return null;
        }

        /*
         * (non-Javadoc)
         * 
         * @see test.CompositionExample.Handle#getImplementedTypes()
         */
        @Override
        public Set<Class<?>> getImplementedTypes() {
            final Set<Class<?>> result = new HashSet<Class<?>>();
            for (final Impl impl : impls) {
                result.add(impl.getImplType());
            }
            return result;
        }

        /*
         * (non-Javadoc)
         * 
         * @see test.CompositionExample.Handle#hasRigidType()
         */
        @Override
        public boolean hasRigidType() {
            return false;
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * test.CompositionExample.Handle#implement(test.CompositionExample.
         * Builder)
         */
        @SuppressWarnings("unchecked")
        @Override
        public boolean implement(final Builder builder) {
            final Class<?> type = builder.getBuildType();
            if (isa(type)) {
                // Already implemented
                return false;
            }
            final Class baseType = getBaseType();
            final boolean isBaseType = (type.getAnnotation(BaseType.class) != null);
            if (baseType != null) {
                if (isBaseType) {
                    // There can be only one base type
                    return false;
                }
                if (!baseType.isAssignableFrom(type)) {
                    // All types must extend the base type
                    return false;
                }
            } else if (!isBaseType) {
                // The first type must be a base type
                return false;
            }
            impls = (Impl[]) ArrayUtils.add(impls, builder.create());
            return true;
        }

        /*
         * (non-Javadoc)
         * 
         * @see test.CompositionExample.Handle#isa(java.lang.Class)
         */
        @SuppressWarnings("unchecked")
        @Override
        public boolean isa(final Class type) {
            for (final Impl impl : impls) {
                if (type.isAssignableFrom(impl.getImplType())) {
                    return true;
                }
            }
            return false;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {

            final StringBuilder result = new StringBuilder(super.toString()
                    + " [");
            for (final Impl impl : impls) {
                result.append(impl + ",");
            }
            result.append(']');
            return result.toString();
        }

        /*
         * (non-Javadoc)
         * 
         * @see test.CompositionExample.Handle#unimplement(java.lang.Class)
         */
        @SuppressWarnings("unchecked")
        @Override
        public boolean unimplement(final Class type) {
            for (int i = 0; i < impls.length; i++) {
                final Impl impl = impls[i];
                if (type.isAssignableFrom(impl.getImplType())) {
                    impls = (Impl[]) ArrayUtils.remove(impls, i);
                    return true;
                }
            }
            return false;
        }
    }

    // ///////////////////////////////////////////
    // API Public Interfaces
    // ///////////////////////////////////////////

    /** The God Type, literally: implements every domain object interface. One of
     * them is Thing. An alternative to bytecode generation would be to create a
     * dynamic proxy. */
    static class AnyObjectImpl extends AnyObjectBase implements ThingProtected {

        /** Instantiates a new any object impl.
         * 
         * @param impls
         *        the impls */
        public AnyObjectImpl(final Impl... impls) {
            super(impls);
        }

        /** Returns the optional Thing implementation. Throws an Exception if not
         * available. */
        private ThingImpl thing() {
            final ThingImpl thing = (ThingImpl) findImpl(Thing.class);
            if (thing == null)
                throw new ClassCastException("Thing not currently implemented");
            return thing;
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * test.CompositionExample.ThingProtectedLogic#computeNextUpdateTime()
         */
        @Override
        public Date computeNextUpdateTime() {
            return thing().vdt.computeNextUpdateTime
                    .computeNextUpdateTime(this);
        }

        /*
         * (non-Javadoc)
         * 
         * @see test.CompositionExample.ThingStaticData#daysBeforeUpdate()
         */
        @Override
        public double daysBeforeUpdate() {
            return thing().vdt.statics.daysBeforeUpdate;
        }

        /*
         * (non-Javadoc)
         * 
         * @see test.CompositionExample.ThingLogic#doA()
         */
        @Override
        public String doA() {
            return thing().vdt.doA.doA(this);
        }

        /*
         * (non-Javadoc)
         * 
         * @see test.CompositionExample.ThingLogic#doB(java.lang.String)
         */
        @Override
        public String doB(final String arg) {
            return thing().vdt.doB.doB(this, arg);
        }

        /*
         * (non-Javadoc)
         * 
         * @see test.CompositionExample.ThingProtectedData#getWhen()
         */
        @Override
        public Date getWhen() {
            return thing().when;
        }

        /*
         * (non-Javadoc)
         * 
         * @see test.CompositionExample.ThingData#getX()
         */
        @Override
        public int getX() {
            return thing().x;
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * test.CompositionExample.ThingProtectedData#setWhen(java.util.Date)
         */
        @Override
        public void setWhen(final Date when) {
            thing().when = when;
        }

        /*
         * (non-Javadoc)
         * 
         * @see test.CompositionExample.ThingData#setX(int)
         */
        @Override
        public void setX(final int x) {
            thing().x = x;
        }
    }

    /** Base class for a domain object Builder.
     * 
     * @param <E>
     *        the element type
     * @param <B>
     *        the generic type */
    abstract static class BaseBuilder<E, B extends Builder<E, B>> implements
            Builder<E, B>, Cloneable {
        /** The base type. */
        private final Class<E> type;

        /** Immutable? */
        private final boolean immutable;

        /** Constructor, taking the base type as parameter.
         * 
         * @param type
         *        the type
         * @param immutable
         *        the immutable */
        protected BaseBuilder(final Class<E> type, final boolean immutable) {
            this.type = type;
            this.immutable = immutable;
        }

        /** Replaces the mutable fields in the copy with copy of those.
         * 
         * @param copy
         *        the copy
         * @return the b */
        protected B replaceMutable(final B copy) {
            return copy;
        }

        /*
         * (non-Javadoc)
         * 
         * @see test.CompositionExample.Builder#copy(boolean)
         */
        @Override
        @SuppressWarnings("unchecked")
        public B copy(final boolean deep) {
            if (isImmutable()) {
                return (B) this;
            }
            try {
                return replaceMutable((B) clone());
            } catch (final CloneNotSupportedException e) {
                // Impossible!
                throw new UndeclaredThrowableException(e);
            }
        }

        /*
         * (non-Javadoc)
         * 
         * @see test.CompositionExample.Builder#getBuildType()
         */
        @Override
        public Class<E> getBuildType() {
            return type;
        }

        /*
         * (non-Javadoc)
         * 
         * @see test.CompositionExample.Builder#isImmutable()
         */
        @Override
        public boolean isImmutable() {
            return immutable;
        }
    }

    /** Annotation designating "base types". A base type interface is the root of
     * a type hierarchy. All types used in the domain must extend a base type,
     * except the base types themselves. */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @interface BaseType {
    }

    /** A domain object builder. Builders are *stateful*. It is assumed, that the
     * concrete builder will have setters allowing the client to setup the
     * parameters to be used by the construction of the object, if any. Since
     * each object might require different parameters at creation time, there is
     * no way of defining a "generic" create method in the base interface of all
     * factories, since factories are normally immutable, which means that it is
     * not possible for the client to pass the required parameters other than
     * through the create method parameters. Since builders are stateful and
     * mutable, the client can set all the desired parameters before calling
     * create. E is the type of object, that this builder creates; all created
     * object will have that type. TODO: This design might be dangerous and
     * error-prone, when it comes to multi-threading. We could make the Builders
     * immutable, which would solve this problem, but create a new one. We would
     * have to replace every setter with a method which returns a new copy of
     * the builder, with one parameter changed. While that would work, it would
     * produce more garbage. I am not sure if it would be important or not.
     * 
     * @param <E>
     *        the element type
     * @param <B>
     *        the generic type */
    interface Builder<E, B extends Builder<E, B>> {

        /** Returns a copy of the current builder, with the same stored
         * parameters. This method must also make a fresh copy of any parameter
         * which is itself a mutable Object if deep is true. Returns this, if
         * isImmutable() is true.
         * 
         * @param deep
         *        the deep
         * @return the b */
        B copy(final boolean deep);

        /** Creates an instance of E, and returns an Handle to it.
         * 
         * @return the handle */
        Handle<E> create();

        /** Returns the type (interface) of created objects.
         * 
         * @return the builds the type */
        Class<E> getBuildType();

        /** Returns true if this Builder is immutable.
         * 
         * @return true, if is immutable */
        boolean isImmutable();
    }

    // ///////////////////////////////////////////
    // Domain Public Interfaces
    // ///////////////////////////////////////////

    /** An handle to a domain object. This handle will remain valid, even if the
     * type of the underlying object changes. Object have to use handles to keep
     * references to each other, as the concrete object that forms the
     * implementation might change over time.
     * 
     * @param <E>
     *        the element type */
    interface Handle<E> {

        /** Either returns the requested interface to the object, if it is
         * currently implemented, or null otherwise.
         * 
         * @param <F>
         *        the generic type
         * @param type
         *        the type
         * @return the f */
        <F extends E> F as(final Class<F> type); // NOPMD

        /** Returns the base type of the object. This cannot change after
         * creation
         * 
         * @return the base type */
        Class<E> getBaseType();

        /** Returns the interfaces *currently implemented* by the object.
         * 
         * @return the implemented types */
        Set<Class<E>> getImplementedTypes();

        /** Returns true, if the implemented types of this object cannot be
         * modified.
         * 
         * @return true, if the implemented types of this object cannot be
         *         modified */
        boolean hasRigidType();

        /** Adds a new implementation to this object, if it is not yet present.
         * Returns true on success, otherwise false if for some reason it is not
         * possible, like for example if some currently implemented type is
         * incompatible with the new type. *All* references to interface
         * implemented by this object must be considered invalid after a
         * successful call to this method. Note that multiple types might be
         * added as a result of this call. Always returns false, if
         * hasRigidType() is true.
         * 
         * @param <F>
         *        the generic type
         * @param builder
         *        the builder
         * @return true, if successful */
        <F extends E> boolean implement(final Builder<F, ?> builder);

        /** Checks if the object currently implements the requested interface.
         * This is equivalent to calling (as(type) != null);
         * 
         * @param <F>
         *        the generic type
         * @param type
         *        the type
         * @return true, if is a */
        <F extends E> boolean isa(final Class<F> type);

        /** Removes an existing implementation in this object, if it is present.
         * Returns true on success, otherwise false. *All* references to
         * interface implemented by this object must be considered invalid after
         * a successful call to this method. Note that multiple types might be
         * removed as a result of this call. Always returns false, if
         * hasRigidType() is true.
         * 
         * @param <F>
         *        the generic type
         * @param type
         *        the type
         * @return true, if successful */
        <F extends E> boolean unimplement(final Class<F> type);
    }

    /** The Interface Impl. */
    interface Impl {

        /** Returns the type implemented by this object.
         * 
         * @return the impl type */
        Class<?> getImplType();

        /** toString must be implemented.
         * 
         * @param obj
         *        the obj
         * @return the string */
        String toString(final Object obj);
    }

    /** All Builders must implement this.
     * 
     * @param <E>
     *        the element type
     * @param <B>
     *        the generic type */
    interface ImplBuilder<E, B extends Builder<E, B>> extends Builder<E, B> {

        /** Returns a new Impl for the type.
         * 
         * @return the impl */
        Impl createImpl();
    }

    /** Provides access to the builders, for the supported types. It might not be
     * required when using AOP ... */
    interface Scope {

        /** Returns a new default Builder for the requested interface. This
         * Builder can be cached. This operation should be thread-safe.
         * 
         * @param <E>
         *        the element type
         * @param baseType
         *        the base type
         * @return the builder */
        <E> Builder<E, ?> builderFor(final Class<E> baseType);

        /** Register some Builders as prototype. They will replace some possibly
         * existing builders with the same type. Copies of this builder will be
         * returned by builderFor(). Returns a new Scope using those builders.
         * 
         * @param builders
         *        the builders
         * @return the scope */
        Scope withBuilders(final Builder<?, ?>... builders);
    }

    /** The Class ScopeImpl. */
    static class ScopeImpl implements Scope {

        /** All Builders. */
        @SuppressWarnings("rawtypes")
        private final Map<Class, Builder> builders;

        /** Default constructor */
        private ScopeImpl(
                @SuppressWarnings("rawtypes") final Map<Class, Builder> builders) {
            this.builders = builders;
        }

        /** Default constructor. */
        @SuppressWarnings("unchecked")
        public ScopeImpl() {
            this(Collections.EMPTY_MAP);
        }

        /*
         * (non-Javadoc)
         * 
         * @see test.CompositionExample.Scope#builderFor(java.lang.Class)
         */
        @SuppressWarnings("unchecked")
        @Override
        public <E> Builder<E, ?> builderFor(final Class<E> baseType) {
            return builders.get(baseType);
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * test.CompositionExample.Scope#withBuilders(test.CompositionExample
         * .Builder<?,?>[])
         */
        @Override
        public Scope withBuilders(final Builder<?, ?>... builders) {
            @SuppressWarnings("rawtypes")
            final Map<Class, Builder> map = new HashMap<Class, Builder>(
                    this.builders);
            for (final Builder<?, ?> b : builders) {
                map.put(b.getBuildType(), b);
            }
            return new ScopeImpl(map);
        }
    }

    // ///////////////////////////////////////////
    // API Implementation Interfaces
    // ///////////////////////////////////////////

    /** Object API is split between Data, Logic and Static Data. */
    @BaseType
    interface Thing extends ThingData, ThingLogic, ThingStaticData {
        // Always empty
    }

    /** Base class for any impl of ThingLogicInternal. */
    static class ThingBaseLogic implements ThingLogicInternal {

        /*
         * (non-Javadoc)
         * 
         * @see
         * test.CompositionExample.ThingLogicInternal#computeNextUpdateTime(
         * test.CompositionExample.ThingProtected)
         */
        @Override
        public Date computeNextUpdateTime(final ThingProtected self) {
            throw new UnsupportedOperationException(
                    "computeNextUpdateTime not implemented in " + getClass());
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * test.CompositionExample.ThingLogicInternal#doA(test.CompositionExample
         * .ThingProtected)
         */
        @Override
        public String doA(final ThingProtected self) {
            throw new UnsupportedOperationException("doA not implemented in "
                    + getClass());
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * test.CompositionExample.ThingLogicInternal#doB(test.CompositionExample
         * .ThingProtected, java.lang.String)
         */
        @Override
        public String doB(final ThingProtected self, final String arg) {
            throw new UnsupportedOperationException("doB not implemented in "
                    + getClass());
        }

        /*
         * (non-Javadoc)
         * 
         * @see test.CompositionExample.ThingLogicInternal#toString(test.
         * CompositionExample.ThingProtected)
         */
        @Override
        public String toString(final ThingProtected self) {
            return "Thing(x=" + self.getX() + ", when=" + self.getWhen()
                    + ", daysBeforeUpdate=" + self.daysBeforeUpdate() + ")";
        }
    }

    // ///////////////////////////////////////////
    // API Implementation classes
    // ///////////////////////////////////////////

    /** Builder for creating Things. */
    interface ThingBuilder extends Builder<Thing, ThingBuilder> {

        /** Gets the when.
         * 
         * @return the when */
        Date getWhen();

        /** Gets the x.
         * 
         * @return the x */
        int getX();

        /** Sets the when.
         * 
         * @param when
         *        the new when */
        void setWhen(final Date when);

        /** Sets the x.
         * 
         * @param x
         *        the new x */
        void setX(final int x);
    }

    /** The Class ThingBuilderImpl. */
    static class ThingBuilderImpl extends BaseBuilder<Thing, ThingBuilder>
            implements ThingBuilder, ImplBuilder<Thing, ThingBuilder> {
        /** Default thing logic and constants. */
        private final ThingVDT defaultThingLogic;

        /** The x. */
        private int x;

        /** The when. */
        private Date when;

        /** Instantiates a new thing builder impl. */
        public ThingBuilderImpl() {
            super(Thing.class, false);
            final ThingLogicInternal one = new ThingImplOne();
            final ThingLogicInternal two = new ThingImplTwo(one);
            final ThingStaticDataImpl statics = new ThingStaticDataImpl(42.0);
            defaultThingLogic = new ThingVDT(statics, one, two, two, one);
        }

        /*
         * (non-Javadoc)
         * 
         * @see test.CompositionExample.Builder#create()
         */
        @SuppressWarnings("unchecked")
        @Override
        public Handle<Thing> create() {
            final AnyObjectImpl result = new AnyObjectImpl(createImpl());
            result.implement(this);
            return result;
        }

        /*
         * (non-Javadoc)
         * 
         * @see test.CompositionExample.ImplBuilder#createImpl()
         */
        @Override
        public Impl createImpl() {
            final ThingImpl impl = new ThingImpl();
            impl.vdt = defaultThingLogic;
            impl.when = when;
            impl.x = x;
            return impl;
        }

        /*
         * (non-Javadoc)
         * 
         * @see test.CompositionExample.ThingBuilder#getWhen()
         */
        @Override
        public Date getWhen() {
            return when;
        }

        /*
         * (non-Javadoc)
         * 
         * @see test.CompositionExample.ThingBuilder#getX()
         */
        @Override
        public int getX() {
            return x;
        }

        /*
         * (non-Javadoc)
         * 
         * @see test.CompositionExample.ThingBuilder#setWhen(java.util.Date)
         */
        @Override
        public void setWhen(final Date when) {
            this.when = when;
        }

        /*
         * (non-Javadoc)
         * 
         * @see test.CompositionExample.ThingBuilder#setX(int)
         */
        @Override
        public void setX(final int x) {
            this.x = x;
        }
    }

    /** Object Public Instance Data. */
    interface ThingData {
        // Properties ..
        /** Gets the x.
         * 
         * @return the x */
        int getX();

        /** Sets the x.
         * 
         * @param x
         *        the new x */
        void setX(final int x);
        // ...
    }

    // ///////////////////////////////////////////
    // Domain Implementation interfaces
    // ///////////////////////////////////////////

    /** Object Implementation. */
    static class ThingImpl implements Impl {
        // Logic
        /** The vdt. */
        public ThingVDT vdt;

        // Public Properties ..
        /** The x. */
        public int x;

        // Protected Properties ..
        /** The when. */
        public Date when;

        // ...
        /*
         * (non-Javadoc)
         * 
         * @see test.CompositionExample.Impl#getImplType()
         */
        @Override
        public Class<?> getImplType() {
            return Thing.class;
        }

        /*
         * (non-Javadoc)
         * 
         * @see test.CompositionExample.Impl#toString(java.lang.Object)
         */
        @Override
        public String toString(final Object obj) {
            return vdt.toString.toString((ThingProtected) obj);
        }
    }

    /** Defines doA. */
    static class ThingImplOne extends ThingBaseLogic {

        /** The Constant MS_PER_DAY. */
        private static final long MS_PER_DAY = 24 * 3600 * 1000;

        /*
         * (non-Javadoc)
         * 
         * @see
         * test.CompositionExample.ThingBaseLogic#computeNextUpdateTime(test
         * .CompositionExample.ThingProtected)
         */
        @Override
        public Date computeNextUpdateTime(final ThingProtected self) {
            final long offset = (long) (self.daysBeforeUpdate() * MS_PER_DAY);
            return new Date(System.currentTimeMillis() + offset);
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * test.CompositionExample.ThingBaseLogic#doA(test.CompositionExample
         * .ThingProtected)
         */
        @Override
        public String doA(final ThingProtected self) {
            return String.valueOf(self.getX());
        }
    }

    /** Defines doB and computeNextUpdateTime(). */
    static class ThingImplTwo extends ThingBaseLogic {

        /** The logic "super" class */
        private final ThingLogicInternal _super;

        /** This logic requires a super(parent) logic.
         * 
         * @param _super
         *        the _super */
        public ThingImplTwo(final ThingLogicInternal _super) {
            super();
            this._super = _super;
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * test.CompositionExample.ThingBaseLogic#computeNextUpdateTime(test
         * .CompositionExample.ThingProtected)
         */
        @Override
        public Date computeNextUpdateTime(final ThingProtected self) {
            Date result = self.getWhen();
            if (result == null) {
                result = _super.computeNextUpdateTime(self);
            }
            return result;
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * test.CompositionExample.ThingBaseLogic#doB(test.CompositionExample
         * .ThingProtected, java.lang.String)
         */
        @Override
        public String doB(final ThingProtected self, final String arg) {
            return "Hello " + arg + " doA()=" + self.doA();
        }
    }

    // ///////////////////////////////////////////
    // Domain Implementation classes
    // ///////////////////////////////////////////

    /** Object Public Logic. */
    interface ThingLogic {
        // Public Domain Logic ...
        /** Do a.
         * 
         * @return the string */
        String doA();

        /** Do b.
         * 
         * @param arg
         *        the arg
         * @return the string */
        String doB(final String arg);
        // ...
    }

    /** Internal Object Logic interface. Not seen by the Object clients. Maps
     * every method of ThingLogic and ThingProtectedLogic, but with an added
     * self parameter. This allows the logic to call other methods on self,
     * including other logic methods, and property access. */
    interface ThingLogicInternal {
        // Protected Domain Logic ...
        /** Compute next update time.
         * 
         * @param self
         *        the self
         * @return the date */
        Date computeNextUpdateTime(final ThingProtected self);

        // Domain Logic ...
        /** Do a.
         * 
         * @param self
         *        the self
         * @return the string */
        String doA(final ThingProtected self);

        /** Do b.
         * 
         * @param self
         *        the self
         * @param arg
         *        the arg
         * @return the string */
        String doB(final ThingProtected self, final String arg);

        // Object methods
        /** To string.
         * 
         * @param self
         *        the self
         * @return the string */
        String toString(final ThingProtected self);
        // ...
    }

    /** The whole Thing API, including private data and logic. */
    interface ThingProtected extends Thing, ThingProtectedData,
            ThingProtectedLogic {
        // Always empty
    }

    // ///////////////////////////////////////////
    // *Generated* Domain Implementation interfaces
    // ///////////////////////////////////////////

    /** Object Protected Instance Data. */
    interface ThingProtectedData {
        // Protected Properties ..
        /** Gets the when.
         * 
         * @return the when */
        Date getWhen();

        /** Sets the when.
         * 
         * @param when
         *        the new when */
        void setWhen(final Date when);
        // ...
    }

    // ///////////////////////////////////////////
    // *Generated* Domain Implementation classes
    // ///////////////////////////////////////////

    /** Object Protected Logic. */
    interface ThingProtectedLogic {
        // Protected Domain Logic ...
        /** Compute next update time.
         * 
         * @return the date */
        Date computeNextUpdateTime();
        // ...
    }

    /** Object Static Data. */
    interface ThingStaticData {
        // Static constants ..
        /** Days before update.
         * 
         * @return the double */
        double daysBeforeUpdate();
        // ...
    }

    /** Object Static Data. */
    static class ThingStaticDataImpl {

        // Static constants ..
        /** The days before update. */
        public final double daysBeforeUpdate;

        // ...

        /** Instantiates a new thing static data impl.
         * 
         * @param daysBeforeUpdate
         *        the days before update */
        public ThingStaticDataImpl(final double daysBeforeUpdate) {
            this.daysBeforeUpdate = daysBeforeUpdate;
        }
    }

    /** A virtual dispatch table for the ThingLogic and ThingProtectedLogic. It
     * contains one reference per method, but they are of type
     * ThingLogicInternal, instead of being specific to the method. Many/all
     * references can point to the same object. In addition, it contains the
     * static constants as well, so they are dynamically configurable. */
    static class ThingVDT {

        /** The statics. */
        public final ThingStaticDataImpl statics;

        /** The do a. */
        public final ThingLogicInternal doA;

        /** The do b. */
        public final ThingLogicInternal doB;

        /** The compute next update time. */
        public final ThingLogicInternal computeNextUpdateTime;

        /** The to string. */
        public final ThingLogicInternal toString;

        /** Instantiates a new thing vdt.
         * 
         * @param statics
         *        the statics
         * @param doA
         *        the do a
         * @param doB
         *        the do b
         * @param computeNextUpdateTime
         *        the compute next update time
         * @param toString
         *        the to string */
        ThingVDT(final ThingStaticDataImpl statics,
                final ThingLogicInternal doA, final ThingLogicInternal doB,
                final ThingLogicInternal computeNextUpdateTime,
                final ThingLogicInternal toString) {
            this.statics = statics;
            this.doA = doA;
            this.doB = doB;
            this.computeNextUpdateTime = computeNextUpdateTime;
            this.toString = toString;
        }
    }

    /** The main method.
     * 
     * @param args
     *        the arguments */
    public static void main(final String[] args) {
        final ThingBuilder builder = new ThingBuilderImpl();

        final Handle<Thing> handle = builder.create();

        final Thing thing = handle.as(Thing.class);

        System.out.println("THING: " + thing);

        thing.setX(99);

        System.out.println("doB(test): " + thing.doB("test"));
    }

    /** Instantiates a new composition example. */
    private CompositionExample() {
    }
}
