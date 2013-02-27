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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

/** This class makes sure that test methods are run in a particular order
 * (alphabetical order). */
public class OrderedRunner extends BlockJUnit4ClassRunner {

    /** @param theKlass
     *        the class used by junit.
     * @throws InitializationError
     *         the initialization error */
    @SuppressWarnings("all")
    public OrderedRunner(final Class theKlass) throws InitializationError {
        super(theKlass);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.junit.runners.BlockJUnit4ClassRunner#computeTestMethods()
     */
    @SuppressWarnings("all")
    @Override
    protected List computeTestMethods() {
        final List list = super.computeTestMethods();
        final List copyList = new ArrayList(list);
        Collections.sort(copyList, new Comparator() {
            public int compare(final Object theFristObj,
                    final Object theSecondObj) {
                final FrameworkMethod m1 = (FrameworkMethod) theFristObj;
                final FrameworkMethod m2 = (FrameworkMethod) theSecondObj;
                return m1.getName().compareTo(m2.getName());
            }
        });
        return copyList;
    }
}
