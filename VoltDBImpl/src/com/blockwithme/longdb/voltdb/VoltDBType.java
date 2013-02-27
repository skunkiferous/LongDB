/*******************************************************************************
 * Copyright (c) 2013 Sebastien Diot..
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0
 * which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/gpl.html
 * 
 * Contributors:
 *     Sebastien Diot. - initial API and implementation
 ******************************************************************************/
package com.blockwithme.longdb.voltdb;

import javax.annotation.ParametersAreNonnullByDefault;

import com.blockwithme.longdb.Backend;
import com.blockwithme.longdb.base.AbstractBackendType;

// TODO: Auto-generated Javadoc
/** The Class Volt DB Type definition */
@ParametersAreNonnullByDefault
public class VoltDBType extends AbstractBackendType {

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractBackendType#backendClass()
     */
    @Override
    protected Class<? extends Backend> backendClass() {
        return VoltDBBackend.class;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractBackendType#configBeanClass()
     */
    @Override
    protected Class<? extends Object> configBeanClass() {
        return VoltDBConfig.class;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.base.AbstractBackendType#configFileName()
     */
    @Override
    protected String configFileName() {
        return "VoltConfig.properties";
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.blockwithme.longdb.discovery.BackendType#name()
     */
    @Override
    public String name() {
        return VoltDBConstants.PROJECT_NAME;
    }
}
