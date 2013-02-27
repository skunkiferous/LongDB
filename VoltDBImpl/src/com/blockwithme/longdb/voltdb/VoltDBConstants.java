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

/** The VoltDB Constants. */
@ParametersAreNonnullByDefault
public interface VoltDBConstants {

    /** The iterator limit used by row iterator for buffering certain number of
     * row Ids. */
    int ITERATOR_LIMIT = 500;

    /** The project name used by externally to load backend service. */
    String PROJECT_NAME = "VoltDBImpl";
}
