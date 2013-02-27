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

import java.io.Closeable;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blockwithme.longdb.BEDatabase;
import com.blockwithme.longdb.BETable;
import com.blockwithme.longdb.exception.DBException;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.LongCursor;

/** Class provides some common methods used by the sub modules. */
@ParametersAreNonnullByDefault
public final class DBUtils {

    /** The Constant LOG. */
    private static final Logger LOG = LoggerFactory.getLogger(DBUtils.class);

    /** Close Quietly.
     * 
     * @param theResource
     *        the resource */
    public static void closeQuitely(final BEDatabase theResource) {
        try {
            theResource.close(); // $codepro.audit.disable closeInFinally
        } catch (final Exception e) {
            LOG.error("Failed to close " + theResource, e);
        }
    }

    /** Close Quietly.
     * 
     * @param theResource
     *        the resource */
    public static void closeQuitely(final BETable theResource) {
        try {
            theResource.close(); // $codepro.audit.disable closeInFinally
        } catch (final Exception e) {
            LOG.error("Failed to close " + theResource, e);
        }
    }

    /** Close Quietly.
     * 
     * @param theResource
     *        the resource */
    public static void closeQuitely(@Nullable final Closeable theResource) {
        try {
            theResource.close(); // $codepro.audit.disable closeInFinally
        } catch (final Exception e) {
            LOG.error("Failed to close " + theResource, e);
        }
    }

    /** Logs and throws the exception to upper layers.
     * 
     * @param theCause
     *        the exception to be logged.
     * @param theMessage
     *        additional message to be logged. */
    public static void throwThis1(final Exception theCause,
            final String theMessage) {
        LOG.error(theMessage, theCause);
        throw new DBException(theMessage, theCause);
    }

    /** Converts LongArrayList to long array.
     * 
     * @param theLongList
     * @return */
    public static Long[] toArray(final LongArrayList theLongList) {
        final Long[] array = new Long[theLongList.size()];
        int count = 0;
        for (final LongCursor id : theLongList) {
            array[count++] = id.value;
        }
        return array;
    }

    /** Hide utility class constructor. */
    private DBUtils() {
    }

}
